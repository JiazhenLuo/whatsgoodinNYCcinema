import asyncio
import logging
import json
import os
import re
import random
import time
import traceback
import psutil
import functools
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import sys
from logging.handlers import RotatingFileHandler

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page

# 设置日志
def setup_logger(log_file=None):
    """设置日志系统，同时输出到控制台和文件"""
    logger = logging.getLogger("metrograph_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers = []  # 清除现有handlers

    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器（如果指定了日志文件）
    if log_file:
        # 确保日志目录存在
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        # 使用RotatingFileHandler替代FileHandler，设置最大文件大小为10MB，保留5个备份文件
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# 异步重试装饰器
def async_retry(max_retries=3, retry_delay=2, backoff_factor=2):
    """异步重试装饰器，支持指数退避"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"{EMOJI['error']} 函数 {func.__name__} 达到最大重试次数 {max_retries}，放弃")
                        logger.error(f"最后一次错误: {str(e)}")
                        logger.error(f"错误详情: {traceback.format_exc()}")
                        raise
                    
                    wait_time = retry_delay * (backoff_factor ** (retries - 1))
                    logger.warning(f"{EMOJI['warning']} 函数 {func.__name__} 出错 (尝试 {retries}/{max_retries}): {str(e)}")
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
        return wrapper
    return decorator

# 性能计时装饰器
def log_execution_time(func):
    """记录函数执行时间和内存使用的装饰器"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # 记录开始内存使用情况
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()
        
        try:
            result = await func(*args, **kwargs)
            # 记录结束时间和内存
            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_diff = end_memory - start_memory
            
            logger.info(f"{EMOJI['perf']} 函数 {func.__name__} 执行完成，耗时: {end_time - start_time:.2f} 秒，"
                       f"内存变化: {memory_diff:.2f} MB")
            return result
        except Exception as e:
            # 记录错误情况下的时间和内存
            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024
            memory_diff = end_memory - start_memory
            
            logger.error(f"{EMOJI['error']} 函数 {func.__name__} 执行失败，耗时: {end_time - start_time:.2f} 秒，"
                        f"内存变化: {memory_diff:.2f} MB, 错误: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise
    return wrapper

# 设置日志文件路径
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"metrograph_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# 初始化日志
logger = setup_logger(log_file)

# 日历页面URL
CALENDAR_URL = "https://metrograph.com/calendar/"
HOME_URL = "https://metrograph.com/"
OUTPUT_DIR = "database"
OUTPUT_FILE = "metrograph_movies.json"

# 特殊字符表情
EMOJI = {
    "start": "✨",
    "calendar": "📅",
    "movie": "🎬",
    "success": "✅",
    "error": "❌",
    "warning": "⚠️",
    "info": "ℹ️",
    "date": "📆",
    "time": "⏰",
    "loading": "⏳",
    "perf": "⚡",
    "debug": "🔍",
    "memory": "📊"
}

# 通用异步页面请求函数
async def fetch_page_content(browser, url, timeout=30000):
    """通用页面内容获取函数，处理加载超时和错误"""
    logger.info(f"正在访问页面: {url}")
    page = await browser.new_page()
    
    try:
        start_time = time.time()
        await page.goto(url, timeout=timeout)
        await page.wait_for_load_state("networkidle")
        load_time = time.time() - start_time
        logger.info(f"{EMOJI['perf']} 页面加载耗时: {load_time:.2f} 秒")
        
        html_content = await page.content()
        return page, html_content
    except Exception as e:
        logger.error(f"{EMOJI['error']} 访问页面 {url} 时出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        await page.close()
        raise

@log_execution_time
async def scrape_metrograph_all():
    """整合主页和日历页面的电影信息"""
    logger.info(f"{EMOJI['start']} 开始爬取Metrograph电影信息...")
    
    # 清理旧日志
    cleanup_old_logs(log_dir)
    
    # 创建输出目录
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", OUTPUT_DIR)
    os.makedirs(output_path, exist_ok=True)
    
    # 使用异步的Playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        
        try:
            # 首先从日历页面获取电影信息
            logger.info("从日历页面获取电影信息...")
            calendar_movies = await scrape_metrograph_calendar(CALENDAR_URL, browser=browser)
            
            # 然后从主页获取电影信息
            logger.info("从主页获取电影信息...")
            homepage_movies = await scrape_metrograph_homepage(HOME_URL, browser=browser)
            
            # 整合两个来源的电影信息
            all_movies = merge_movie_data(calendar_movies, homepage_movies)
            
            # 尝试补充电影详情
            enriched_movies = await enrich_movie_details(all_movies, browser)
            
            # 保存到JSON文件
            output_file = os.path.join(output_path, OUTPUT_FILE)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(enriched_movies, f, ensure_ascii=False, indent=4)
            
            # 统计信息
            total_movies = len(enriched_movies)
            movies_with_showtimes = sum(1 for movie in enriched_movies if movie["show_dates"])
            movies_without_showtimes = total_movies - movies_with_showtimes
            
            total_showtimes = 0
            total_dates = set()  # 使用集合避免重复计数
            
            for movie in enriched_movies:
                for date in movie["show_dates"]:
                    date_str = date["date"]
                    total_dates.add(date_str)
                    total_showtimes += len(date["times"])
            
            logger.info(f"{EMOJI['success']} 爬取完成，数据已存入 {os.path.abspath(output_file)}")
            logger.info(f"  - 总计: {total_movies} 部电影")
            logger.info(f"  - 有放映场次: {movies_with_showtimes} 部")
            logger.info(f"  - 无放映场次: {movies_without_showtimes} 部")
            logger.info(f"  - 总放映日期: {len(total_dates)} 个")
            logger.info(f"  - 总放映场次: {total_showtimes} 场")
            
            # 记录内存使用情况
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            logger.info(f"{EMOJI['memory']} 当前内存使用: {memory_usage:.2f} MB")
            
            return enriched_movies
            
        finally:
            await browser.close()

def merge_movie_data(calendar_movies: List[Dict], homepage_movies: List[Dict]) -> List[Dict]:
    """
    整合日历页面和主页的电影信息
    
    Args:
        calendar_movies: 从日历页面获取的电影列表
        homepage_movies: 从主页获取的电影列表
    
    Returns:
        整合后的电影列表
    """
    # 使用标题作为键，创建字典方便查找和合并
    movie_dict = {}
    
    # 先加入日历页面的电影信息
    for movie in calendar_movies:
        title = movie["title_en"]
        movie_dict[title] = movie
    
    # 整合主页电影的额外信息
    for movie in homepage_movies:
        title = movie["title_en"]
        if title in movie_dict:
            # 合并电影信息，保留日历页面的放映信息
            if movie.get("director") and not movie_dict[title].get("director"):
                movie_dict[title]["director"] = movie["director"]
            if movie.get("year") and not movie_dict[title].get("year"):
                movie_dict[title]["year"] = movie["year"]
            if movie.get("image_url") and not movie_dict[title].get("image_url"):
                movie_dict[title]["image_url"] = movie["image_url"]
            if movie.get("duration") and not movie_dict[title].get("duration"):
                movie_dict[title]["duration"] = movie["duration"]
            if movie.get("language") and not movie_dict[title].get("language"):
                movie_dict[title]["language"] = movie["language"]
            if movie.get("overview_en") and not movie_dict[title].get("overview_en"):
                movie_dict[title]["overview_en"] = movie["overview_en"]
            if movie.get("trailer_url") and not movie_dict[title].get("trailer_url"):
                movie_dict[title]["trailer_url"] = movie["trailer_url"]
            if movie.get("note") and not movie_dict[title].get("note"):
                movie_dict[title]["note"] = movie["note"]
        else:
            # 如果是主页独有的电影，也加入整合后的列表
            movie_dict[title] = movie
    
    # 将字典转换回列表
    return list(movie_dict.values())

async def scrape_metrograph_homepage(url: str, browser=None) -> List[Dict]:
    """
    爬取Metrograph主页的电影信息
    
    Args:
        url: 主页URL
        browser: 浏览器实例
    
    Returns:
        电影信息列表
    """
    logger.info(f"开始爬取Metrograph主页: {url}")
    
    close_browser = False
    if browser is None:
        p = await async_playwright()
        browser = await p.chromium.launch()
        close_browser = True
    
    page = await browser.new_page()
    movies = []

    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_load_state("networkidle")
        
        # 获取页面内容
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找主页的电影卡片/区块
        movie_cards = soup.select('div.film-card') or soup.select('.film-item') or soup.select('.now-playing-item')
        
        for card in movie_cards:
            movie = {}
            
            # 提取标题
            title_tag = card.select_one('h3.title') or card.select_one('.title') or card.select_one('h3')
            if title_tag:
                movie["title_en"] = title_tag.get_text(strip=True)
            else:
                continue  # 如果找不到标题，跳过此卡片
            
            # 提取详情链接
            detail_link = None
            link_tag = card.select_one('a')
            if link_tag:
                detail_link = link_tag.get('href', '')
                if detail_link and not detail_link.startswith('http'):
                    detail_link = f"https://metrograph.com{detail_link}"
                movie["detail_url"] = detail_link
            
            # 提取图片
            img_tag = card.select_one('img')
            if img_tag:
                img_url = img_tag.get('src', '')
                if img_url and not img_url.startswith('http'):
                    img_url = f"https://metrograph.com{img_url}"
                movie["image_url"] = img_url
            
            # 提取其他基本信息
            info_tag = card.select_one('.info') or card.select_one('.meta')
            if info_tag:
                # 提取导演
                director_tag = info_tag.select_one('.director') or info_tag.find(string=re.compile(r'Director', re.IGNORECASE))
                if director_tag:
                    director_text = director_tag.get_text(strip=True) if hasattr(director_tag, 'get_text') else str(director_tag)
                    director_match = re.search(r'Director[s]?:\s*([^,]+)', director_text, re.IGNORECASE)
                    if director_match:
                        movie["director"] = director_match.group(1).strip()
                
                # 提取年份
                year_tag = info_tag.select_one('.year') or info_tag.find(string=re.compile(r'\b\d{4}\b'))
                if year_tag:
                    year_text = year_tag.get_text(strip=True) if hasattr(year_tag, 'get_text') else str(year_tag)
                    year_match = re.search(r'\b(\d{4})\b', year_text)
                    if year_match:
                        movie["year"] = int(year_match.group(1))
                
                # 提取时长
                duration_tag = info_tag.select_one('.duration') or info_tag.find(string=re.compile(r'\d+\s*min'))
                if duration_tag:
                    duration_text = duration_tag.get_text(strip=True) if hasattr(duration_tag, 'get_text') else str(duration_tag)
                    duration_match = re.search(r'(\d+)\s*min', duration_text)
                    if duration_match:
                        movie["duration"] = f"{duration_match.group(1)} min"
            
            # 提取简介
            desc_tag = card.select_one('.description') or card.select_one('.overview')
            if desc_tag:
                movie["overview_en"] = desc_tag.get_text(strip=True)
            
            # 设置影院信息
            movie["cinema"] = "Metrograph"
            
            # 添加空的放映信息（将在后续合并中保留日历数据）
            movie["show_dates"] = []
            
            movies.append(movie)
    
    except Exception as e:
        logger.error(f"爬取主页时出错: {str(e)}")
    
    finally:
        await page.close()
        if close_browser:
            await browser.close()
    
    logger.info(f"从主页获取了 {len(movies)} 部电影的信息")
    return movies

@log_execution_time
async def scrape_metrograph_calendar(
    url: str = CALENDAR_URL,
    max_retries: int = 3,
    retry_delay: int = 2,
    browser = None
) -> List[Dict]:
    """
    爬取Metrograph日历页面，先解析日历获取可用日期，然后访问每个日期页面爬取电影信息
    
    Args:
        url: 日历页面URL
        max_retries: 最大重试次数
        retry_delay: 重试间隔（秒）
        browser: 已有的浏览器实例
    
    Returns:
        包含所有电影放映信息的列表
    """
    logger.info(f"{EMOJI['calendar']} 开始爬取Metrograph日历信息: {url}")
    
    close_browser = False
    if browser is None:
        p = await async_playwright()
        browser = await p.chromium.launch()
        close_browser = True
    
    try:
        # 首先访问日历页面，获取所有有效日期
        available_dates = await get_available_dates(browser, url, max_retries, retry_delay)
        logger.info(f"从日历中找到 {len(available_dates)} 个有效放映日期")
        
        if not available_dates:
            logger.error(f"{EMOJI['error']} 未能从日历页面获取有效日期")
            if close_browser:
                await browser.close()
            return []
        
        # 用于存储所有电影数据的字典
        all_movies = {}
        
        # 访问每个日期页面
        for i, date_str in enumerate(available_dates):
            date_url = f"{url}?date={date_str}"
            logger.info(f"[{i+1}/{len(available_dates)}] 爬取日期 {date_str} 的电影信息: {date_url}")
            
            # 爬取该日期页面的电影信息
            date_movies = await scrape_calendar_page(date_url, max_retries, retry_delay, browser)
            logger.info(f"  - 从该日期页面获取了 {len(date_movies)} 部电影的信息")
            
            # 合并电影数据
            for movie in date_movies:
                title = movie["title_en"]
                if title in all_movies:
                    # 检查是否有相同日期的放映信息
                    for new_date in movie["show_dates"]:
                        date_exists = False
                        for existing_date in all_movies[title]["show_dates"]:
                            if existing_date["date"] == new_date["date"]:
                                # 如果已有相同日期，检查是否有相同时间的放映
                                for new_time in new_date["times"]:
                                    time_exists = False
                                    for existing_time in existing_date["times"]:
                                        if existing_time["time"] == new_time["time"]:
                                            time_exists = True
                                            break
                                    if not time_exists:
                                        existing_date["times"].append(new_time)
                                date_exists = True
                                break
                        if not date_exists:
                            all_movies[title]["show_dates"].append(new_date)
                else:
                    all_movies[title] = movie
        
        if close_browser:
            await browser.close()
        
        # 将字典转换为列表
        movies_list = list(all_movies.values())
        logger.info(f"{EMOJI['movie']} 共获取到 {len(movies_list)} 部电影的放映信息")
        
        # 格式化日期，使其与Film Forum格式一致 (YYYY-MM-DD)
        logger.info(f"{EMOJI['date']} 正在格式化日期为标准格式 (YYYY-MM-DD)...")
        date_format_count = 0
        for movie in movies_list:
            for date_info in movie["show_dates"]:
                original_date = date_info["date"]
                try:
                    # 尝试解析如 "Friday March 28, 2025" 格式的日期
                    parsed_date = datetime.strptime(original_date, "%A %B %d, %Y")
                    date_info["date"] = parsed_date.strftime("%Y-%m-%d")
                    date_format_count += 1
                except ValueError:
                    # 如果解析失败，保留原始格式
                    logger.warning(f"{EMOJI['warning']} 无法解析日期格式: {original_date}")
        logger.info(f"  - 成功格式化 {date_format_count} 个日期")
        
        return movies_list
    
    except Exception as e:
        logger.error(f"{EMOJI['error']} 爬取日历页面时出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        if close_browser:
            await browser.close()
        return []

@log_execution_time
async def get_available_dates(browser, url: str, max_retries: int, retry_delay: int) -> List[str]:
    """
    从日历页面获取所有有效放映日期
    
    Args:
        browser: Playwright浏览器实例
        url: 日历页面URL
        max_retries: 最大重试次数
        retry_delay: 重试间隔（秒）
    
    Returns:
        可用日期列表，格式为YYYY-MM-DD
    """
    page = await browser.new_page()
    available_dates = []
    retries = 0
    
    while retries < max_retries:
        try:
            logger.info(f"访问日历页面获取可用日期: {url}")
            start_time = time.time()
            await page.goto(url)
            await page.wait_for_load_state("networkidle")
            load_time = time.time() - start_time
            logger.info(f"{EMOJI['perf']} 页面加载耗时: {load_time:.2f} 秒")
            
            # 获取页面内容
            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 查找所有日历月份
            calendar_months = soup.select('div.calendar_month')
            
            for month in calendar_months:
                # 获取每个月中的所有日期条目
                date_items = month.select('li[data-thisdate]')
                
                for item in date_items:
                    # 获取日期值
                    date_str = item.get('data-thisdate')
                    if not date_str:
                        continue
                    
                    # 检查是否是可以查看放映信息的日期（非past和unscheduled）
                    is_past = 'past' in item.get('class', [])
                    is_unscheduled = 'unscheduled' in item.get('class', [])
                    title = item.get('title', '')
                    
                    if not is_past and not is_unscheduled and ('See showtimes' in title or 'Today' in title):
                        available_dates.append(date_str)
                        logger.debug(f"{EMOJI['debug']} 添加可用日期: {date_str}")
            
            break
        except Exception as e:
            retries += 1
            logger.error(f"获取日历日期时出错 (尝试 {retries}/{max_retries}): {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            if retries < max_retries:
                wait_time = retry_delay * (2 ** (retries - 1))
                logger.info(f"等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
            else:
                logger.error("达到最大重试次数，放弃获取日历日期")
    
    await page.close()
    return available_dates

async def scrape_calendar_page(
    url: str,
    max_retries: int = 3,
    retry_delay: int = 2,
    browser = None
) -> List[Dict]:
    """
    爬取指定日期的Metrograph电影日历页面并解析电影放映信息
    
    Args:
        url: 日历页面URL，包含日期参数
        max_retries: 最大重试次数
        retry_delay: 重试间隔（秒）
        browser: 已有的浏览器实例，如果提供则使用此实例，否则创建新实例
    
    Returns:
        包含电影放映信息的字典列表
    """
    logger.info(f"开始从页面爬取电影信息: {url}")
    
    html_content = None
    retries = 0
    close_browser = False
    
    # 如果未提供浏览器实例，创建新实例
    if browser is None:
        p = await async_playwright()
        browser = await p.chromium.launch()
        close_browser = True
    
    page = await browser.new_page()
    
    try:
        while retries < max_retries:
            try:
                logger.info(f"访问页面: {url}")
                await page.goto(url)
                logger.info("页面加载完成，等待网络空闲...")
                await page.wait_for_load_state("networkidle")
                
                html_content = await page.content()
                break
            except Exception as e:
                retries += 1
                logger.error(f"加载页面时出错 (尝试 {retries}/{max_retries}): {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                if retries < max_retries:
                    wait_time = retry_delay * (2 ** (retries - 1))  # 指数退避
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("达到最大重试次数，放弃爬取")
                    await page.close()
                    if close_browser:
                        await browser.close()
                    return []
        
        if not html_content:
            logger.error("未能获取页面内容")
            await page.close()
            if close_browser:
                await browser.close()
            return []
        
        logger.info("开始解析页面内容...")
        movies_data = parse_calendar_page(html_content)
        
        # 如果是需要丰富电影详情的场景，则调用enrich_movie_details
        # enriched_movies = await enrich_movie_details(movies_data, browser)
        
        await page.close()
        if close_browser:
            await browser.close()
            
        return movies_data
    
    except Exception as e:
        logger.error(f"爬取页面时出现错误: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        await page.close()
        if close_browser:
            await browser.close()
        return []

def parse_calendar_page(html_content: str) -> List[Dict]:
    """
    解析HTML内容，提取电影放映信息
    
    Args:
        html_content: HTML内容
    
    Returns:
        包含电影信息的列表
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 初始化年份变量
        year = None
    
    # 尝试从页面获取当前年份
    now_time_tag = soup.find(string=re.compile(r'Now time \d{4}-\d{2}-\d{2}'))
    if now_time_tag:
        year_match = re.search(r'(\d{4})-\d{2}-\d{2}', now_time_tag)
            if year_match:
            year = year_match.group(1)
            logger.info(f"从'Now time'标记中提取到年份: {year}")
    
    if not year:
        # 如果找不到年份，默认为当前年份
        from datetime import datetime
        year = str(datetime.now().year)
        logger.info(f"未找到年份标记，使用当前年份: {year}")
    
    # 找到所有日期区块 - 修复选择器
    date_blocks = soup.select('div.calendar-list-day')
    if len(date_blocks) == 0:
        # 尝试其他可能的选择器
        date_blocks = soup.select('.calendar-list-day')
        if len(date_blocks) == 0:
            # 再尝试其他选择器
            date_blocks = soup.select('li.day-section')
            if len(date_blocks) == 0:
                # 最后尝试
                date_blocks = soup.find_all('div', class_=lambda c: c and 'calendar-list-day' in c)
                
    logger.info(f"找到 {len(date_blocks)} 个日期区块")
    
    # 如果仍然没有找到日期区块，记录HTML结构以便调试
    if len(date_blocks) == 0:
        logger.warning("无法找到任何日期区块，记录HTML结构以便调试")
        with open("calendar_html_debug.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.warning("已将HTML内容保存到calendar_html_debug.html文件，请检查HTML结构")
        
        # 尝试查找页面中的关键元素来帮助识别结构
        logger.info("尝试识别页面结构:")
        for tag in soup.select('div[class]'):
            class_attr = tag.get('class', [])
            if class_attr and any('calendar' in c.lower() for c in class_attr):
                logger.info(f"发现可能相关的元素: {tag.name}.{' '.join(class_attr)}")
    
    # 用于保存所有电影数据的字典，键为电影标题，值为电影信息
    # 这样可以合并同一电影在不同日期的场次
    movies_dict = {}
    
    # 遍历每个日期区块
    for date_block in date_blocks:
        # 提取日期 - 修复日期选择器
        date_heading = date_block.select_one('h2.date-heading')
        if not date_heading:
            date_heading = date_block.select_one('.date')
            if not date_heading:
                date_heading = date_block.select_one('h2') or date_block.select_one('h3')
                if not date_heading:
                    continue

        date_text = date_heading.get_text(strip=True)
        logger.info(f"处理日期: {date_text}")
        
        # 确保日期包含年份
        if year and year not in date_text:
            date_text = f"{date_text}, {year}"
        
        # 提取该日期下的所有电影条目 - 修复电影条目选择器
        movie_entries = date_block.select('div.movie-entry')
        if len(movie_entries) == 0:
            movie_entries = date_block.select('.item') or date_block.select('div.item')
            if len(movie_entries) == 0:
                movie_entries = date_block.select('li') or date_block.select('.movie')
        
        logger.info(f"  在此日期下找到 {len(movie_entries)} 个电影条目")
        
        # 遍历每个电影条目
        for movie_entry in movie_entries:
            # 提取电影标题 - 修复标题选择器
            title_tag = movie_entry.select_one('h3.movie-entry-title a')
            if not title_tag:
                title_tag = movie_entry.select_one('a.title') or movie_entry.select_one('.title a') or movie_entry.select_one('a')
                if not title_tag:
                    continue

            title = title_tag.get_text(strip=True)
            detail_url = title_tag.get('href', '')
            if detail_url and not detail_url.startswith('http'):
                detail_url = f"https://metrograph.com{detail_url}"
            
            # 提取场次信息 - 修复放映时间选择器
            showtime_items = movie_entry.select('ul.movie-entry-showtimes li')
            if len(showtime_items) == 0:
                showtimes_container = movie_entry.select_one('.calendar-list-showtimes') or movie_entry.select_one('.showtimes')
                if showtimes_container:
                    time_links = showtimes_container.select('a')
                    # 确保跳过第一个链接如果它是标题链接
                    start_idx = 1 if len(time_links) > 1 and time_links[0].get_text(strip=True) == title else 0
                    showtime_items = time_links[start_idx:]
            
            showtimes = []
            
            for item in showtime_items:
                time_text = item.get_text(strip=True)
                
                # 检查是否售罄 - 增强检测逻辑
                sold_out = False
                # 检查是否有表示售罄的红色文本
                if item.select_one('span.text-red'):
                    sold_out = True
                # 检查class中是否有sold_out或sold-out标记
                item_classes = item.get('class', [])
                if item_classes and ('sold_out' in item_classes or 'sold-out' in item_classes):
                    sold_out = True
                # 检查是否有售罄的文本提示
                if 'sold out' in time_text.lower() or 'sold-out' in time_text.lower():
                    sold_out = True
                # 检查a标签是否有sold_out类
                a_tag = item.select_one('a')
                if a_tag and a_tag.get('class') and ('sold_out' in a_tag.get('class') or 'sold-out' in a_tag.get('class')):
                    sold_out = True
                
                # 提取购票链接
                ticket_link = None
                if hasattr(item, 'name') and item.name == 'a':
                    link_tag = item
                else:
                    link_tag = item.select_one('a')
                
                if link_tag and not sold_out:
                    ticket_link = link_tag.get('href', '')
                    if ticket_link and not ticket_link.startswith('http'):
                        if 'ticketing' in ticket_link.lower():
                            ticket_link = f"https://t.metrograph.com{ticket_link}"
                        else:
                            ticket_link = f"https://metrograph.com{ticket_link}"
                
                showtimes.append({
                        "time": time_text,
                    "ticket_url": ticket_link,
                        "sold_out": sold_out
                    })

            # 如果该电影已存在，添加新的放映日期
            if title in movies_dict:
                movies_dict[title]["show_dates"].append({
                    "date": date_text,
                    "times": showtimes
                })
            else:
                # 否则创建新的电影条目
                movies_dict[title] = {
                    "title_en": title,
                    "show_dates": [{
                        "date": date_text,
                        "times": showtimes
                    }],
                    "detail_url": detail_url,
                    "image_url": "",
                    "director": "",
                    "year": None,
                    "cinema": "Metrograph"
                }
            
            logger.info(f"  - 电影 '{title}': {len(showtimes)} 个放映时间")
    
    # 将字典转换为列表
    movies_list = list(movies_dict.values())
    
    # 计算放映场次总数
    total_showtimes = 0
    for movie in movies_list:
        for date in movie["show_dates"]:
            total_showtimes += len(date["times"])
    
    logger.info(f"从日历页面提取了 {len(movies_list)} 部电影的放映信息，共 {total_showtimes} 场放映")
    
    return movies_list

async def enrich_movie_details(movies: List[Dict], browser) -> List[Dict]:
    """
    访问电影详情页面，补充电影信息
    
    Args:
        movies: 电影信息列表
        browser: 已经创建的浏览器实例
    
    Returns:
        补充了详细信息的电影列表
    """
    logger.info(f"{EMOJI['info']} 开始enriching {len(movies)} 部电影的详细信息...")
    
    enriched_movies = []
    
    try:
        page = await browser.new_page()
        
        for i, movie in enumerate(movies):
            logger.info(f"[{i+1}/{len(movies)}] Enriching movie: {movie['title_en']}")
            
            # 如果没有详情页URL，跳过
            if not movie["detail_url"]:
                logger.info(f"  - 没有详情页URL，跳过")
                enriched_movies.append(movie)
                continue
            
            try:
                # 访问详情页
                logger.info(f"  - 正在访问详情页: {movie['detail_url']}")
                await page.goto(movie["detail_url"], timeout=30000)
                await page.wait_for_load_state("networkidle")
                
                # 提取详细信息
                movie_details = await extract_movie_details(page)
                
                # 获取详细的放映信息，包括Q&A和特别活动
                detailed_showtimes = await extract_showtimes_with_details(page)
                
                # 打印查找到的详细信息
                detail_found = []
                
                # 更新电影信息
                if movie_details:
                    if movie_details.get("director") and not movie.get("director"):
                        movie["director"] = movie_details["director"]
                        detail_found.append(f"导演: {movie_details['director']}")
                    
                    if movie_details.get("year") and not movie.get("year"):
                        movie["year"] = movie_details["year"]
                        detail_found.append(f"年份: {movie_details['year']}")
                    
                    if movie_details.get("image_url") and not movie.get("image_url"):
                        movie["image_url"] = movie_details["image_url"]
                        detail_found.append("获取到海报图片")
                    
                    if movie_details.get("duration") and not movie.get("duration"):
                        movie["duration"] = movie_details["duration"]
                        detail_found.append(f"时长: {movie_details['duration']}")
                    
                    if movie_details.get("language") and not movie.get("language"):
                        movie["language"] = movie_details["language"]
                        detail_found.append(f"语言: {movie_details['language']}")
                    
                    if movie_details.get("overview_en") and not movie.get("overview_en"):
                        movie["overview_en"] = movie_details["overview_en"]
                        overview_summary = movie_details["overview_en"][:50] + "..." if len(movie_details["overview_en"]) > 50 else movie_details["overview_en"]
                        detail_found.append(f"简介: {overview_summary}")
                    
                    if movie_details.get("has_qa") is not None:
                        movie["has_qa"] = movie_details["has_qa"]
                        if movie["has_qa"] and movie_details.get("qa_details"):
                            movie["qa_details"] = movie_details["qa_details"]
                            detail_found.append(f"Q&A信息: {movie_details['qa_details'][:50]}..." if len(movie_details['qa_details']) > 50 else movie_details['qa_details'])
                    
                    if movie_details.get("has_introduction") is not None:
                        movie["has_introduction"] = movie_details["has_introduction"]
                        if movie["has_introduction"] and movie_details.get("introduction_details"):
                            movie["introduction_details"] = movie_details["introduction_details"]
                            detail_found.append(f"介绍信息: {movie_details['introduction_details'][:50]}..." if len(movie_details['introduction_details']) > 50 else movie_details['introduction_details'])
                    
                    if movie_details.get("trailer_url") and not movie.get("trailer_url"):
                        movie["trailer_url"] = movie_details["trailer_url"]
                        detail_found.append("获取到预告片链接")
                
                # 处理详细放映信息
                if detailed_showtimes:
                    # 创建映射来匹配详细放映信息与原始放映信息
                    detailed_map = {}
                    for ds in detailed_showtimes:
                        key = f"{ds.get('date')}_{ds.get('time')}"
                        detailed_map[key] = ds
                    
                    has_qa_screenings = False
                    has_intro_screenings = False
                    qa_details_found = []
                    intro_details_found = []
                    
                    # 遍历电影的所有放映日期
                    for date_info in movie["show_dates"]:
                        date_str = date_info["date"]
                        
                        # 更新每个放映时间的详细信息
                        for time_info in date_info["times"]:
                            time_str = time_info["time"]
                            key = f"{date_str}_{time_str}"
                            
                            # 如果在详细信息中找到匹配的放映
                            if key in detailed_map:
                                details = detailed_map[key]
                                
                                # 添加Q&A信息
                                if details.get("has_qa"):
                                    time_info["has_qa"] = True
                                    has_qa_screenings = True
                                    if details.get("special_event"):
                                        time_info["qa_details"] = details["special_event"]
                                        if details["special_event"] not in qa_details_found:
                                            qa_details_found.append(details["special_event"])
                                    if details.get("qa_person"):
                                        time_info["qa_person"] = details["qa_person"]
                                
                                # 添加介绍信息
                                if details.get("has_introduction"):
                                    time_info["has_introduction"] = True
                                    has_intro_screenings = True
                                    if details.get("special_event"):
                                        time_info["introduction_details"] = details["special_event"]
                                        if details["special_event"] not in intro_details_found:
                                            intro_details_found.append(details["special_event"])
                                    if details.get("introduction_person"):
                                        time_info["introduction_person"] = details["introduction_person"]
                                
                                # 添加特别活动信息
                                if details.get("special_event") and not details.get("has_qa") and not details.get("has_introduction"):
                                    time_info["special_event"] = details["special_event"]
                    
                    # 更新电影的Q&A和介绍标志
                    if has_qa_screenings:
                        movie["has_qa"] = True
                        if qa_details_found:
                            movie["qa_details"] = " | ".join(qa_details_found)
                            detail_found.append(f"Q&A信息: {movie['qa_details'][:50]}..." if len(movie['qa_details']) > 50 else movie['qa_details'])
                    
                    if has_intro_screenings:
                        movie["has_introduction"] = True
                        if intro_details_found:
                            movie["introduction_details"] = " | ".join(intro_details_found)
                            detail_found.append(f"介绍信息: {movie['introduction_details'][:50]}..." if len(movie['introduction_details']) > 50 else movie['introduction_details'])
                    
                    detail_found.append(f"详细放映信息: {len(detailed_showtimes)} 个场次")
                
                if detail_found:
                    logger.info(f"  - 已补充信息: {', '.join(detail_found)}")
                else:
                    logger.info("  - 未找到新增信息")
                
                # 显示找到的简介
                if movie_details.get("overview_en"):
                    logger.info(f"  - 电影简介: {movie_details['overview_en'][:100]}...")
                
                # 显示Q&A信息和Introduction信息
                if movie.get("has_qa") and movie.get("qa_details"):
                    logger.info(f"  - Q&A信息: {movie['qa_details']}")
                
                if movie.get("has_introduction") and movie.get("introduction_details"):
                    logger.info(f"  - 介绍信息: {movie['introduction_details']}")
                
                # 随机延迟以避免请求过快
                delay = random.uniform(0.5, 1.5)
                logger.info(f"  - {EMOJI['loading']} 等待 {delay:.2f} 秒...")
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"获取 '{movie['title_en']}' 详情时出错: {str(e)}")
                
            enriched_movies.append(movie)
        
        await page.close()
        
    except Exception as e:
        logger.error(f"{EMOJI['error']} Enriching电影详情时出错: {str(e)}")
        # 如果出错，返回原始电影列表
    return movies
    
    return enriched_movies

async def extract_movie_details(page: Page) -> Dict[str, Any]:
    """
    从电影详情页提取信息，兼容Film Forum格式
    
    Args:
        page: Playwright页面对象
    
    Returns:
        包含电影详细信息的字典
    """
    details = {}
    
    try:
        # 获取页面内容
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 提取导演
        director_tag = soup.select_one('span.director') or soup.find(string=re.compile(r'Director', re.IGNORECASE))
        if director_tag:
            director_text = director_tag.get_text(strip=True) if hasattr(director_tag, 'get_text') else str(director_tag)
            director_match = re.search(r'Director[s]?:\s*([^,]+)', director_text, re.IGNORECASE)
            if director_match:
                details["director"] = director_match.group(1).strip()
        
        # 从h5标签中提取年份和时长信息
        # 查找格式如 "1992 / 115min / DCP" 的h5标签
        h5_tags = soup.find_all('h5')
        for h5 in h5_tags:
            # 匹配年份/时长格式
            year_duration_match = re.search(r'(\d{4})\s*/\s*(\d+)min', h5.get_text(strip=True))
            if year_duration_match:
                details["year"] = int(year_duration_match.group(1))
                details["duration"] = f"{year_duration_match.group(2)} min"
                break
        
        # 如果h5标签中没找到，尝试其他方式
        if not details.get("year"):
            year_tag = soup.select_one('span.year') or soup.find(string=re.compile(r'\b\d{4}\b'))
            if year_tag:
                year_text = year_tag.get_text(strip=True) if hasattr(year_tag, 'get_text') else str(year_tag)
                year_match = re.search(r'\b(\d{4})\b', year_text)
                if year_match:
                    details["year"] = int(year_match.group(1))
        
        # 如果没找到时长，尝试其他方式
        if not details.get("duration"):
            duration_tag = soup.select_one('.duration') or soup.find(string=re.compile(r'\d+\s*min'))
            if duration_tag:
                duration_text = duration_tag.get_text(strip=True) if hasattr(duration_tag, 'get_text') else str(duration_tag)
                duration_match = re.search(r'(\d+)\s*min', duration_text)
                if duration_match:
                    details["duration"] = f"{duration_match.group(1)} min"
        
        # 提取语言
        language_tag = soup.select_one('.language') or soup.find(string=re.compile(r'Language', re.IGNORECASE))
        if language_tag:
            language_text = language_tag.get_text(strip=True) if hasattr(language_tag, 'get_text') else str(language_tag)
            language_match = re.search(r'Language[s]?:\s*([^,]+)', language_text, re.IGNORECASE)
            if language_match:
                details["language"] = language_match.group(1).strip()
        
        # 提取概述/简介
        # 1. 首先尝试使用常见选择器
        overview_tag = soup.select_one('.description') or soup.select_one('.overview') or soup.select_one('.synopsis')
        
        # 2. 如果没有找到，尝试找到时长后的段落
        if not overview_tag or not overview_tag.get_text(strip=True):
            # 寻找包含时长信息的h5标签
            duration_h5 = soup.find('h5', string=re.compile(r'\d+\s*min'))
            if duration_h5:
                # 找到时长后的非空p标签
                next_p = duration_h5.find_next('p')
                # 如果第一个p标签是空的，查找后续非空p标签
                while next_p and not next_p.get_text(strip=True):
                    next_p = next_p.find_next('p')
                if next_p and next_p.get_text(strip=True):
                    overview_tag = next_p
        
        # 3. 尝试在电影信息区域中找非空p标签
        if not overview_tag or not overview_tag.get_text(strip=True):
            movie_info = soup.select_one('.movie-info')
            if movie_info:
                p_tags = movie_info.select('p')
                # 查找第一个非空p标签作为描述
                for p in p_tags:
                    if p.get_text(strip=True):
                        overview_tag = p
                        break
        
        if overview_tag and overview_tag.get_text(strip=True):
            overview_text = overview_tag.get_text(strip=True)
            details["overview_en"] = overview_text
        
        # 检查电影信息区域中的所有段落，查找Q&A信息
        details["has_qa"] = False
        movie_info = soup.select_one('.movie-info')
        qa_details = []
        
        if movie_info:
            p_tags = movie_info.select('p')
            for p in p_tags:
                p_text = p.get_text(strip=True)
                if not p_text:
                    continue
                    
                qa_match = re.search(r'Q\s*&\s*A\s+with|Q\s*&\s*A\s+featuring|Q\s*&\s*A\s+by|Q\s*&\s*A\s+moderated\s+by|discussion\s+with', p_text, re.IGNORECASE)
                if qa_match or "Q&A" in p_text:
                    details["has_qa"] = True
                    qa_details.append(p_text)
        
        if details["has_qa"] and qa_details:
            details["qa_details"] = " | ".join(qa_details)
        
        # 检查电影信息区域中的所有段落，查找介绍信息
        details["has_introduction"] = False
        intro_details = []
        
        if movie_info:
            p_tags = movie_info.select('p')
            for p in p_tags:
                p_text = p.get_text(strip=True)
                if not p_text:
                    continue
                    
                intro_match = re.search(r'introduced\s+by|introduction\s+by|presented\s+by|moderated\s+by', p_text, re.IGNORECASE)
                if intro_match and "Q&A" not in p_text.upper():  # 确保不是Q&A的一部分
                    details["has_introduction"] = True
                    intro_details.append(p_text)
        
        if details["has_introduction"] and intro_details:
            details["introduction_details"] = " | ".join(intro_details)
        
        # 提取预告片
        trailer_tag = soup.select_one('iframe[src*="youtube"]') or soup.select_one('iframe[src*="vimeo"]') or soup.select_one('lite-youtube')
        if trailer_tag:
            if trailer_tag.name == 'lite-youtube':
                # 提取视频ID
                video_id = trailer_tag.get('videoid', '')
                if video_id:
                    details["trailer_url"] = f"https://www.youtube.com/watch?v={video_id}"
            else:
                details["trailer_url"] = trailer_tag.get('src', '')
        
        # 提取海报图片
        image_tag = soup.select_one('div.film-poster img') or soup.select_one('.poster img') or soup.select_one('.movie-image img')
        if image_tag:
            image_url = image_tag.get('src', '')
            if image_url:
                if not image_url.startswith('http'):
                    image_url = f"https://metrograph.com{image_url}"
                details["image_url"] = image_url
        
        # 提取特别注释（如"NEW 4K RESTORATION"）
        note_tag = soup.select_one('.note') or soup.select_one('.special-note')
        if note_tag:
            details["note"] = note_tag.get_text(strip=True)
    
    except Exception as e:
        logger.error(f"解析电影详情页时出错: {str(e)}")
    
    return details

async def extract_showtimes_with_details(page: Page) -> List[Dict]:
    """
    从电影详情页提取每个放映场次的详细信息，特别关注Q&A和特别活动
    
    Args:
        page: Playwright页面对象
    
    Returns:
        包含每个放映场次详细信息的列表
    """
    showtimes_details = []
    
    try:
        # 获取页面内容
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找所有放映日期区块
        date_blocks = soup.select('.showtimes-date') or soup.select('.showtime-date-block')
        
        for date_block in date_blocks:
            # 提取日期
            date_heading = date_block.select_one('h3') or date_block.select_one('.date-heading')
            if not date_heading:
                continue
                
            date_text = date_heading.get_text(strip=True)
            
            # 查找该日期下的所有放映时间
            time_items = date_block.select('.showtime-item') or date_block.select('.time-item')
            
            for time_item in time_items:
                showtime_info = {"date": date_text}
                
                # 提取时间
                time_tag = time_item.select_one('.time') or time_item.select_one('span.hour')
                if time_tag:
                    showtime_info["time"] = time_tag.get_text(strip=True)
                else:
                    # 如果找不到专门的时间标签，尝试从整个item中提取
                    time_text = time_item.get_text(strip=True)
                    time_match = re.search(r'\b(\d{1,2}:\d{2}(?:\s*[AP]M)?)\b', time_text)
                    if time_match:
                        showtime_info["time"] = time_match.group(1)
                    else:
                        continue  # 如果找不到时间信息，跳过此项
                
                # 检查是否售罄
                sold_out = False
                sold_out_tag = time_item.select_one('.sold-out') or time_item.select_one('.unavailable')
                if sold_out_tag or 'sold out' in time_item.get_text(strip=True).lower():
                    sold_out = True
                showtime_info["sold_out"] = sold_out
                
                # 提取购票链接
                ticket_link = None
                link_tag = time_item.select_one('a')
                if link_tag and not sold_out:
                    ticket_link = link_tag.get('href', '')
                    if ticket_link and not ticket_link.startswith('http'):
                        if 'ticketing' in ticket_link.lower():
                            ticket_link = f"https://t.metrograph.com{ticket_link}"
                        else:
                            ticket_link = f"https://metrograph.com{ticket_link}"
                showtime_info["ticket_url"] = ticket_link
                
                # 查找特别活动信息，如Q&A
                event_info = time_item.select_one('.event-info') or time_item.select_one('.special-info')
                if event_info:
                    event_text = event_info.get_text(strip=True)
                    showtime_info["special_event"] = event_text
                    
                    # 判断是否包含Q&A
                    qa_match = re.search(r'Q\s*&\s*A|discussion', event_text, re.IGNORECASE)
                    showtime_info["has_qa"] = bool(qa_match)
                    
                    # 判断是否包含introduction
                    intro_match = re.search(r'intro|presented by', event_text, re.IGNORECASE)
                    showtime_info["has_introduction"] = bool(intro_match)
                else:
                    # 尝试从整个item文本中查找特别事件信息
                    item_text = time_item.get_text(strip=True)
                    
                    # 移除时间部分，只看其余文本是否有特别事件
                    time_pattern = r'\b\d{1,2}:\d{2}(?:\s*[AP]M)?\b'
                    event_text = re.sub(time_pattern, '', item_text).strip()
                    
                    if event_text and event_text != "Buy Tickets" and event_text != "Sold Out":
                        showtime_info["special_event"] = event_text
                        showtime_info["has_qa"] = 'q&a' in event_text.lower() or 'discussion' in event_text.lower()
                        showtime_info["has_introduction"] = 'intro' in event_text.lower() or 'presented by' in event_text.lower()
                
                showtimes_details.append(showtime_info)
        
        # 如果直接从放映区块无法找到信息，尝试从页面其他位置查找Q&A和特别活动信息
        if not any(st.get("has_qa") or st.get("has_introduction") for st in showtimes_details):
            # 在整个页面中查找Q&A或介绍信息
            movie_info = soup.select_one('.movie-info') or soup.select_one('.film-info')
            if movie_info:
                info_text = movie_info.get_text(strip=True)
                qa_match = re.search(r'Q\s*&\s*A\s+with\s+([^\.]+)', info_text, re.IGNORECASE)
                intro_match = re.search(r'introduced\s+by\s+([^\.]+)', info_text, re.IGNORECASE)
                
                # 如果找到，将信息应用到所有场次
                if qa_match or intro_match:
                    for showtime in showtimes_details:
                        if qa_match:
                            showtime["has_qa"] = True
                            showtime["qa_person"] = qa_match.group(1).strip()
                        if intro_match:
                            showtime["has_introduction"] = True
                            showtime["introduction_person"] = intro_match.group(1).strip()
    
    except Exception as e:
        logger.error(f"提取放映场次详细信息时出错: {str(e)}")
    
    return showtimes_details

async def get_detailed_showtimes(movie_url: str, browser) -> List[Dict]:
    """
    获取电影详情页中的每个放映场次的详细信息
    
    Args:
        movie_url: 电影详情页URL
        browser: 浏览器实例
    
    Returns:
        包含每个放映场次详细信息的列表
    """
    logger.info(f"获取电影详情页的放映场次详细信息: {movie_url}")
    
    page = await browser.new_page()
    detailed_showtimes = []
    
    try:
        await page.goto(movie_url, timeout=30000)
        await page.wait_for_load_state("networkidle")
        
        detailed_showtimes = await extract_showtimes_with_details(page)
        logger.info(f"从电影详情页获取了 {len(detailed_showtimes)} 个放映场次的详细信息")
        
        # 格式化日期为YYYY-MM-DD格式
        for showtime in detailed_showtimes:
            if "date" in showtime:
                try:
                    # 尝试解析日期格式
                    date_text = showtime["date"]
                    # 常见格式：'Friday March 28, 2025' 或 'March 28'
                    for fmt in ["%A %B %d, %Y", "%B %d, %Y", "%B %d"]:
                        try:
                            # 如果缺少年份，添加当前年份
                            if "%Y" not in fmt:
                                current_year = datetime.now().year
                                parsed_date = datetime.strptime(f"{date_text}, {current_year}", f"{fmt}, %Y")
                            else:
                                parsed_date = datetime.strptime(date_text, fmt)
                            
                            showtime["date"] = parsed_date.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                except Exception as e:
                    logger.warning(f"无法解析日期格式: {showtime.get('date')}, 错误: {str(e)}")
    
    except Exception as e:
        logger.error(f"获取放映场次详细信息时出错: {str(e)}")
    
    finally:
        await page.close()
    
    return detailed_showtimes

@log_execution_time
async def main():
    """主函数"""
    try:
        logger.info(f"{EMOJI['start']} 开始爬取Metrograph电影信息...")
        await scrape_metrograph_all()
    except Exception as e:
        logger.error(f"{EMOJI['error']} 爬取过程中出现严重错误: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        
# 添加一个函数，用于清理旧的日志文件
def cleanup_old_logs(log_dir, max_age_days=30):
    """清理旧的日志文件"""
    try:
        current_time = datetime.now()
        log_path = Path(log_dir)
        
        # 只处理 .log 文件
        for log_file in log_path.glob("*.log*"):
            file_age = current_time - datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_age.days > max_age_days:
                logger.info(f"清理旧日志文件: {log_file}")
                log_file.unlink()
                
    except Exception as e:
        logger.error(f"清理旧日志时出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")

if __name__ == "__main__":
    # 在启动时打印系统信息
    process = psutil.Process(os.getpid())
    logger.info(f"开始程序，进程ID: {os.getpid()}")
    logger.info(f"Python版本: {sys.version}")
    logger.info(f"操作系统: {sys.platform}")
    logger.info(f"初始内存使用: {process.memory_info().rss / 1024 / 1024:.2f} MB")
    
    # 启动爬虫
    asyncio.run(main())