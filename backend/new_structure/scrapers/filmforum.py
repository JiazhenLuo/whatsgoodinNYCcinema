import logging
import os
import re
import json
import asyncio
import traceback
import sys
from pathlib import Path
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Any

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page

# 设置日志
def setup_logger(log_file=None):
    """设置日志系统，同时输出到控制台和文件"""
    logger = logging.getLogger("filmforum_scraper")
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
        # 使用RotatingFileHandler替代FileHandler
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# 设置日志文件路径
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"filmforum_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# 初始化日志
logger = setup_logger(log_file)

# 设置URL和输出路径
BASE_URL = "https://filmforum.org"
NOW_PLAYING_URL = f"{BASE_URL}/now_playing"
OUTPUT_DIR = "database"
OUTPUT_FILE = "filmforum_movies.json"

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
    "loading": "⏳"
}

async def scrape_filmforum():
    """爬取Film Forum电影信息的主函数"""
    logger.info(f"{EMOJI['start']} 开始爬取Film Forum电影信息...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            
            # 访问放映中页面
            logger.info(f"访问Film Forum电影页面: {NOW_PLAYING_URL}")
            await page.goto(NOW_PLAYING_URL, timeout=60000)
            await page.wait_for_load_state("networkidle")
            
            # 解析HTML
            html_content = await page.content()
            soup = BeautifulSoup(html_content, "html.parser")
            
            # 找到放映时间表容器
            showtimes_table = soup.find("div", class_="module showtimes-table")
            if not showtimes_table:
                logger.error(f"{EMOJI['error']} 未找到放映时间表容器")
                return []
            
            # 获取当前日期和星期
            today = datetime.now()
            weekdays = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
            logger.info(f"当前日期: {today.strftime('%Y-%m-%d')}")
            
            # 找到当前活动的标签页(星期几)
            active_tab = showtimes_table.find("li", class_="ui-tabs-active")
            if not active_tab:
                logger.error(f"{EMOJI['error']} 未找到活动标签页")
                return []
                
            active_weekday = None
            for day_class in active_tab["class"]:
                if day_class.lower() in weekdays:
                    active_weekday = day_class.lower()
                    break
            
            if not active_weekday:
                logger.error(f"{EMOJI['error']} 未找到活动星期")
                return []
                
            logger.info(f"当前活动星期: {active_weekday}")
            
            # 计算日期映射
            date_mapping = {}
            active_index = weekdays.index(active_weekday)
            for i, day in enumerate(weekdays):
                # 计算与活动日的偏移量
                offset = (i - active_index) % 7
                target_date = today + timedelta(days=offset)
                date_mapping[day] = target_date.strftime("%Y-%m-%d")
            
            logger.info(f"日期映射: {date_mapping}")
            
            # 提取所有电影
            movie_dict = {}  # 用于存储电影信息，以标题为键
            
            # 获取所有标签页内容
            tabs = showtimes_table.find_all("div", id=lambda x: x and x.startswith("tabs-"))
            logger.info(f"找到 {len(tabs)} 个标签页")
            
            for tab_index, tab in enumerate(tabs):
                tab_id = tab.get("id", "")
                logger.info(f"处理标签页 {tab_index+1}/{len(tabs)}: {tab_id}")
                
                # 找到对应的星期几标签
                weekday_li = showtimes_table.find("li", attrs={"aria-controls": tab_id})
                if not weekday_li:
                    logger.warning(f"{EMOJI['warning']} 未找到标签页 {tab_id} 对应的星期标签")
                    continue
                    
                weekday = None
                for day_class in weekday_li["class"]:
                    if day_class.lower() in weekdays:
                        weekday = day_class.lower()
                        break
                
                if not weekday:
                    logger.warning(f"{EMOJI['warning']} 未找到标签页 {tab_id} 对应的星期")
                    continue
                
                # 获取日期
                date_str = date_mapping.get(weekday, "Unknown")
                logger.info(f"  星期 {weekday} 对应日期: {date_str}")
                
                # 解析这个标签页中的所有电影
                current_series = None
                movie_count = 0
                
                for p in tab.find_all("p"):
                    # 检查是否是系列名称
                    series_link = p.find("a", href=lambda x: x and "series" in x)
                    if series_link and not p.find("strong"):
                        current_series = series_link.text.strip()
                        logger.info(f"  找到系列: {current_series}")
                        continue
                    
                    # 获取电影链接和标题
                    movie_link = p.find("strong")
                    if not movie_link:
                        continue
                    
                    movie_a = movie_link.find("a")
                    if not movie_a:
                        continue
                    
                    # 获取电影基本信息
                    movie_url = movie_a.get("href", "")
                    movie_title = movie_a.text.strip()
                    
                    # 处理导演信息(如果在标题行中)
                    director = None
                    director_links = p.find_all("a")
                    # 检查第一个不是电影标题的链接是否可能是导演
                    for link in director_links:
                        if link.text.strip() != movie_title and ("'" in link.text or "'" in link.text):
                            director = link.text.strip().replace("'s", "").replace("'s", "")
                            break
                    
                    # 获取放映时间
                    times = []
                    for span in p.find_all("span"):
                        # 跳过提示信息
                        if span.get("class") and "alert" in span.get("class"):
                            continue
                        
                        time_text = span.text.strip()
                        if re.match(r'\d{1,2}:\d{2}', time_text):
                            # 构建一个更可靠的票务URL
                            safe_title = movie_title.lower().replace(' ', '-').replace("'", "").replace(":", "").replace("?", "")
                            ticket_url = f"https://my.filmforum.org/tickets/{safe_title}"
                            
                            # 检查是否售罄
                            sold_out = "sold out" in span.text.lower()
                            
                            times.append({
                                "time": time_text,
                                "ticket_url": ticket_url,
                                "sold_out": sold_out
                            })
                    
                    if not times:
                        logger.warning(f"  电影 '{movie_title}' 未找到放映时间")
                        continue
                        
                    logger.info(f"  找到电影: {movie_title} ({len(times)} 场放映)")
                    
                    # 将电影添加到字典或更新现有条目
                    if movie_title in movie_dict:
                        # 检查是否已有该日期
                        date_exists = False
                        for d in movie_dict[movie_title]["show_dates"]:
                            if d["date"] == date_str:
                                # 合并放映时间，避免重复
                                existing_times = {t["time"] for t in d["times"]}
                                for new_time in times:
                                    if new_time["time"] not in existing_times:
                                        d["times"].append(new_time)
                                        existing_times.add(new_time["time"])
                                date_exists = True
                                break
                        
                        if not date_exists:
                            movie_dict[movie_title]["show_dates"].append({
                                "date": date_str,
                                "times": times
                            })
                    else:
                        # 创建新电影条目
                        movie_dict[movie_title] = {
                            "title_en": movie_title,
                            "detail_url": movie_url if movie_url.startswith("http") else f"{BASE_URL}{movie_url}",
                            "director": director,
                            "series": current_series,
                            "cinema": "Film Forum",
                            "show_dates": [{
                                "date": date_str,
                                "times": times
                            }]
                        }
                        movie_count += 1
                
                logger.info(f"  在 {date_str} 找到 {movie_count} 部电影")
            
            # 访问每部电影的详情页获取更多信息
            await enrich_movies_details(browser, movie_dict)
            
            # 将字典值转换为列表
            movies = list(movie_dict.values())
            logger.info(f"{EMOJI['success']} 爬取完成，共获取 {len(movies)} 部电影信息")
            
            return movies
            
        except Exception as e:
            logger.error(f"{EMOJI['error']} 爬取Film Forum电影信息时出错: {str(e)}")
            logger.error(traceback.format_exc())
            return []
        finally:
            await browser.close()

async def enrich_movies_details(browser, movie_dict):
    """访问每部电影的详情页，补充详细信息"""
    logger.info(f"{EMOJI['info']} 开始补充电影详细信息...")
    
    page = await browser.new_page()
    
    try:
        movie_count = len(movie_dict)
        for i, (title, movie) in enumerate(movie_dict.items()):
            try:
                logger.info(f"[{i+1}/{movie_count}] 获取电影详情: {title}")
                
                # 访问详情页
                detail_url = movie["detail_url"]
                logger.info(f"  访问: {detail_url}")
                
                await page.goto(detail_url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                detail_soup = BeautifulSoup(await page.content(), "html.parser")
                
                # 获取主标题
                main_title = detail_soup.find("h2", class_="main-title")
                if main_title:
                    movie["title_en"] = main_title.text.strip()
                    logger.info(f"  确认标题: {movie['title_en']}")
                
                # 方法1: 尝试从slideshow中获取图片
                slideshow = detail_soup.find("ul", class_="slides")
                if slideshow and slideshow.find("li") and slideshow.find("li").find("img"):
                    img_url = slideshow.find("li").find("img").get("src", "")
                    if img_url:
                        movie["image_url"] = img_url if img_url.startswith("http") else f"{BASE_URL}{img_url}"
                        logger.info(f"  找到slideshow图片")
                
                # 方法2：如果slideshow中没有图片，尝试从hero-image区域获取
                if not movie.get("image_url"):
                    hero_img = detail_soup.find("div", class_="hero-image")
                    if hero_img and hero_img.find("img"):
                        img_url = hero_img.find("img").get("src", "")
                        if img_url:
                            movie["image_url"] = img_url if img_url.startswith("http") else f"{BASE_URL}{img_url}"
                            logger.info(f"  找到hero-image图片")
                
                # 方法3：尝试从页面上任何位置获取电影相关图片
                if not movie.get("image_url"):
                    # 查找任何可能的img标签，筛选出与电影标题相关的
                    all_imgs = detail_soup.find_all("img")
                    for img in all_imgs:
                        img_src = img.get("src", "")
                        img_alt = img.get("alt", "")
                        # 检查图片是否与电影标题相关
                        if (title.lower() in img_src.lower() or 
                            (img_alt and title.lower() in img_alt.lower())):
                            movie["image_url"] = img_src if img_src.startswith("http") else f"{BASE_URL}{img_src}"
                            logger.info(f"  找到相关图片")
                            break
                
                # 从urgent区域获取导演信息
                urgent_div = detail_soup.find("div", class_="urgent")
                if urgent_div and urgent_div.find("p"):
                    urgent_text = urgent_div.find("p").text.strip()
                    if "DIRECTED BY" in urgent_text:
                        movie["director"] = urgent_text.replace("DIRECTED BY", "").strip()
                        logger.info(f"  导演: {movie['director']}")
                    else:
                        movie["note"] = urgent_text
                        logger.info(f"  特别说明: {movie['note']}")
                
                # 从copy区域获取更多详细信息
                copy_div = detail_soup.find("div", class_="copy")
                if copy_div:
                    # 查找第一个段落，它可能包含年份、时长等信息
                    first_p = copy_div.find("p")
                    if first_p:
                        p_text = first_p.text.strip()
                        
                        # 提取年份
                        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', p_text)
                        if year_match:
                            movie["year"] = int(year_match.group(1))
                            logger.info(f"  年份: {movie['year']}")
                        
                        # 提取时长
                        duration_match = re.search(r'(\d+)\s*min\.', p_text, re.IGNORECASE)
                        if duration_match:
                            movie["duration"] = f"{duration_match.group(1)} min"
                            logger.info(f"  时长: {movie['duration']}")
                        
                        # 提取语言
                        language_match = re.search(r'In\s+([A-Za-z]+)\s+with', p_text, re.IGNORECASE)
                        if language_match:
                            movie["language"] = language_match.group(1)
                            logger.info(f"  语言: {movie['language']}")
                    
                    # 获取简介(通常是第二个段落)
                    p_tags = copy_div.find_all("p")
                    if len(p_tags) > 1:
                        movie["overview_en"] = p_tags[1].text.strip()
                        logger.info(f"  找到电影简介")
                        
                    # 检查是否有Q&A或介绍场次
                    movie["has_qa"] = False
                    movie["has_introduction"] = False
                    
                    # 查找页面中所有段落，检查是否包含Q&A或介绍信息
                    all_p = detail_soup.find_all("p")
                    for p in all_p:
                        p_text = p.text.lower()
                        if ("q&a" in p_text or "discussion" in p_text or "conversation" in p_text):
                            movie["has_qa"] = True
                            movie["qa_details"] = p.text.strip()
                            logger.info(f"  发现Q&A场次: {movie['qa_details']}")
                        elif ("introduce" in p_text or "introduction" in p_text or "presented by" in p_text):
                            movie["has_introduction"] = True
                            movie["introduction_details"] = p.text.strip()
                            logger.info(f"  发现介绍场次: {movie['introduction_details']}")
                
                # 获取预告片链接
                trailer = detail_soup.find("div", class_="flex-video")
                if trailer and trailer.find("iframe"):
                    movie["trailer_url"] = trailer.find("iframe").get("src", "")
                    logger.info(f"  找到预告片链接")
                
                # 随机延迟，避免请求过快
                delay = 1 + (i % 2)  # 1-2秒之间
                logger.info(f"  {EMOJI['loading']} 等待 {delay} 秒...")
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"  {EMOJI['error']} 获取电影 '{title}' 详情时出错: {str(e)}")
                logger.error(traceback.format_exc())
    
    except Exception as e:
        logger.error(f"{EMOJI['error']} 补充电影详情时出现严重错误: {str(e)}")
        logger.error(traceback.format_exc())
    
    finally:
        await page.close()

async def main():
    """主函数"""
    try:
        # 爬取电影信息
        movies = await scrape_filmforum()
        
        if not movies:
            logger.error(f"{EMOJI['error']} 未获取到有效的电影数据")
            return
        
        # 创建保存目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        database_dir = os.path.join(os.path.dirname(script_dir), OUTPUT_DIR)
        os.makedirs(database_dir, exist_ok=True)
        
        # 检查是否有现有数据，如果有则完全替换
        output_path = os.path.join(database_dir, OUTPUT_FILE)
        
        # 保存JSON - 直接使用新爬取的数据，不再合并旧数据
        # 这样可以避免重复数据
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(movies, f, indent=4, ensure_ascii=False)
        
        # 统计信息
        total_movies = len(movies)
        total_dates = set()
        total_showtimes = 0
        
        for movie in movies:
            for date in movie.get("show_dates", []):
                total_dates.add(date["date"])
                total_showtimes += len(date.get("times", []))
        
        logger.info(f"{EMOJI['success']} 爬取完成，数据已存入 {os.path.abspath(output_path)}")
        logger.info(f"  - 总计: {total_movies} 部电影")
        logger.info(f"  - 总放映日期: {len(total_dates)} 个")
        logger.info(f"  - 总放映场次: {total_showtimes} 场")
    
    except Exception as e:
        logger.error(f"{EMOJI['error']} 处理过程中出现严重错误: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main()) 