from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timedelta
import os

def scrape_filmforum_with_playwright():
    url = "https://filmforum.org/now_playing"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # 运行无头浏览器
        page = browser.new_page()
        page.goto(url, timeout=60000)  # 访问 Film Forum 电影页面
        page.wait_for_load_state("networkidle")  # 等待 JavaScript 完全加载
        html_content = page.content()  # 获取完整 HTML
        
        # 解析 HTML
        soup = BeautifulSoup(html_content, "html.parser")
        movies = []
        
        # 找到放映时间表容器
        showtimes_table = soup.find("div", class_="module showtimes-table")
        if not showtimes_table:
            print("未找到放映时间表容器")
            browser.close()
            return []
        
        # 获取当前日期和星期
        today = datetime.now()
        weekdays = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        
        # 找到当前活动的标签页(星期几)
        active_tab = showtimes_table.find("li", class_="ui-tabs-active")
        active_weekday = None
        for day_class in active_tab["class"]:
            if day_class.lower() in weekdays:
                active_weekday = day_class.lower()
                break
        
        # 计算日期映射
        date_mapping = {}
        active_index = weekdays.index(active_weekday)
        for i, day in enumerate(weekdays):
            # 计算与活动日的偏移量
            offset = (i - active_index) % 7
            target_date = today + timedelta(days=offset)
            date_mapping[day] = target_date.strftime("%Y-%m-%d")
        
        # 提取所有电影
        movie_dict = {}  # 用于存储电影信息，以标题为键
        
        # 获取所有标签页内容
        tabs = showtimes_table.find_all("div", id=lambda x: x and x.startswith("tabs-"))
        
        for tab in tabs:
            tab_id = tab.get("id", "")
            
            # 找到对应的星期几标签
            weekday_li = showtimes_table.find("li", attrs={"aria-controls": tab_id})
            weekday = None
            for day_class in weekday_li["class"]:
                if day_class.lower() in weekdays:
                    weekday = day_class.lower()
                    break
            
            # 获取日期
            date_str = date_mapping.get(weekday, "Unknown")
            
            # 解析这个标签页中的所有电影
            current_series = None
            for p in tab.find_all("p"):
                # 检查是否是系列名称
                series_link = p.find("a", href=lambda x: x and "series" in x)
                if series_link and not p.find("strong"):
                    current_series = series_link.text.strip()
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
                director_link = p.find("a")
                if director_link and "'" in director_link.text and director_link.text.strip() != movie_title:
                    director = director_link.text.strip().replace("'s", "")
                
                # 获取放映时间
                times = []
                for span in p.find_all("span"):
                    # 跳过提示信息
                    if span.get("class") and "alert" in span.get("class"):
                        continue
                    
                    time_text = span.text.strip()
                    if re.match(r'\d{1,2}:\d{2}', time_text):
                        times.append({
                                "time": time_text,
                            "ticket_url": f"https://my.filmforum.org/tickets/{movie_title.lower().replace(' ', '-')}",
                            "sold_out": False  # Film Forum页面不显示售罄状态，默认为False
                        })
                
                # 将电影添加到字典或更新现有条目
                if movie_title in movie_dict:
                    # 检查是否已有该日期
                    date_exists = False
                    for d in movie_dict[movie_title]["show_dates"]:
                        if d["date"] == date_str:
                            d["times"] = times
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
                        "detail_url": movie_url if movie_url.startswith("http") else f"https://filmforum.org{movie_url}",
                        "director": director,
                        "series": current_series,
                "cinema": "Film Forum",
                        "show_dates": [{
                            "date": date_str,
                            "times": times
                        }]
            }
            
        # 访问每部电影的详情页获取更多信息
        for title, movie in movie_dict.items():
            try:
                print(f"获取电影详情: {title}")
                page.goto(movie["detail_url"], timeout=60000)
                page.wait_for_load_state("networkidle")
                
                detail_soup = BeautifulSoup(page.content(), "html.parser")
                    
                # 获取主标题
                main_title = detail_soup.find("h2", class_="main-title")
                if main_title:
                    movie["title_en"] = main_title.text.strip()
                
                # 方法1: 尝试从slideshow中获取图片
                slideshow = detail_soup.find("ul", class_="slides")
                if slideshow and slideshow.find("li") and slideshow.find("li").find("img"):
                    img_url = slideshow.find("li").find("img").get("src", "")
                    if img_url:
                        movie["image_url"] = img_url if img_url.startswith("http") else f"https://filmforum.org{img_url}"
                
                # 方法2：如果slideshow中没有图片，尝试从hero-image区域获取
                if not movie.get("image_url"):
                    hero_img = detail_soup.find("div", class_="hero-image")
                    if hero_img and hero_img.find("img"):
                        img_url = hero_img.find("img").get("src", "")
                        if img_url:
                            movie["image_url"] = img_url if img_url.startswith("http") else f"https://filmforum.org{img_url}"
                
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
                            movie["image_url"] = img_src if img_src.startswith("http") else f"https://filmforum.org{img_src}"
                            break
                    
                # 从urgent区域获取导演信息
                urgent_div = detail_soup.find("div", class_="urgent")
                if urgent_div and urgent_div.find("p"):
                    urgent_text = urgent_div.find("p").text.strip()
                    if "DIRECTED BY" in urgent_text:
                        movie["director"] = urgent_text.replace("DIRECTED BY", "").strip()
                    else:
                        movie["note"] = urgent_text
                    
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
                        
                        # 提取时长
                        duration_match = re.search(r'(\d+)\s*min\.', p_text, re.IGNORECASE)
                        if duration_match:
                            movie["duration"] = f"{duration_match.group(1)} min"
                        
                        # 提取语言
                        language_match = re.search(r'In\s+([A-Za-z]+)\s+with', p_text, re.IGNORECASE)
                        if language_match:
                            movie["language"] = language_match.group(1)
                    
                    # 获取简介(通常是第二个段落)
                    if len(copy_div.find_all("p")) > 1:
                        movie["overview_en"] = copy_div.find_all("p")[1].text.strip()
                
                # 获取预告片链接
                trailer = detail_soup.find("div", class_="flex-video")
                if trailer and trailer.find("iframe"):
                    movie["trailer_url"] = trailer.find("iframe").get("src", "")
                
            except Exception as e:
                print(f"获取电影 '{title}' 详情时出错: {str(e)}")
        
        # 将字典值转换为列表
        movies = list(movie_dict.values())
        
        browser.close()
        return movies

# 运行爬虫
print("开始爬取 Film Forum 电影信息...")
movies = scrape_filmforum_with_playwright()

# 创建保存目录
database_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database")
if not os.path.exists(database_dir):
    os.makedirs(database_dir)

# 存为 JSON
output_path = os.path.join(database_dir, "filmforum_movies.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(movies, f, indent=4, ensure_ascii=False)

print(f"✅ 爬取完成，数据已存入 {output_path}（共 {len(movies)} 部电影）") 