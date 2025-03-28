#!/usr/bin/env python
"""
批量更新所有电影的中文标题和描述
该脚本使用多种策略来查找和匹配TMDB上的中文电影信息
"""
import os
import sys
import sqlite3
import requests
import time
import json
from pathlib import Path

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend", "movies.db")

# TMDB API信息
TMDB_API_KEY = "7423601ab4ef1d83b7e7e5fa279db0c5"  # 请使用项目中实际的API密钥
TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_movies_needing_chinese_titles():
    """获取需要更新中文标题的电影"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 获取所有中文标题不存在或为MakingofXXX格式的电影
    cursor.execute("""
        SELECT id, title_en, title_cn, year, director, imdb_id, tmdb_id
        FROM movies
        WHERE 
            title_cn IS NULL OR 
            title_cn = '' OR 
            title_cn LIKE 'Making%' OR
            title_cn LIKE '%of%' OR
            title_cn = title_en
        ORDER BY id
    """)
    
    movies = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return movies

def search_movie_by_imdb(imdb_id):
    """通过IMDb ID在TMDB搜索电影"""
    if not imdb_id:
        return None
        
    url = f"{TMDB_BASE_URL}/find/{imdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "zh-CN",
        "external_source": "imdb_id"
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            movie_results = data.get("movie_results", [])
            if movie_results:
                return movie_results[0]
    except Exception as e:
        print(f"通过IMDb ID搜索时出错: {e}")
    
    return None

def search_movie_by_tmdb(tmdb_id):
    """直接通过TMDB ID获取电影信息"""
    if not tmdb_id:
        return None
        
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "zh-CN"
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"通过TMDB ID搜索时出错: {e}")
    
    return None

def clean_title(title):
    """清理标题，移除特殊字符和括号内容"""
    if not title:
        return ""
        
    # 移除括号内容
    import re
    title = re.sub(r'\s*\[.*?\]\s*', ' ', title)
    title = re.sub(r'\s*\(.*?\)\s*', ' ', title)
    
    # 移除特殊字符
    title = re.sub(r'[^\w\s]', ' ', title)
    
    # 处理多余空格
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title

def search_movie_by_title(title, year=None):
    """通过标题在TMDB搜索电影"""
    if not title:
        return None
        
    original_title = title
    clean_title_text = clean_title(title)
    
    url = f"{TMDB_BASE_URL}/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": clean_title_text,
        "language": "zh-CN",
        "include_adult": "true"
    }
    
    if year:
        params["year"] = year
    
    try:
        # 首先尝试使用清理后的标题和年份
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]
                
        # 如果有年份但未找到，尝试不使用年份限制
        if year:
            params.pop("year")
            response = requests.get(url, params=params)
            if response.status_code == 200:
                results = response.json().get("results", [])
                if results:
                    return results[0]
                    
        # 尝试使用原始标题
        params["query"] = original_title
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]
                
        # 尝试取标题的前两个单词
        words = clean_title_text.split()
        if len(words) > 1:
            simple_title = " ".join(words[:2])
            params["query"] = simple_title
            response = requests.get(url, params=params)
            if response.status_code == 200:
                results = response.json().get("results", [])
                if results:
                    return results[0]
                    
    except Exception as e:
        print(f"通过标题搜索时出错: {e}")
    
    return None

def get_movie_details(tmdb_id):
    """获取电影详细信息，包括中英文版本"""
    if not tmdb_id:
        return None
        
    # 获取中文详情
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "append_to_response": "external_ids,videos,credits",
        "language": "zh-CN"
    }
    
    zh_details = None
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            zh_details = response.json()
    except Exception as e:
        print(f"获取中文电影详情时出错: {e}")
    
    # 获取英文详情
    params["language"] = "en-US"
    en_details = None
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            en_details = response.json()
    except Exception as e:
        print(f"获取英文电影详情时出错: {e}")
    
    if zh_details:
        # 合并有用的英文信息到中文详情中
        if en_details:
            # 添加英文概述
            if "overview" in en_details and en_details["overview"]:
                zh_details["overview_en"] = en_details["overview"]
            
            # 如果中文概述为空但英文不为空，复制英文到中文
            if (not zh_details.get("overview") or zh_details.get("overview") == "") and en_details.get("overview"):
                zh_details["overview"] = f"[自动翻译待确认] {en_details.get('overview')[:100]}..."
        
        # 处理导演信息
        if "credits" in zh_details and "crew" in zh_details["credits"]:
            directors = [person for person in zh_details["credits"]["crew"] if person["job"] == "Director"]
            if directors:
                zh_details["zh_directors"] = directors
                
                # 获取导演的英文名
                if en_details and "credits" in en_details and "crew" in en_details["credits"]:
                    en_directors = [person for person in en_details["credits"]["crew"] if person["job"] == "Director"]
                    zh_details["en_directors"] = en_directors
                
        return zh_details
    
    return None

def update_movie_chinese_info(movie_id, tmdb_data, movie_details, original_info):
    """更新电影的中文信息"""
    if not tmdb_data or not movie_id:
        return False
        
    # 提取数据
    title_cn = tmdb_data.get("title", "")
    
    # 如果返回的中文标题与英文标题相同，尝试用原始电影标题
    if title_cn == tmdb_data.get("original_title") and "中国" not in title_cn and "华语" not in title_cn:
        original_title = original_info.get("title_en", "")
        if ":" in original_title:
            title_cn = original_title.split(":")[0] + "：" + title_cn
            
    overview_cn = tmdb_data.get("overview", "")
    overview_en = movie_details.get("overview_en", "")
    vote_average = tmdb_data.get("vote_average")
    poster_path = tmdb_data.get("poster_path")
    tmdb_id = str(tmdb_data.get("id", ""))
    
    # 获取IMDb ID
    imdb_id = None
    if "external_ids" in movie_details and movie_details["external_ids"].get("imdb_id"):
        imdb_id = movie_details["external_ids"].get("imdb_id")
    
    # 处理导演信息
    director_en = original_info.get("director")  # 保留原始导演
    director_cn = None
    
    if "zh_directors" in movie_details and movie_details["zh_directors"]:
        director_cn = ", ".join([d.get("name", "") for d in movie_details["zh_directors"]])
    
    if "en_directors" in movie_details and movie_details["en_directors"]:
        director_en = ", ".join([d.get("name", "") for d in movie_details["en_directors"]])
    
    # 获取预告片
    trailer_url = None
    if "videos" in movie_details and movie_details["videos"].get("results"):
        trailers = [v for v in movie_details["videos"]["results"] if v.get("type") == "Trailer" and v.get("site") == "YouTube"]
        if trailers:
            trailer_key = trailers[0].get("key")
            if trailer_key:
                trailer_url = f"https://www.youtube.com/watch?v={trailer_key}"
    
    # 处理图片URL
    image_url = None
    if poster_path:
        image_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
    
    # 更新数据库
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    update_fields = []
    update_values = []
    
    # 只更新有值的字段
    if title_cn:
        update_fields.append("title_cn = ?")
        update_values.append(title_cn)
        
    if overview_cn:
        update_fields.append("overview_cn = ?")
        update_values.append(overview_cn)
        
    if overview_en:
        update_fields.append("overview_en = COALESCE(?, overview_en)")
        update_values.append(overview_en)
        
    if vote_average:
        update_fields.append("rating = COALESCE(?, rating)")
        update_values.append(vote_average)
        
    if image_url:
        update_fields.append("image_url = COALESCE(?, image_url)")
        update_values.append(image_url)
        
    if tmdb_id:
        update_fields.append("tmdb_id = COALESCE(?, tmdb_id)")
        update_values.append(tmdb_id)
        
    if imdb_id:
        update_fields.append("imdb_id = COALESCE(?, imdb_id)")
        update_values.append(imdb_id)
        
    if director_en:
        update_fields.append("director = COALESCE(?, director)")
        update_values.append(director_en)
        
    if director_cn:
        update_fields.append("director_cn = ?")
        update_values.append(director_cn)
        
    if trailer_url:
        update_fields.append("trailer_url = COALESCE(?, trailer_url)")
        update_values.append(trailer_url)
    
    if update_fields:
        query = f"UPDATE movies SET {', '.join(update_fields)} WHERE id = ?"
        update_values.append(movie_id)
        
        cursor.execute(query, update_values)
        conn.commit()
        
        print(f"更新电影 ID {movie_id}:")
        print(f"  中文标题: {title_cn}")
        if overview_cn:
            print(f"  中文概述: {overview_cn[:50]}...")
        else:
            print("  中文概述: 无")
        print(f"  中文导演: {director_cn}")
        
        conn.close()
        return True
    
    conn.close()
    return False

def save_special_case(movie_id, title_en, special_title_cn):
    """保存特殊情况的中文标题"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE movies
        SET title_cn = ?
        WHERE id = ?
    """, (special_title_cn, movie_id))
    
    conn.commit()
    conn.close()
    print(f"已手动更新电影 ID {movie_id} '{title_en}' 的中文标题为 '{special_title_cn}'")
    return True

def main():
    # 获取需要更新的电影
    movies = get_movies_needing_chinese_titles()
    print(f"找到 {len(movies)} 部电影需要更新中文标题")
    
    # 特殊情况映射表
    special_cases = {
        "Antiporno": "反色情",
        "Weird Medicine Shorts": "奇怪医学短片系列",
        "This Long Century Presents: Ari Marcopoulos": "本世纪长镜头：阿里·马尔科普洛斯",
        "Wicked Games - Rimini Sparta": "恶作剧 - 里米尼斯巴达",
        "Making of Antiporno": "反色情",
        "Julie Keeps Quiet": "保持沉默的朱莉",
        # 在此添加更多特殊情况
    }
    
    updated_count = 0
    for movie in movies:
        movie_id = movie["id"]
        title_en = movie["title_en"]
        year = movie["year"]
        title_cn = movie["title_cn"]
        
        print(f"\n处理电影: {title_en} (ID: {movie_id}, 当前中文: {title_cn})")
        
        # 检查是否是特殊情况
        for key, value in special_cases.items():
            if key.lower() in title_en.lower():
                save_special_case(movie_id, title_en, value)
                updated_count += 1
                continue
                
        # 策略1: 使用TMDB ID直接获取
        tmdb_movie = None
        if movie["tmdb_id"]:
            print(f"尝试通过TMDB ID {movie['tmdb_id']} 查找...")
            tmdb_movie = search_movie_by_tmdb(movie["tmdb_id"])
        
        # 策略2: 使用IMDb ID查找
        if not tmdb_movie and movie["imdb_id"]:
            print(f"尝试通过IMDb ID {movie['imdb_id']} 查找...")
            tmdb_movie = search_movie_by_imdb(movie["imdb_id"])
        
        # 策略3: 使用标题查找
        if not tmdb_movie:
            print(f"尝试通过标题 '{title_en}' 查找...")
            tmdb_movie = search_movie_by_title(title_en, year)
        
        # 如果找到了电影，更新信息
        if tmdb_movie:
            print(f"在TMDB找到: {tmdb_movie.get('title')} (ID: {tmdb_movie.get('id')})")
            movie_details = get_movie_details(tmdb_movie.get('id'))
            if movie_details:
                if update_movie_chinese_info(movie_id, tmdb_movie, movie_details, movie):
                    updated_count += 1
                time.sleep(0.5)  # 避免API速率限制
            else:
                print(f"无法获取电影ID {movie_id} '{title_en}' 的详细信息")
        else:
            print(f"在TMDB中找不到电影ID {movie_id} '{title_en}'")
    
    print(f"\n总共更新了 {updated_count}/{len(movies)} 部电影的中文信息")

if __name__ == "__main__":
    main() 