#!/usr/bin/env python
"""
刷新'Antiporno'电影的中文信息
"""
import os
import sys
import sqlite3
import requests
import time

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend", "movies.db")

# TMDB API信息
TMDB_API_KEY = "7423601ab4ef1d83b7e7e5fa279db0c5"  # 使用您项目中实际的API密钥
TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_movie_info(movie_id):
    """从数据库获取电影信息"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM movies WHERE id = ?", (movie_id,))
    movie = cursor.fetchone()
    conn.close()
    
    if movie:
        return dict(movie)
    return None

def search_movie_by_imdb(imdb_id):
    """通过IMDb ID在TMDB搜索电影"""
    url = f"{TMDB_BASE_URL}/find/{imdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "zh-CN",
        "external_source": "imdb_id"
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        movie_results = data.get("movie_results", [])
        if movie_results:
            return movie_results[0]
    
    return None

def search_movie_by_title(title, year=None):
    """通过标题在TMDB搜索电影"""
    url = f"{TMDB_BASE_URL}/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "language": "zh-CN",
        "include_adult": "true"
    }
    
    if year:
        params["year"] = year
        
    response = requests.get(url, params=params)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            return results[0]
    
    return None

def get_movie_details(tmdb_id):
    """获取电影详细信息"""
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "append_to_response": "external_ids,videos,credits",
        "language": "zh-CN"  # 获取中文详情
    }
    
    response = requests.get(url, params=params)
    zh_details = None
    if response.status_code == 200:
        zh_details = response.json()
    
    # 再获取英文详情
    params["language"] = "en-US"
    response = requests.get(url, params=params)
    en_details = None
    if response.status_code == 200:
        en_details = response.json()
    
    if zh_details and en_details:
        # 添加英文概述到中文详情
        if "overview" in en_details:
            zh_details["overview_en"] = en_details["overview"]
        
        # 获取导演信息
        if "credits" in zh_details and "crew" in zh_details["credits"]:
            directors = [person for person in zh_details["credits"]["crew"] if person["job"] == "Director"]
            if directors:
                zh_details["zh_directors"] = directors
                
                # 获取导演的英文名
                if "credits" in en_details and "crew" in en_details["credits"]:
                    en_directors = [person for person in en_details["credits"]["crew"] if person["job"] == "Director"]
                    zh_details["en_directors"] = en_directors
                
        return zh_details
    
    return None

def update_movie_info(movie_id, tmdb_data, movie_details, original_director):
    """使用TMDB数据更新电影信息"""
    # 提取数据
    title_cn = tmdb_data.get("title", "")
    overview_cn = tmdb_data.get("overview", "")
    overview_en = movie_details.get("overview_en", "")
    vote_average = tmdb_data.get("vote_average")
    poster_path = tmdb_data.get("poster_path")
    tmdb_id = tmdb_data.get("id")
    imdb_id = None
    
    # 从外部ID获取IMDb ID
    if "external_ids" in movie_details and movie_details["external_ids"].get("imdb_id"):
        imdb_id = movie_details["external_ids"].get("imdb_id")
    
    # 获取导演信息
    director_en = original_director  # 如果没有新数据，保留原始导演
    director_cn = None
    
    if "zh_directors" in movie_details and movie_details["zh_directors"]:
        director_cn = ", ".join([d.get("name", "") for d in movie_details["zh_directors"]])
    
    if "en_directors" in movie_details and movie_details["en_directors"]:
        director_en = ", ".join([d.get("name", "") for d in movie_details["en_directors"]])
    
    # 获取预告片URL
    trailer_url = None
    if "videos" in movie_details and movie_details["videos"].get("results"):
        trailers = [v for v in movie_details["videos"]["results"] if v.get("type") == "Trailer" and v.get("site") == "YouTube"]
        if trailers:
            trailer_key = trailers[0].get("key")
            if trailer_key:
                trailer_url = f"https://www.youtube.com/watch?v={trailer_key}"
    
    # 如果有海报路径，创建图片URL
    image_url = None
    if poster_path:
        image_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
    
    # 更新数据库
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"更新电影 ID {movie_id}:")
    print(f"  中文标题: {title_cn}")
    print(f"  中文概述: {overview_cn[:50]}..." if overview_cn else "  中文概述: 无")
    print(f"  中文导演: {director_cn}")
    
    cursor.execute("""
        UPDATE movies
        SET 
            title_cn = ?,
            overview_cn = ?,
            overview_en = COALESCE(?, overview_en),
            rating = COALESCE(?, rating),
            image_url = COALESCE(?, image_url),
            tmdb_id = COALESCE(?, tmdb_id),
            imdb_id = COALESCE(?, imdb_id),
            director = COALESCE(?, director),
            director_cn = ?,
            trailer_url = COALESCE(?, trailer_url)
        WHERE id = ?
    """, (
        title_cn, overview_cn, overview_en, vote_average, image_url, 
        tmdb_id, imdb_id, director_en, director_cn, trailer_url, movie_id
    ))
    
    conn.commit()
    conn.close()
    
    print(f"已成功更新电影 ID {movie_id} 的TMDB数据")
    return True

def main():
    movie_id = 21  # Antiporno电影的ID
    
    # 获取电影信息
    movie = get_movie_info(movie_id)
    if not movie:
        print(f"未找到电影ID {movie_id}")
        return
    
    print(f"处理电影: {movie['title_en']} (ID: {movie_id})")
    print(f"当前中文标题: {movie['title_cn']}")
    print(f"当前IMDb ID: {movie['imdb_id']}")
    print(f"当前TMDB ID: {movie['tmdb_id']}")
    
    # 尝试通过IMDb ID查找
    tmdb_movie = None
    if movie['imdb_id']:
        print(f"尝试通过IMDb ID {movie['imdb_id']} 查找...")
        tmdb_movie = search_movie_by_imdb(movie['imdb_id'])
    
    # 如果IMDb ID查找失败，尝试通过标题查找
    if not tmdb_movie:
        # 尝试不同的标题变体
        title_variations = [
            "Antiporno",
            "Anti Porno",
            "アンチポルノ",  # 日文标题
            "安蒂波诺",      # 可能的音译
            "反色情",        # 可能的直译
            "アンチポルノ Antiporno" # 日文+英文
        ]
        
        for title in title_variations:
            print(f"尝试通过标题 '{title}' 查找...")
            tmdb_movie = search_movie_by_title(title, movie.get('year'))
            if tmdb_movie:
                print(f"找到匹配: {tmdb_movie.get('title')} (ID: {tmdb_movie.get('id')})")
                break
    
    if tmdb_movie:
        print(f"在TMDB找到: {tmdb_movie.get('title')} (ID: {tmdb_movie.get('id')})")
        movie_details = get_movie_details(tmdb_movie.get('id'))
        if movie_details:
            update_movie_info(movie_id, tmdb_movie, movie_details, movie.get('director'))
        else:
            print("无法获取电影详情")
    else:
        print("在TMDB中找不到此电影")

if __name__ == "__main__":
    main() 