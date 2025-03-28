#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import time
import requests
from urllib.parse import quote
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 配置
TMDB_API_KEY = os.environ.get('TMDB_API_KEY', '')  # 从环境变量获取API密钥
if not TMDB_API_KEY:
    logger.error("未设置TMDB_API_KEY环境变量")
    exit(1)

# 文件路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
METROGRAPH_MOVIES_FILE = os.path.join(DATABASE_DIR, 'metrograph_movies.json')

# TMDB API URLs
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_MOVIE_URL = "https://api.themoviedb.org/3/movie/"

def load_movies():
    """加载电影数据"""
    try:
        with open(METROGRAPH_MOVIES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载电影数据失败: {e}")
        return {}

def save_movies(movies):
    """保存电影数据"""
    try:
        with open(METROGRAPH_MOVIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(movies, f, ensure_ascii=False, indent=4)
        logger.info(f"已保存电影数据到 {METROGRAPH_MOVIES_FILE}")
    except Exception as e:
        logger.error(f"保存电影数据失败: {e}")

def search_movie_tmdb(title, year=None):
    """使用TMDB API搜索电影"""
    params = {
        'api_key': TMDB_API_KEY,
        'query': title,
        'language': 'en-US',
        'include_adult': 'false',
        'page': 1
    }
    
    if year:
        params['year'] = year
    
    try:
        response = requests.get(TMDB_SEARCH_URL, params=params)
        response.raise_for_status()
        results = response.json().get('results', [])
        return results[0] if results else None
    except Exception as e:
        logger.error(f"搜索电影 '{title}' 失败: {e}")
        return None

def get_movie_details(tmdb_id):
    """获取TMDB电影详细信息"""
    params = {
        'api_key': TMDB_API_KEY,
        'language': 'zh-CN',  # 请求中文数据
        'append_to_response': 'credits,external_ids'  # 包含演职人员和外部ID信息
    }
    
    try:
        response = requests.get(f"{TMDB_MOVIE_URL}{tmdb_id}", params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"获取电影详情失败 (TMDB ID: {tmdb_id}): {e}")
        return None

def get_directors(credits):
    """从演职人员中提取导演信息"""
    directors = []
    if credits and 'crew' in credits:
        directors = [person for person in credits['crew'] if person['job'] == 'Director']
    return directors

def update_movie_info(movie, force=False):
    """更新单个电影信息"""
    # 如果已经有TMDB ID和中文标题，且不强制更新，则跳过
    if not force and movie.get('tmdb_id') and movie.get('title_zh'):
        logger.debug(f"跳过已有信息的电影: {movie['title_en']}")
        return movie
    
    title = movie.get('title_en', '')
    year = movie.get('year')
    
    if not title:
        logger.warning("电影没有英文标题，跳过")
        return movie
    
    # 搜索TMDB
    search_result = search_movie_tmdb(title, year)
    if not search_result:
        logger.warning(f"未在TMDB找到电影: {title} ({year})")
        return movie
    
    # 设置TMDB ID
    tmdb_id = search_result.get('id')
    movie['tmdb_id'] = tmdb_id
    
    # 获取详细信息
    details = get_movie_details(tmdb_id)
    if not details:
        logger.warning(f"无法获取电影详情: {title} (TMDB ID: {tmdb_id})")
        return movie
    
    # 更新中文标题
    if details.get('title'):
        movie['title_zh'] = details.get('title')
    
    # 更新IMDb ID
    if 'external_ids' in details and details['external_ids'].get('imdb_id'):
        movie['imdb_id'] = details['external_ids']['imdb_id']
    
    # 更新导演中文名
    if 'credits' in details:
        directors = get_directors(details['credits'])
        if directors:
            # 保留原始导演名
            if 'director_en' not in movie and 'director' in movie:
                movie['director_en'] = movie['director']
            
            # 设置中文导演名
            director_names = [d['name'] for d in directors]
            movie['director'] = ', '.join(director_names)
    
    logger.info(f"已更新电影信息: {title} -> 中文标题: {movie.get('title_zh', '未找到')}")
    return movie

def update_all_movies(force=False):
    """更新所有电影信息"""
    movies = load_movies()
    total = len(movies)
    updated = 0
    
    logger.info(f"开始更新 {total} 部电影信息")
    
    for key, movie in movies.items():
        logger.info(f"处理电影 [{updated+1}/{total}]: {movie.get('title_en', '未知')}")
        movies[key] = update_movie_info(movie, force)
        updated += 1
        
        # 防止API请求过快
        time.sleep(0.5)
    
    # 保存更新后的数据
    save_movies(movies)
    logger.info(f"电影信息更新完成，共更新 {updated} 部电影")

def update_missing_info():
    """只更新缺少信息的电影"""
    movies = load_movies()
    missing_info = [key for key, movie in movies.items() 
                   if not movie.get('tmdb_id') or not movie.get('title_zh')]
    
    total = len(missing_info)
    updated = 0
    
    logger.info(f"开始更新 {total} 部缺少信息的电影")
    
    for key in missing_info:
        logger.info(f"处理电影 [{updated+1}/{total}]: {movies[key].get('title_en', '未知')}")
        movies[key] = update_movie_info(movies[key])
        updated += 1
        
        # 防止API请求过快
        time.sleep(0.5)
    
    # 保存更新后的数据
    save_movies(movies)
    logger.info(f"电影信息更新完成，共更新 {updated} 部电影")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='从TMDB API更新电影信息')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help='更新所有电影信息')
    group.add_argument('--missing', action='store_true', help='只更新缺少信息的电影')
    parser.add_argument('--force', action='store_true', help='强制更新已有信息')
    
    args = parser.parse_args()
    
    if args.all:
        update_all_movies(args.force)
    elif args.missing:
        update_missing_info()
    else:
        # 默认只更新缺少信息的电影
        update_missing_info() 