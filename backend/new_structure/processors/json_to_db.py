#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sqlite3
import logging
import sys
import re
from pathlib import Path
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 文件路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
DB_PATH = os.path.join(DATABASE_DIR, 'movies.db')
MOVIES_FILES = {
    'metrograph': os.path.join(DATABASE_DIR, 'metrograph_movies.json'),
    'filmforum': os.path.join(DATABASE_DIR, 'filmforum_movies.json')
}

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """初始化数据库结构"""
    logger.info("初始化数据库...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 创建电影表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title_en TEXT NOT NULL,
        title_zh TEXT,
        director TEXT,
        director_en TEXT,
        year INTEGER,
        duration TEXT,
        language TEXT,
        overview TEXT,
        overview_en TEXT,
        overview_zh TEXT,
        image_url TEXT,
        trailer_url TEXT,
        imdb_id TEXT,
        tmdb_id INTEGER,
        douban_url TEXT,
        letterboxd_url TEXT,
        cinema TEXT,
        has_qa BOOLEAN DEFAULT 0,
        qa_details TEXT,
        has_introduction BOOLEAN DEFAULT 0,
        introduction_details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建放映表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS screenings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id INTEGER,
        cinema TEXT,
        date TEXT,
        time TEXT,
        ticket_url TEXT,
        sold_out BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (movie_id) REFERENCES movies (id)
    )
    ''')
    
    # 创建更新触发器
    cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS update_movies_timestamp
    AFTER UPDATE ON movies
    BEGIN
        UPDATE movies SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;
    ''')
    
    cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS update_screenings_timestamp
    AFTER UPDATE ON screenings
    BEGIN
        UPDATE screenings SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;
    ''')
    
    # 提交并关闭
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成")

def load_json_file(file_path):
    """加载JSON文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载文件失败 {file_path}: {e}")
        return None

def extract_year(text):
    """从文本中提取年份"""
    if not text:
        return None
    
    # 尝试直接转换
    if isinstance(text, int):
        if 1900 <= text <= 2100:
            return text
    
    # 从字符串中提取年份
    try:
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', str(text))
        if year_match:
            return int(year_match.group(1))
    except:
        pass
    
    return None

def extract_duration(text):
    """从文本中提取时长"""
    if not text:
        return None
    
    # 标准化时长
    try:
        duration_match = re.search(r'(\d+)\s*min', str(text))
        if duration_match:
            return f"{duration_match.group(1)} min"
        else:
            return str(text)
    except:
        pass
    
    return text

def import_metrograph_data(json_data):
    """处理Metrograph格式的数据"""
    movies = []
    screenings = []
    
    # 检查数据是否为字典格式（键为电影标题）
    if isinstance(json_data, dict):
        for key, movie_data in json_data.items():
            movie = {
                'title_en': movie_data.get('title_en'),
                'title_zh': movie_data.get('title_zh'),
                'director': movie_data.get('director'),
                'director_en': movie_data.get('director_en'),
                'year': extract_year(movie_data.get('year')),
                'duration': extract_duration(movie_data.get('duration')),
                'language': movie_data.get('language'),
                'overview': movie_data.get('overview'),
                'overview_en': movie_data.get('overview_en'),
                'overview_zh': movie_data.get('overview_zh'),
                'image_url': movie_data.get('image_url'),
                'trailer_url': movie_data.get('trailer_url'),
                'imdb_id': movie_data.get('imdb_id'),
                'tmdb_id': movie_data.get('tmdb_id'),
                'cinema': 'Metrograph',
                'has_qa': 1 if movie_data.get('has_qa') else 0,
                'qa_details': movie_data.get('qa_details'),
                'has_introduction': 1 if movie_data.get('has_introduction') else 0,
                'introduction_details': movie_data.get('introduction_details')
            }
            movies.append(movie)
            
            # 处理放映信息
            if movie_data.get('show_dates'):
                for date_info in movie_data.get('show_dates'):
                    date_str = date_info.get('date')
                    if not date_str:
                        continue
                    
                    # 处理放映时间
                    if date_info.get('times'):
                        for time_info in date_info.get('times'):
                            screening = {
                                'movie_title': movie['title_en'],
                                'cinema': 'Metrograph',
                                'date': date_str,
                                'time': time_info.get('time'),
                                'ticket_url': time_info.get('ticket_url'),
                                'sold_out': 1 if time_info.get('sold_out') else 0
                            }
                            screenings.append(screening)
    
    return movies, screenings

def import_filmforum_data(json_data):
    """处理Film Forum格式的数据"""
    movies = []
    screenings = []
    
    # Film Forum数据通常是列表格式
    if isinstance(json_data, list):
        for movie_data in json_data:
            movie = {
                'title_en': movie_data.get('title_en'),
                'title_zh': movie_data.get('title_zh'),
                'director': movie_data.get('director'),
                'director_en': movie_data.get('director_en'),
                'year': extract_year(movie_data.get('year')),
                'duration': extract_duration(movie_data.get('duration')),
                'language': movie_data.get('language'),
                'overview': movie_data.get('overview_en'),  # Film Forum只有英文简介
                'overview_en': movie_data.get('overview_en'),
                'overview_zh': movie_data.get('overview_zh'),
                'image_url': movie_data.get('image_url'),
                'trailer_url': movie_data.get('trailer_url'),
                'imdb_id': movie_data.get('imdb_id'),
                'tmdb_id': movie_data.get('tmdb_id'),
                'cinema': 'Film Forum',
                'has_qa': 1 if movie_data.get('has_qa') else 0,
                'qa_details': movie_data.get('qa_details'),
                'has_introduction': 1 if movie_data.get('has_introduction') else 0,
                'introduction_details': movie_data.get('introduction_details')
            }
            movies.append(movie)
            
            # 处理放映信息
            if movie_data.get('show_dates'):
                for date_info in movie_data.get('show_dates'):
                    date_str = date_info.get('date')
                    if not date_str:
                        continue
                    
                    # 处理放映时间
                    if date_info.get('times'):
                        for time_info in date_info.get('times'):
                            time_str = time_info
                            ticket_url = None
                            sold_out = 0
                            
                            # 处理复杂的时间信息结构
                            if isinstance(time_info, dict):
                                time_str = time_info.get('time')
                                ticket_url = time_info.get('ticket_url')
                                sold_out = 1 if time_info.get('sold_out') else 0
                            
                            if time_str:
                                screening = {
                                    'movie_title': movie['title_en'],
                                    'cinema': 'Film Forum',
                                    'date': date_str,
                                    'time': time_str,
                                    'ticket_url': ticket_url,
                                    'sold_out': sold_out
                                }
                                screenings.append(screening)
    
    return movies, screenings

def insert_movie(conn, movie):
    """插入电影数据"""
    cursor = conn.cursor()
    
    # 检查电影是否已存在
    cursor.execute("SELECT id FROM movies WHERE title_en = ?", (movie['title_en'],))
    result = cursor.fetchone()
    
    if result:
        # 更新现有电影
        movie_id = result['id']
        update_fields = ', '.join([f"{key} = ?" for key in movie.keys() if key != 'title_en'])
        update_values = [movie[key] for key in movie.keys() if key != 'title_en']
        update_values.append(movie_id)
        
        cursor.execute(f"UPDATE movies SET {update_fields} WHERE id = ?", update_values)
        logger.info(f"更新电影: {movie['title_en']}")
        return movie_id
    else:
        # 插入新电影
        fields = ', '.join(movie.keys())
        placeholders = ', '.join(['?' for _ in movie.keys()])
        values = [movie[key] for key in movie.keys()]
        
        cursor.execute(f"INSERT INTO movies ({fields}) VALUES ({placeholders})", values)
        movie_id = cursor.lastrowid
        logger.info(f"添加电影: {movie['title_en']}")
        return movie_id

def insert_screening(conn, screening, movie_id_map):
    """插入放映信息"""
    cursor = conn.cursor()
    
    # 获取对应的电影ID
    movie_title = screening.pop('movie_title')
    movie_id = movie_id_map.get(movie_title)
    
    if not movie_id:
        logger.warning(f"未找到电影ID: {movie_title}")
        return None
    
    # 检查放映是否已存在
    cursor.execute(
        "SELECT id FROM screenings WHERE movie_id = ? AND date = ? AND time = ?", 
        (movie_id, screening['date'], screening['time'])
    )
    result = cursor.fetchone()
    
    if result:
        # 更新现有放映
        screening_id = result['id']
        update_fields = ', '.join([f"{key} = ?" for key in screening.keys()])
        update_values = [screening[key] for key in screening.keys()]
        update_values.append(screening_id)
        
        cursor.execute(f"UPDATE screenings SET {update_fields} WHERE id = ?", update_values)
        return screening_id
    else:
        # 插入新放映
        screening['movie_id'] = movie_id
        fields = ', '.join(screening.keys())
        placeholders = ', '.join(['?' for _ in screening.keys()])
        values = [screening[key] for key in screening.keys()]
        
        cursor.execute(f"INSERT INTO screenings ({fields}) VALUES ({placeholders})", values)
        return cursor.lastrowid

def process_all_files():
    """处理所有电影数据文件并导入到数据库"""
    # 初始化数据库
    init_db()
    
    # 连接数据库
    conn = get_db_connection()
    
    all_movies = []
    all_screenings = []
    movie_id_map = {}
    
    # 处理每个文件
    for source, file_path in MOVIES_FILES.items():
        if not os.path.exists(file_path):
            logger.warning(f"文件不存在: {file_path}")
            continue
        
        logger.info(f"正在处理 {source} 数据: {file_path}")
        
        # 加载数据
        data = load_json_file(file_path)
        if not data:
            continue
        
        # 根据来源处理数据
        if source == 'metrograph':
            movies, screenings = import_metrograph_data(data)
        elif source == 'filmforum':
            movies, screenings = import_filmforum_data(data)
        else:
            logger.warning(f"未知数据源: {source}")
            continue
        
        logger.info(f"从 {source} 提取了 {len(movies)} 部电影和 {len(screenings)} 场放映")
        
        # 添加到所有数据中
        all_movies.extend(movies)
        all_screenings.extend(screenings)
    
    # 开始事务
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # 插入电影数据
        for movie in all_movies:
            movie_id = insert_movie(conn, movie)
            if movie_id:
                movie_id_map[movie['title_en']] = movie_id
        
        # 插入放映数据
        screening_count = 0
        for screening in all_screenings:
            if insert_screening(conn, screening, movie_id_map):
                screening_count += 1
        
        # 提交事务
        conn.commit()
        logger.info(f"成功导入 {len(movie_id_map)} 部电影和 {screening_count} 场放映")
    
    except Exception as e:
        # 回滚事务
        conn.rollback()
        logger.error(f"导入数据出错: {e}")
    
    finally:
        # 关闭数据库连接
        conn.close()

if __name__ == "__main__":
    logger.info("开始导入电影数据到数据库...")
    process_all_files()
    logger.info("导入完成") 