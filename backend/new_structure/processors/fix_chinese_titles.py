#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import logging
import re

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 文件路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
MOVIES_FILES = [
    os.path.join(DATABASE_DIR, 'metrograph_movies.json'),
    os.path.join(DATABASE_DIR, 'filmforum_movies.json')
]

def load_json_file(file_path):
    """加载JSON文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载文件失败 {file_path}: {e}")
        return None

def save_json_file(data, file_path):
    """保存JSON文件"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"已保存文件 {file_path}")
        return True
    except Exception as e:
        logger.error(f"保存文件失败 {file_path}: {e}")
        return False

def is_english(text):
    """检查文本是否主要是英文"""
    if not text:
        return True
    
    # 计算英文字符和非英文字符的比例
    english_chars = len(re.findall(r'[a-zA-Z]', text))
    total_chars = len(text.strip())
    
    # 如果80%以上是英文字符，认为是英文文本
    return (english_chars / total_chars > 0.8) if total_chars > 0 else True

def fix_chinese_title_format(movies_data, is_list=False):
    """修复中文标题的格式问题"""
    if is_list:
        # 处理电影列表格式
        for movie in movies_data:
            if 'title_zh' in movie and is_english(movie['title_zh']):
                logger.info(f"电影 '{movie.get('title_en', '')}' 的中文标题实际是英文: '{movie['title_zh']}'")
                # 如果中文标题是英文，使用英文标题
                if movie.get('title_en'):
                    movie['title_zh'] = movie['title_en']
                    logger.info(f"已将中文标题设为与英文标题相同")
    else:
        # 处理电影字典格式（键为电影标题）
        for key, movie in movies_data.items():
            if 'title_zh' in movie and is_english(movie['title_zh']):
                logger.info(f"电影 '{movie.get('title_en', '')}' 的中文标题实际是英文: '{movie['title_zh']}'")
                # 如果中文标题是英文，使用英文标题
                if movie.get('title_en'):
                    movie['title_zh'] = movie['title_en']
                    logger.info(f"已将中文标题设为与英文标题相同")
    
    return movies_data

def process_all_files():
    """处理所有电影数据文件"""
    for file_path in MOVIES_FILES:
        if not os.path.exists(file_path):
            logger.warning(f"文件不存在: {file_path}")
            continue
        
        logger.info(f"正在处理文件: {file_path}")
        
        # 加载数据
        data = load_json_file(file_path)
        if not data:
            continue
        
        # 判断数据结构并修复
        if isinstance(data, list):
            logger.info(f"检测到列表格式数据")
            fixed_data = fix_chinese_title_format(data, is_list=True)
        else:
            logger.info(f"检测到字典格式数据")
            fixed_data = fix_chinese_title_format(data, is_list=False)
        
        # 保存修复后的数据
        if fixed_data:
            save_json_file(fixed_data, file_path)

if __name__ == "__main__":
    logger.info("开始修复中文标题格式...")
    process_all_files()
    logger.info("中文标题格式修复完成") 