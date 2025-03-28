#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import logging
import re
import unicodedata

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

def normalize_text(text):
    """规范化文本，修复Unicode编码问题"""
    if not text:
        return text
    
    # 使用NFC正规化，最常用的Unicode组合形式
    normalized_text = unicodedata.normalize('NFC', text)
    
    # 修复可能的显示问题
    normalized_text = normalized_text.replace('\u2019', '\'')  # 替换右单引号
    normalized_text = normalized_text.replace('\u2018', '\'')  # 替换左单引号
    normalized_text = normalized_text.replace('\u201c', '"')   # 替换左双引号
    normalized_text = normalized_text.replace('\u201d', '"')   # 替换右双引号
    normalized_text = normalized_text.replace('\u2014', '-')   # 替换破折号
    normalized_text = normalized_text.replace('\u2013', '-')   # 替换连字符
    normalized_text = normalized_text.replace('\u00a0', ' ')   # 替换不间断空格
    
    return normalized_text

def fix_unicode_issues(movies_data, is_list=False):
    """修复中文文本的Unicode编码问题"""
    text_fields = ['title_en', 'title_zh', 'director', 'overview', 'overview_en', 'overview_zh', 'qa_details', 'introduction_details']
    fixed_count = 0
    
    if is_list:
        # 处理电影列表格式
        for movie in movies_data:
            for field in text_fields:
                if field in movie and movie[field]:
                    original_text = movie[field]
                    normalized_text = normalize_text(original_text)
                    if normalized_text != original_text:
                        movie[field] = normalized_text
                        fixed_count += 1
                        logger.debug(f"已修复 '{field}' 字段: {original_text[:30]}... -> {normalized_text[:30]}...")
    else:
        # 处理电影字典格式（键为电影标题）
        for key, movie in movies_data.items():
            for field in text_fields:
                if field in movie and movie[field]:
                    original_text = movie[field]
                    normalized_text = normalize_text(original_text)
                    if normalized_text != original_text:
                        movie[field] = normalized_text
                        fixed_count += 1
                        logger.debug(f"已修复 '{field}' 字段: {original_text[:30]}... -> {normalized_text[:30]}...")
    
    logger.info(f"已修复 {fixed_count} 个文本字段的Unicode问题")
    return movies_data

def process_all_files():
    """处理所有电影数据文件"""
    total_fixed = 0
    
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
            fixed_data = fix_unicode_issues(data, is_list=True)
        else:
            logger.info(f"检测到字典格式数据")
            fixed_data = fix_unicode_issues(data, is_list=False)
        
        # 保存修复后的数据
        if fixed_data:
            save_json_file(fixed_data, file_path)

if __name__ == "__main__":
    logger.info("开始修复中文文本Unicode问题...")
    process_all_files()
    logger.info("Unicode问题修复完成") 