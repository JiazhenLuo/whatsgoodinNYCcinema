#!/usr/bin/env python
"""
修复数据库中的中文文本格式问题

这个脚本清理SQLite数据库中中文文本的格式问题，包括：
1. 移除Unicode特殊字符（如\u3000）
2. 标准化换行符和空格
3. 修复中英文混合场景下的格式问题
"""
import sqlite3
import re
import os
import logging
from argparse import ArgumentParser

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fix_unicode')

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "movies.db")

def fix_unicode_escapes(text):
    """
    修复文本中的Unicode格式问题
    """
    if not text:
        return text
    
    # 修复Unicode特殊字符
    if '\u3000' in text:  # 修复全角空格
        text = text.replace('\u3000', '')
    
    # 修复所有可能的换行符变体
    text = text.replace('\\r', ' ')
    text = text.replace('\\n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')
    
    # 修复常见的空格问题
    text = re.sub(r'\s+', ' ', text)  # 替换多个空格为单个空格
    
    # 删除标点符号周围的多余空格
    text = re.sub(r'\s+([，。！？,\.!?:;；：])', r'\1', text)
    
    # 修复括号周围的空格
    text = re.sub(r'\(\s+', '(', text)
    text = re.sub(r'\s+\)', ')', text)
    
    # 修复中英文混合场景下的空格
    # 匹配模式如 "甘茨 Bruno" 修复为 "甘茨Bruno"
    text = re.sub(r'([^\s(（]+)\s+([A-Za-z])', r'\1\2', text)
    
    # 修复 "多马丁 Solveig" 为 "多马丁Solveig"
    text = re.sub(r'([\u4e00-\u9fff][^\s]*)\s+([A-Za-z])', r'\1\2', text)
    
    # 修复中文特殊标点符号周围的空格
    text = re.sub(r'([•·])\s+', r'\1', text)
    text = re.sub(r'\s+([•·])', r'\1', text)
    
    # 最终清理
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.strip()
    
    return text

def fix_database_unicode():
    """修复数据库中的Unicode问题"""
    logger.info(f"正在连接到数据库: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 修复 overview_cn 字段
    logger.info('修复中文电影简介...')
    cursor.execute('SELECT id, overview_cn FROM movies WHERE overview_cn IS NOT NULL')
    overview_fixed_count = 0
    
    for movie_id, overview_cn in cursor.fetchall():
        if overview_cn:
            fixed_overview = fix_unicode_escapes(overview_cn)
            if fixed_overview != overview_cn:
                cursor.execute('UPDATE movies SET overview_cn = ? WHERE id = ?', 
                              (fixed_overview, movie_id))
                overview_fixed_count += 1
                logger.info(f"已修复电影ID {movie_id} 的简介")
    
    # 修复 title_cn 字段
    logger.info('修复中文电影标题...')
    cursor.execute('SELECT id, title_cn FROM movies WHERE title_cn IS NOT NULL')
    title_fixed_count = 0
    
    for movie_id, title_cn in cursor.fetchall():
        if title_cn:
            fixed_title = fix_unicode_escapes(title_cn)
            if fixed_title != title_cn:
                cursor.execute('UPDATE movies SET title_cn = ? WHERE id = ?', 
                              (fixed_title, movie_id))
                title_fixed_count += 1
                logger.info(f"已修复电影ID {movie_id} 的标题")
    
    # 修复 director_cn 字段
    logger.info('修复中文导演名称...')
    cursor.execute('SELECT id, director_cn FROM movies WHERE director_cn IS NOT NULL')
    director_fixed_count = 0
    
    for movie_id, director_cn in cursor.fetchall():
        if director_cn:
            fixed_director = fix_unicode_escapes(director_cn)
            if fixed_director != director_cn:
                cursor.execute('UPDATE movies SET director_cn = ? WHERE id = ?', 
                              (fixed_director, movie_id))
                director_fixed_count += 1
                logger.info(f"已修复电影ID {movie_id} 的导演名称")
    
    # 提交更改
    conn.commit()
    conn.close()
    
    logger.info(f'修复结果统计:')
    logger.info(f'- 中文简介: {overview_fixed_count} 条')
    logger.info(f'- 中文标题: {title_fixed_count} 条')
    logger.info(f'- 中文导演: {director_fixed_count} 条')
    logger.info('数据库中文格式问题修复完成!')

def main():
    """主函数"""
    parser = ArgumentParser(description='修复数据库中的中文字符格式问题')
    parser.add_argument('--db', help='数据库文件路径 (默认: backend/movies.db)')
    
    args = parser.parse_args()
    
    global DB_PATH
    if args.db:
        DB_PATH = args.db
        logger.info(f"使用指定的数据库: {DB_PATH}")
    
    fix_database_unicode()

if __name__ == "__main__":
    main() 