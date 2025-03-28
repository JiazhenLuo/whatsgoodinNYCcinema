#!/usr/bin/env python
"""
脚本用于修复数据库中所有电影的中文标题
主要处理两个问题：
1. 如果中文标题实际上是英文，则使用英文标题代替
2. 尝试使用智能搜索找到缺失的中文标题
"""
import os
import sys
import time
from pathlib import Path

# 添加项目根目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.models.movie import Movie
from app.services.movie_updater import MovieUpdater

def fix_english_chinese_titles():
    """
    修复所有中文标题实际上是英文的情况
    """
    print("=== 检查中文标题实际上是英文的情况 ===")
    
    # 获取所有电影
    all_movies = Movie.get_all_movies(limit=9999)
    print(f"数据库中共有 {len(all_movies)} 部电影")
    
    # 筛选出中文标题是英文的电影
    movies_with_english_cn_title = []
    for movie in all_movies:
        # 中文标题存在并且是英文，同时英文标题也存在
        if (movie.get('title_cn') and MovieUpdater.is_english(movie.get('title_cn')) 
            and movie.get('title_en') and movie.get('title_cn') != movie.get('title_en')):
            movies_with_english_cn_title.append(movie)
    
    print(f"发现 {len(movies_with_english_cn_title)} 部电影的中文标题实际上是英文且与英文标题不同")
    
    # 更新这些电影的中文标题
    updated_count = 0
    for movie in movies_with_english_cn_title:
        movie_id = movie['id']
        title_en = movie['title_en']
        title_cn = movie['title_cn']
        
        if title_en:
            print(f"\n修复电影 ID {movie_id}: 英文标题='{title_en}', 当前中文标题='{title_cn}'")
            # 使用原始英文标题（保留所有格式元素）作为中文标题
            result = Movie.update_movie(movie_id, {'title_cn': title_en})
            if result:
                print(f"✓ 更新中文标题为原始英文标题")
                updated_count += 1
    
    print(f"\n共修复了 {updated_count} 部电影的中文标题")
    return updated_count

def find_missing_chinese_titles():
    """
    为缺少中文标题的电影查找中文标题
    """
    print("\n=== 查找缺少中文标题的电影 ===")
    
    # 获取所有没有中文标题的电影
    conn = MovieUpdater.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM movies WHERE title_cn IS NULL OR title_cn = ''")
    movies_without_cn_title = [dict(movie) for movie in cursor.fetchall()]
    conn.close()
    
    print(f"发现 {len(movies_without_cn_title)} 部电影没有中文标题")
    
    # 尝试使用TMDB查找中文标题
    updated_count = 0
    for movie in movies_without_cn_title:
        movie_id = movie['id']
        title_en = movie['title_en']
        year = movie['year']
        director = movie['director']
        
        print(f"\n处理电影 ID {movie_id}: '{title_en}'")
        
        # 使用多种搜索策略查找电影
        tmdb_movie = None
        
        # 1. 使用TMDB ID直接获取
        if movie.get('tmdb_id'):
            tmdb_id = movie.get('tmdb_id')
            print(f"使用TMDB ID {tmdb_id} 查找")
            tmdb_movie = MovieUpdater.get_movie_by_tmdb_id(tmdb_id)
        
        # 2. 使用IMDb ID查找
        if not tmdb_movie and movie.get('imdb_id'):
            imdb_id = movie.get('imdb_id')
            print(f"使用IMDb ID {imdb_id} 查找")
            tmdb_movie = MovieUpdater.search_movie_by_imdb(imdb_id)
        
        # 3. 使用标题搜索变体
        if not tmdb_movie and title_en:
            # 在搜索时使用search_clean_title处理过的标题，但在显示和更新时使用原始标题
            print(f"使用标题变体搜索: '{title_en}' (搜索用清理后标题: '{MovieUpdater.search_clean_title(title_en)}')")
            tmdb_movie = MovieUpdater.search_movie_with_variants(title_en, year, director)
        
        if tmdb_movie:
            # 获取详细信息
            movie_details = MovieUpdater.get_movie_details(tmdb_movie.get('id'))
            if movie_details:
                # 检查TMDB返回的标题是否是英文
                found_title_cn = tmdb_movie.get('title', '')
                if found_title_cn and MovieUpdater.is_english(found_title_cn):
                    print(f"TMDB返回了英文标题 '{found_title_cn}'，将使用原始英文标题 '{title_en}' 作为中文标题")
                    # 直接手动更新中文标题为英文标题，保留所有格式
                    Movie.update_movie(movie_id, {'title_cn': title_en})
                    print(f"✓ 更新中文标题: '{title_en}'")
                    updated_count += 1
                else:
                    # 使用TMDB更新，其中会处理英文标题的情况
                    if MovieUpdater.update_movie_with_tmdb(movie_id, tmdb_movie, movie_details, director):
                        updated_movie = Movie.get_movie_by_id(movie_id)
                        if updated_movie.get('title_cn'):
                            print(f"✓ 更新中文标题: '{updated_movie.get('title_cn')}'")
                            updated_count += 1
                        else:
                            print("✗ 未能获取中文标题")
        else:
            print("✗ 在TMDB中未找到电影，使用原始英文标题作为中文标题")
            # 使用原始英文标题作为中文标题
            if title_en:
                Movie.update_movie(movie_id, {'title_cn': title_en})
                print(f"✓ 更新中文标题为原始英文标题: '{title_en}'")
                updated_count += 1
        
        # 添加延迟避免API限制
        time.sleep(0.5)
    
    print(f"\n共更新了 {updated_count} 部电影的中文标题")
    return updated_count

def refresh_existing_chinese_titles():
    """
    尝试刷新已有但可能不准确的中文标题
    """
    print("\n=== 刷新现有中文标题 ===")
    
    # 获取已有中文标题但可能需要刷新的电影（中文标题与英文标题相同的）
    conn = MovieUpdater.get_db_connection()
    cursor = conn.cursor()
    # 使用search_clean_title进行相似度比较，但保留原始格式
    cursor.execute("""
        SELECT m1.* FROM movies m1
        WHERE (
            m1.title_cn = m1.title_en 
            OR (
                m1.title_cn IS NOT NULL 
                AND m1.title_cn != '' 
                AND m1.title_en IS NOT NULL
                AND m1.title_en != ''
                AND m1.title_cn LIKE m1.title_en
            )
        )
    """)
    movies_with_same_titles = [dict(movie) for movie in cursor.fetchall()]
    conn.close()
    
    print(f"发现 {len(movies_with_same_titles)} 部电影的中文标题与英文标题相同或相似")
    
    # 尝试使用TMDB刷新这些电影的中文标题
    updated_count = 0
    for movie in movies_with_same_titles:
        movie_id = movie['id']
        title_en = movie['title_en']
        title_cn = movie['title_cn']
        year = movie['year']
        director = movie['director']
        
        print(f"\n处理电影 ID {movie_id}: '{title_en}' (当前中文标题: '{title_cn}')")
        
        # 使用TMDB ID直接获取
        tmdb_movie = None
        if movie.get('tmdb_id'):
            tmdb_id = movie.get('tmdb_id')
            print(f"使用TMDB ID {tmdb_id} 查找")
            tmdb_movie = MovieUpdater.get_movie_by_tmdb_id(tmdb_id)
        
        # 如果没有找到，使用搜索变体
        if not tmdb_movie:
            tmdb_movie = MovieUpdater.search_movie_with_variants(title_en, year, director)
        
        if tmdb_movie:
            # 获取详细信息
            movie_details = MovieUpdater.get_movie_details(tmdb_movie.get('id'))
            if movie_details:
                # 只有当找到的中文标题与英文标题不同时才更新
                found_title_cn = tmdb_movie.get('title', '')
                
                # 比较标题时使用search_clean_title清理后比较
                cleaned_found_title = MovieUpdater.search_clean_title(found_title_cn)
                cleaned_en_title = MovieUpdater.search_clean_title(title_en)
                
                if (found_title_cn and 
                    not MovieUpdater.is_english(found_title_cn) and 
                    cleaned_found_title != cleaned_en_title):
                    
                    if MovieUpdater.update_movie_with_tmdb(movie_id, tmdb_movie, movie_details, director):
                        updated_movie = Movie.get_movie_by_id(movie_id)
                        print(f"✓ 更新中文标题: '{title_cn}' -> '{updated_movie.get('title_cn')}'")
                        updated_count += 1
                else:
                    print("✗ 找到的中文标题仍然是英文或与英文标题相同")
        else:
            print("✗ 在TMDB中未找到电影")
        
        # 添加延迟避免API限制
        time.sleep(0.5)
    
    print(f"\n共刷新了 {updated_count} 部电影的中文标题")
    return updated_count

def main():
    """
    主函数
    """
    print("开始修复中文标题问题...")
    
    # 1. 修复英文中文标题
    fixed_english_count = fix_english_chinese_titles()
    
    # 2. 查找缺失的中文标题
    found_missing_count = find_missing_chinese_titles()
    
    # 3. 刷新现有中文标题
    refreshed_count = refresh_existing_chinese_titles()
    
    # 打印总结
    print("\n=== 总结 ===")
    print(f"- 修复英文中文标题: {fixed_english_count}")
    print(f"- 找到缺失中文标题: {found_missing_count}")
    print(f"- 刷新现有中文标题: {refreshed_count}")
    print(f"- 总共更新电影数: {fixed_english_count + found_missing_count + refreshed_count}")

if __name__ == "__main__":
    main() 