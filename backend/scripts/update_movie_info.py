#!/usr/bin/env python
"""
Script to update movie information from external APIs.
"""
import sys
import os
import argparse
from pathlib import Path

# Add the parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.models.movie import Movie
from app.services.movie_updater import MovieUpdater

def update_all_movies():
    """
    Update all movies without complete information.
    """
    # First, get movies that need TMDB info
    movies_for_tmdb = Movie.get_movies_without_tmdb()
    print(f"Found {len(movies_for_tmdb)} movies needing TMDB updates")
    
    # Update TMDB info
    updated_tmdb_count = 0
    for movie in movies_for_tmdb:
        movie_id = movie['id']
        title_en = movie['title_en']
        year = movie['year']
        director = movie['director']
        title_cn = movie['title_cn']
        
        print(f"\nProcessing: {title_en or title_cn} (ID: {movie_id})")
        
        # 使用智能搜索方法
        tmdb_movie = MovieUpdater.search_movie_with_variants(title_en or title_cn, year, director)
        
        if tmdb_movie:
            print(f"Found in TMDB: {tmdb_movie.get('title')} (ID: {tmdb_movie.get('id')})")
            movie_details = MovieUpdater.get_movie_details(tmdb_movie.get('id'))
            if movie_details:
                if MovieUpdater.update_movie_with_tmdb(movie_id, tmdb_movie, movie_details, director):
                    updated_tmdb_count += 1

    # Get movies without director or IMDb ID
    movies_without_info = Movie.get_movies_without_director_or_imdb()
    print(f"\nFound {len(movies_without_info)} movies needing director or IMDb ID updates")
    
    # Update missing information using OMDb
    updated_omdb_count = 0
    for movie in movies_without_info:
        movie_id = movie['id']
        title_en = movie['title_en']
        year = movie['year']
        title_cn = movie['title_cn']
        
        print(f"\nProcessing: {title_en or title_cn} (ID: {movie_id})")
        
        # Try to get OMDb data
        omdb_data = None
        if title_en:
            omdb_data = MovieUpdater.get_omdb_info(title_en, year)
            
        if not omdb_data and title_cn and not MovieUpdater.is_english(title_cn):
            omdb_data = MovieUpdater.get_omdb_info(title_cn, year)
        
        if omdb_data:
            if MovieUpdater.update_movie_with_omdb(movie_id, omdb_data):
                updated_omdb_count += 1

    # Process movies without Chinese overview
    movies_without_cn_overview = Movie.get_movies_without_cn_overview()
    print(f"\nFound {len(movies_without_cn_overview)} movies needing Chinese overview updates")
    
    # Update Chinese overviews
    updated_overview_count = 0
    for movie in movies_without_cn_overview:
        movie_id = movie['id']
        
        print(f"\nUpdating Chinese overview for movie ID {movie_id}")
        if MovieUpdater.update_chinese_overview(movie_id):
            updated_overview_count += 1

    # 检查标题为英文的中文标题
    movies_with_english_cn_title = []
    all_movies = Movie.get_all_movies(limit=999999)
    for movie in all_movies:
        if movie.get('title_cn') and MovieUpdater.is_english(movie.get('title_cn')):
            movies_with_english_cn_title.append(movie)
    
    print(f"\n找到 {len(movies_with_english_cn_title)} 部电影的中文标题实际是英文")
    english_cn_updated = 0
    for movie in movies_with_english_cn_title:
        movie_id = movie['id']
        title_en = movie['title_en']
        
        # 如果中文标题是英文，使用英文标题作为中文标题
        if title_en:
            print(f"\n更新电影 {title_en} (ID: {movie_id}) 的中文标题")
            result = Movie.update_movie(movie_id, {'title_cn': title_en})
            if result:
                english_cn_updated += 1
    
    # Print summary
    print("\nUpdate summary:")
    print(f"- TMDB updates: {updated_tmdb_count}")
    print(f"- OMDb updates: {updated_omdb_count}")
    print(f"- Chinese overview updates: {updated_overview_count}")
    print(f"- English Chinese title corrected: {english_cn_updated}")
    print(f"- Total updated movies: {updated_tmdb_count + updated_omdb_count + updated_overview_count + english_cn_updated}")

def main():
    """
    Main function.
    """
    parser = argparse.ArgumentParser(description='Update movie information from external APIs')
    parser.add_argument('--all', action='store_true', help='Update all movies')
    args = parser.parse_args()
    
    if args.all:
        update_all_movies()
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 