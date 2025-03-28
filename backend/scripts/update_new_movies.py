#!/usr/bin/env python
"""
Script to update information for newly added movies.
"""
import sys
import os
import argparse
from pathlib import Path
import time

# Add the parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.models.movie import Movie
from app.services.movie_updater import MovieUpdater
from app.utils.douban import generate_douban_search_url
from app.utils.letterboxd import generate_letterboxd_url

def update_recent_movies(days=7):
    """
    Update information for movies added in the last specified number of days.
    
    Args:
        days: Number of days to look back
    """
    # Get recent movies
    recent_movies = Movie.get_recent_movies(days)
    print(f"Found {len(recent_movies)} movies added in the last {days} days")
    
    # Keep track of movies needing updates
    movies_needing_updates = []
    
    # Check which movies need updates
    for movie in recent_movies:
        movie_id = movie['id']
        title_en = movie['title_en']
        title_cn = movie['title_cn']
        
        # Get the full movie data to check completeness
        full_movie = Movie.get_movie_by_id(movie_id)
        
        # Check if movie needs updates
        needs_update = (
            not full_movie.get('tmdb_id') or
            not full_movie.get('imdb_id') or
            not full_movie.get('director') or
            not full_movie.get('overview_en') or
            not full_movie.get('overview_cn') or
            (full_movie.get('title_cn') and MovieUpdater.is_english(full_movie.get('title_cn')))
        )
        
        if needs_update:
            print(f"Movie '{title_en or title_cn}' (ID: {movie_id}) needs updates")
            movies_needing_updates.append(movie)
    
    print(f"\nFound {len(movies_needing_updates)} recent movies needing updates")
    
    # Update movies
    updated_tmdb_count = 0
    updated_omdb_count = 0
    updated_overview_count = 0
    
    for movie in movies_needing_updates:
        movie_id = movie['id']
        title_en = movie['title_en']
        year = movie.get('year')
        director = movie.get('director')
        title_cn = movie.get('title_cn')
        
        print(f"\nProcessing: {title_en or title_cn} (ID: {movie_id})")
        
        # 使用智能搜索方法
        tmdb_movie = MovieUpdater.search_movie_with_variants(title_en or title_cn, year, director)
        
        if tmdb_movie:
            print(f"Found in TMDB: {tmdb_movie.get('title')} (ID: {tmdb_movie.get('id')})")
            movie_details = MovieUpdater.get_movie_details(tmdb_movie.get('id'))
            if movie_details:
                if MovieUpdater.update_movie_with_tmdb(movie_id, tmdb_movie, movie_details, director):
                    updated_tmdb_count += 1
                    
                    # Brief delay to avoid API rate limits
                    time.sleep(0.5)
        
        # Get updated movie data
        updated_movie = Movie.get_movie_by_id(movie_id)
        
        # Try to get OMDb data
        omdb_data = None
        if updated_movie.get('imdb_id'):
            omdb_data = MovieUpdater.get_omdb_info(None, None, updated_movie.get('imdb_id'))
        
        if not omdb_data and title_en:
            omdb_data = MovieUpdater.get_omdb_info(title_en, year)
        
        if omdb_data:
            if MovieUpdater.update_movie_with_omdb(movie_id, omdb_data):
                updated_omdb_count += 1
                time.sleep(0.5)
        
        # Update Chinese overview if needed
        if updated_movie.get('overview_en') and not updated_movie.get('overview_cn'):
            if MovieUpdater.update_chinese_overview(movie_id):
                updated_overview_count += 1
                time.sleep(0.5)
    
    # Print summary
    print("\nUpdate summary:")
    print(f"- Updated with TMDB: {updated_tmdb_count}")
    print(f"- Updated with OMDb: {updated_omdb_count}")
    print(f"- Updated Chinese overviews: {updated_overview_count}")
    print(f"- Total updated movies: {updated_tmdb_count + updated_omdb_count + updated_overview_count}")

def main():
    """
    Main function.
    """
    parser = argparse.ArgumentParser(description='Update movie information')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look back for recent movies')
    args = parser.parse_args()
    
    update_recent_movies(days=args.days)

if __name__ == "__main__":
    main() 