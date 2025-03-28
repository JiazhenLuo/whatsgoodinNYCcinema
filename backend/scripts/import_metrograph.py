#!/usr/bin/env python
"""
Script to import Metrograph movie data from JSON file into the database.
"""
import sys
import os
import json
from pathlib import Path
from datetime import datetime
import sqlite3

# Add the parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.models.database import get_db_connection
from app.models.movie import Movie
from app.models.screening import Screening
from app.config.settings import DB_PATH

METROGRAPH_JSON_PATH = os.path.join(parent_dir, "database", "metrograph_movies.json")

def import_metrograph_data():
    """
    Import Metrograph movie data from JSON file into the database.
    """
    try:
        # Load Metrograph movie data from JSON file
        print(f"Loading data from {METROGRAPH_JSON_PATH}...")
        with open(METROGRAPH_JSON_PATH, 'r', encoding='utf-8') as f:
            movies_data = json.load(f)
        
        print(f"Found {len(movies_data)} movies to import")
        print(f"Using database: {DB_PATH}")
        
        # Explicitly set database path to avoid lock issues
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        imported_movies = 0
        imported_screenings = 0
        
        for movie_data in movies_data:
            title_en = movie_data.get('title_en')
            if not title_en:
                print(f"Skipping movie with no title: {movie_data}")
                continue
                
            # Check if movie already exists in the database
            cursor.execute("""
                SELECT id FROM movies 
                WHERE title_en = ? AND cinema = 'Metrograph'
            """, (title_en,))
            
            existing_movie = cursor.fetchone()
            
            if existing_movie:
                movie_id = existing_movie['id']
                print(f"Movie already exists: {title_en} (ID: {movie_id})")
                
                # Update existing movie with new data
                update_fields = []
                update_values = []
                
                if movie_data.get('director') and movie_data.get('director') != '':
                    update_fields.append("director = ?")
                    update_values.append(movie_data.get('director'))
                
                if movie_data.get('year'):
                    update_fields.append("year = ?")
                    update_values.append(movie_data.get('year'))
                
                if movie_data.get('overview_en') and movie_data.get('overview_en') != '':
                    update_fields.append("overview_en = ?")
                    update_values.append(movie_data.get('overview_en'))
                
                if movie_data.get('detail_url'):
                    update_fields.append("detail_url = ?")
                    update_values.append(movie_data.get('detail_url'))
                
                if movie_data.get('image_url'):
                    update_fields.append("image_url = ?")
                    update_values.append(movie_data.get('image_url'))
                
                if movie_data.get('trailer_url'):
                    update_fields.append("trailer_url = ?")
                    update_values.append(movie_data.get('trailer_url'))
                
                if update_fields:
                    update_query = f"""
                        UPDATE movies SET {', '.join(update_fields)}
                        WHERE id = ?
                    """
                    update_values.append(movie_id)
                    
                    cursor.execute(update_query, update_values)
                    conn.commit()
                    print(f"  - Updated movie: {title_en} with new data")
            else:
                # Insert new movie
                cursor.execute("""
                    INSERT INTO movies (
                        title_en, director, year, cinema, 
                        detail_url, image_url, overview_en, trailer_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    title_en,
                    movie_data.get('director', ''),
                    movie_data.get('year'),
                    'Metrograph',
                    movie_data.get('detail_url', ''),
                    movie_data.get('image_url', ''),
                    movie_data.get('overview_en', ''),
                    movie_data.get('trailer_url', '')
                ))
                
                movie_id = cursor.lastrowid
                conn.commit()
                imported_movies += 1
                print(f"  - Imported new movie: {title_en} (ID: {movie_id})")
            
            # Process screenings
            if movie_data.get('show_dates'):
                # First, remove old screenings for this movie at Metrograph
                cursor.execute("""
                    DELETE FROM screenings 
                    WHERE movie_id = ? AND cinema = 'Metrograph'
                """, (movie_id,))
                
                # Add new screenings
                for date_info in movie_data.get('show_dates', []):
                    for time_info in date_info.get('times', []):
                        screening_date = date_info.get('date')
                        screening_time = time_info.get('time')
                        sold_out = time_info.get('sold_out', False)
                        ticket_url = time_info.get('ticket_url', '')
                        
                        if screening_date and screening_time:
                            # Check if the date is already in YYYY-MM-DD format
                            if not screening_date.startswith('20'):
                                # Try to parse the date
                                try:
                                    parsed_date = datetime.strptime(screening_date, "%A %B %d, %Y")
                                    screening_date = parsed_date.strftime("%Y-%m-%d")
                                except ValueError:
                                    print(f"  - Warning: Could not parse date: {screening_date}")
                            
                            # Add screening
                            cursor.execute("""
                                INSERT INTO screenings (
                                    movie_id, cinema, date, time, sold_out, ticket_url, title_en
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                movie_id,
                                'Metrograph',
                                screening_date,
                                screening_time,
                                1 if sold_out else 0,  # SQLite uses 1/0 for boolean
                                ticket_url,
                                title_en
                            ))
                            imported_screenings += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Import completed successfully!")
        print(f"  - Movies imported/updated: {imported_movies}")
        print(f"  - Screenings imported: {imported_screenings}")
        
    except Exception as e:
        print(f"❌ Error importing data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    import_metrograph_data() 