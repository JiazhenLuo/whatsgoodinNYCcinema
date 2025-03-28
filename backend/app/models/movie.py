"""
Movie model and database operations.
"""
import sqlite3
from datetime import datetime, timedelta
from .database import get_db_connection
from ..config.settings import DB_PATH

class Movie:
    """
    Movie model and related operations.
    """
    
    @staticmethod
    def get_all_movies(page=1, limit=20):
        """
        Get all movies from the database with pagination.
        
        Args:
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of movies for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        
        cursor.execute("""
            SELECT * FROM movies
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        movies = cursor.fetchall()
        conn.close()
        
        return [dict(movie) for movie in movies]
    
    @staticmethod
    def count_all_movies():
        """
        Count the total number of movies in the database.
        
        Returns:
            Total number of movies
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM movies")
        count = cursor.fetchone()[0]
        
        conn.close()
        
        return count
    
    @staticmethod
    def get_movie_by_id(movie_id):
        """
        Get a movie by its ID.
        
        Args:
            movie_id: ID of the movie to retrieve
            
        Returns:
            Movie data as dictionary or None if not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM movies
            WHERE id = ?
        """, (movie_id,))
        
        movie = cursor.fetchone()
        conn.close()
        
        return dict(movie) if movie else None
    
    @staticmethod
    def get_movies_without_tmdb():
        """
        Get movies without TMDB information.
        
        Returns:
            List of movies without TMDB ID
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, title_en, year, director, title_cn
            FROM movies 
            WHERE tmdb_id IS NULL
            ORDER BY id
        """)
        
        movies = cursor.fetchall()
        conn.close()
        
        return [dict(movie) for movie in movies]
    
    @staticmethod
    def get_movies_without_director_or_imdb():
        """
        Get movies without director information or IMDB ID.
        
        Returns:
            List of movies without director or IMDb ID
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, title_en, year, director, title_cn, imdb_id
            FROM movies 
            WHERE imdb_id IS NULL OR director IS NULL OR director = ''
            ORDER BY id
        """)
        
        movies = cursor.fetchall()
        conn.close()
        
        return [dict(movie) for movie in movies]
    
    @staticmethod
    def get_movies_without_chinese_overview():
        """
        Get movies with English overview but without Chinese overview.
        
        Returns:
            List of movies with English overview but without Chinese overview
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, title_en, year, director, title_cn, imdb_id, overview_en
            FROM movies 
            WHERE (overview_cn IS NULL OR overview_cn = '') 
            AND overview_en IS NOT NULL
            AND overview_en != ''
            ORDER BY id
        """)
        
        movies = cursor.fetchall()
        conn.close()
        
        return [dict(movie) for movie in movies]
    
    @staticmethod
    def get_recent_movies(days=7, page=1, limit=20):
        """
        Get movies added in the last specified number of days.
        
        Args:
            days: Number of days to look back
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of recently added movies for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate the date from X days ago
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        offset = (page - 1) * limit
        
        cursor.execute("""
            SELECT id, title_en, year, director, title_cn, image_url, overview_en, 
                   overview_cn, rating, created_at
            FROM movies 
            WHERE DATE(created_at) >= ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, (cutoff_date, limit, offset))
        
        movies = cursor.fetchall()
        conn.close()
        
        return [dict(movie) for movie in movies]
    
    @staticmethod
    def update_movie(movie_id, data):
        """
        Update movie information in the database.
        
        Args:
            movie_id: ID of the movie to update
            data: Dictionary of fields to update
            
        Returns:
            True if the update was successful, False otherwise
        """
        if not data:
            return False
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Prepare the SET part of the SQL
        set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
        values = list(data.values()) + [movie_id]
        
        # Update the movie
        cursor.execute(f"""
            UPDATE movies
            SET {set_clause}
            WHERE id = ?
        """, values)
        
        conn.commit()
        conn.close()
        
        return cursor.rowcount > 0
    
    @staticmethod
    def search_movies(query, page=1, limit=20):
        """
        Search for movies by title (English or Chinese).
        
        Args:
            query: Search query
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of movies matching the search query for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        search_param = f"%{query}%"
        
        cursor.execute("""
            SELECT * FROM movies
            WHERE title_en LIKE ? OR title_cn LIKE ?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """, (search_param, search_param, limit, offset))
        
        movies = cursor.fetchall()
        conn.close()
        
        return [dict(movie) for movie in movies]
    
    @staticmethod
    def get_movies_without_cn_overview():
        """
        Get movies that have English overview but no Chinese overview.
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM movies WHERE overview_en IS NOT NULL AND overview_en != '' "
            "AND (overview_cn IS NULL OR overview_cn = '') "
            "ORDER BY id DESC"
        )
        
        movies = [dict(movie) for movie in cursor.fetchall()]
        
        conn.close()
        
        return movies 