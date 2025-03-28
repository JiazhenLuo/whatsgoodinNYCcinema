"""
Screening model and database operations.
"""
from .database import get_db_connection

class Screening:
    """
    Screening model and related operations.
    """
    
    @staticmethod
    def get_all_screenings(page=1, limit=20):
        """
        Get all screenings from the database with pagination.
        
        Args:
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of screenings for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        
        cursor.execute("""
            SELECT s.*, m.title_en, m.title_cn, m.image_url, m.director, m.year
            FROM screenings s
            JOIN movies m ON s.movie_id = m.id
            ORDER BY s.date DESC, s.time
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        screenings = cursor.fetchall()
        conn.close()
        
        return [dict(screening) for screening in screenings]
    
    @staticmethod
    def count_all_screenings():
        """
        Count the total number of screenings in the database.
        
        Returns:
            Total number of screenings
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM screenings")
        count = cursor.fetchone()[0]
        
        conn.close()
        
        return count
        
    @staticmethod
    def get_screening_by_id(screening_id):
        """
        Get a screening by its ID.
        
        Args:
            screening_id: ID of the screening to retrieve
            
        Returns:
            Screening data as dictionary or None if not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT s.*, m.title_en, m.title_cn, m.image_url, m.director, m.year
            FROM screenings s
            JOIN movies m ON s.movie_id = m.id
            WHERE s.id = ?
        """, (screening_id,))
        
        screening = cursor.fetchone()
        conn.close()
        
        return dict(screening) if screening else None
    
    @staticmethod
    def get_screenings_by_movie_id(movie_id):
        """
        Get all screenings for a specific movie.
        
        Args:
            movie_id: ID of the movie
            
        Returns:
            List of screenings for the movie
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT s.*, m.title_en, m.title_cn, m.image_url, m.director, m.year
            FROM screenings s
            JOIN movies m ON s.movie_id = m.id
            WHERE movie_id = ?
            ORDER BY date, time
        """, (movie_id,))
        
        screenings = cursor.fetchall()
        conn.close()
        
        return [dict(screening) for screening in screenings]
    
    @staticmethod
    def get_upcoming_screenings(days=7, page=1, limit=20):
        """
        Get screenings for the next specified number of days.
        
        Args:
            days: Number of days to look ahead
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of upcoming screenings for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        
        cursor.execute("""
            SELECT s.*, m.title_en, m.title_cn, m.image_url, m.director, m.year
            FROM screenings s
            JOIN movies m ON s.movie_id = m.id
            WHERE date(s.date) >= date('now')
            AND date(s.date) <= date('now', '+' || ? || ' days')
            ORDER BY s.date, s.time
            LIMIT ? OFFSET ?
        """, (days, limit, offset))
        
        screenings = cursor.fetchall()
        conn.close()
        
        return [dict(screening) for screening in screenings]
    
    @staticmethod
    def get_screenings_by_cinema(cinema, days=7, page=1, limit=20):
        """
        Get screenings for a specific cinema for the next specified number of days.
        
        Args:
            cinema: Name of the cinema
            days: Number of days to look ahead
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of screenings for the cinema for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        
        cursor.execute("""
            SELECT s.*, m.title_en, m.title_cn, m.image_url, m.director, m.year
            FROM screenings s
            JOIN movies m ON s.movie_id = m.id
            WHERE s.cinema = ?
            AND date(s.date) >= date('now')
            AND date(s.date) <= date('now', '+' || ? || ' days')
            ORDER BY s.date, s.time
            LIMIT ? OFFSET ?
        """, (cinema, days, limit, offset))
        
        screenings = cursor.fetchall()
        conn.close()
        
        return [dict(screening) for screening in screenings]
    
    @staticmethod
    def get_screenings_by_date(date, page=1, limit=20):
        """
        Get screenings for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
            page: Page number (1-indexed)
            limit: Number of items per page
            
        Returns:
            List of screenings for the date for the requested page
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        
        cursor.execute("""
            SELECT s.*, m.title_en, m.title_cn, m.image_url, m.director, m.year
            FROM screenings s
            JOIN movies m ON s.movie_id = m.id
            WHERE s.date = ?
            ORDER BY s.cinema, s.time
            LIMIT ? OFFSET ?
        """, (date, limit, offset))
        
        screenings = cursor.fetchall()
        conn.close()
        
        return [dict(screening) for screening in screenings]
    
    @staticmethod
    def add_screening(movie_id, cinema, date, time, sold_out=False, ticket_url=None, title_en=None):
        """
        Add a new screening for a movie.
        
        Args:
            movie_id: ID of the movie
            cinema: Cinema name
            date: Screening date in YYYY-MM-DD format
            time: Screening time
            sold_out: Whether the screening is sold out
            ticket_url: URL to purchase tickets
            title_en: English title of the movie (as backup)
            
        Returns:
            ID of the new screening
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get movie title if not provided
        if not title_en and movie_id:
            cursor.execute("SELECT title_en FROM movies WHERE id = ?", (movie_id,))
            result = cursor.fetchone()
            if result:
                title_en = result['title_en']
        
        cursor.execute("""
            INSERT INTO screenings (movie_id, title_en, cinema, date, time, sold_out, ticket_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (movie_id, title_en, cinema, date, time, sold_out, ticket_url))
        
        screening_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return screening_id 