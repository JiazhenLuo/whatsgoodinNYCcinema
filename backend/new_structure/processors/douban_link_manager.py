import os
import sqlite3
import time
import re
import argparse
import requests
from urllib.parse import quote

# Database path
script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "movies.db")

def get_all_movies(with_douban=False, only_without_douban=False, only_without_letterboxd=False):
    """
    Get all movies with IMDb ID
    
    Parameters:
        with_douban: Whether to include movies with Douban links
        only_without_douban: Whether to only get movies without Douban links
        only_without_letterboxd: Whether to only get movies without Letterboxd links
    """
    max_retries = 5
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=20)
            cursor = conn.cursor()
            
            query = """
                SELECT id, title_en, title_cn, imdb_id, year, director, douban_url, letterboxd_url 
                FROM movies 
                WHERE imdb_id IS NOT NULL
            """
            
            if only_without_douban:
                query += " AND (douban_url IS NULL OR douban_url = '')"
            elif not with_douban:
                query += " AND (douban_url IS NULL OR douban_url = '')"
                
            if only_without_letterboxd:
                query += " AND (letterboxd_url IS NULL OR letterboxd_url = '')"
                
            query += " ORDER BY id"
            
            cursor.execute(query)
            movies = cursor.fetchall()
            conn.close()
            
            return movies
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database is locked, waiting {retry_delay} seconds before retrying...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise
    
    return []

def add_required_columns():
    """
    Ensure required columns exist (Douban and Letterboxd links)
    """
    max_retries = 5
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=20)
            cursor = conn.cursor()
            
            # Check if the required columns already exist in the movies table
            cursor.execute("PRAGMA table_info(movies)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Add Douban link column
            if "douban_url" not in columns:
                print("Adding douban_url column to movies table...")
                cursor.execute("ALTER TABLE movies ADD COLUMN douban_url TEXT")
                conn.commit()
            
            # Add Letterboxd link column
            if "letterboxd_url" not in columns:
                print("Adding letterboxd_url column to movies table...")
                cursor.execute("ALTER TABLE movies ADD COLUMN letterboxd_url TEXT")
                conn.commit()
                
            conn.close()
            return
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database is locked, waiting {retry_delay} seconds before retrying...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise

def add_douban_column():
    """
    Ensure Douban link field exists (for backward compatibility)
    """
    add_required_columns()

def create_letterboxd_url(imdb_id):
    """
    Create Letterboxd link based on IMDb ID
    
    Letterboxd uses IMDb ID as movie identifier
    Format: https://letterboxd.com/imdb/{imdb_id}/
    """
    if not imdb_id:
        return None
    
    # Ensure IMDb ID has 'tt' prefix
    if not imdb_id.startswith('tt'):
        imdb_id = f"tt{imdb_id}"
    
    # Build Letterboxd link
    letterboxd_url = f"https://letterboxd.com/imdb/{imdb_id}/"
    
    return letterboxd_url

def update_letterboxd_url(movie_id, letterboxd_url):
    """
    Update movie's Letterboxd link
    
    Parameters:
        movie_id: Movie ID
        letterboxd_url: Letterboxd link
    """
    max_retries = 5
    retry_delay = 1
    
    if not letterboxd_url:
        return False
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=20)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE movies 
                SET letterboxd_url = ? 
                WHERE id = ?
            """, (letterboxd_url, movie_id))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Successfully updated Letterboxd link for movie ID {movie_id}")
            return True
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database is locked, waiting {retry_delay} seconds before retrying...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise
    
    return False

def create_simple_search_url(imdb_id, title=None):
    """
    Create basic Douban search links (IMDb ID and title only)
    
    Parameters:
        imdb_id: IMDb ID
        title: Movie title (optional)
    """
    # Prefer search by IMDb ID
    douban_url = f"https://www.douban.com/search?cat=1002&q=imdb:{imdb_id}"
    
    # For Chinese titles, also provide direct search link as backup
    title_search_url = None
    if title and title.strip():
        title_search_url = f"https://www.douban.com/search?cat=1002&q={quote(title.strip())}"
    
    return douban_url, title_search_url

def create_smart_search_url(movie_data, auto_click=False):
    """
    Create smart Douban search link combining multiple information
    
    Parameters:
        movie_data: Format (id, title_en, title_cn, imdb_id, year, director, douban_url, letterboxd_url)
        auto_click: Whether to add auto-click marker (for frontend auto-clicking first search result)
    """
    _, title_en, title_cn, imdb_id, year, director, _, _ = movie_data
    search_urls = []
    
    # Build various search URLs
    # 1. IMDb ID search (highest priority)
    if imdb_id:
        imdb_search = f"https://www.douban.com/search?cat=1002&q=imdb:{imdb_id}"
        if auto_click:
            imdb_search = f"{imdb_search}#auto_click"
        search_urls.append(imdb_search)
    
    # 2. Chinese title + year search
    if title_cn and year:
        cn_year_search = f"https://www.douban.com/search?cat=1002&q={quote(title_cn.strip())}+{year}"
        if auto_click:
            cn_year_search = f"{cn_year_search}#auto_click"
        search_urls.append(cn_year_search)
    
    # 3. Chinese title + director search
    if title_cn and director and director.strip():
        cn_dir_search = f"https://www.douban.com/search?cat=1002&q={quote(title_cn.strip())}+{quote(director.strip())}"
        if auto_click:
            cn_dir_search = f"{cn_dir_search}#auto_click"
        search_urls.append(cn_dir_search)
    
    # 4. English title + year search
    if title_en and year:
        en_year_search = f"https://www.douban.com/search?cat=1002&q={quote(title_en.strip())}+{year}"
        if auto_click:
            en_year_search = f"{en_year_search}#auto_click"
        search_urls.append(en_year_search)
    
    # 5. Chinese title only search
    if title_cn:
        cn_search = f"https://www.douban.com/search?cat=1002&q={quote(title_cn.strip())}"
        if auto_click:
            cn_search = f"{cn_search}#auto_click"
        search_urls.append(cn_search)
    
    # 6. English title only search
    if title_en:
        en_search = f"https://www.douban.com/search?cat=1002&q={quote(title_en.strip())}"
        if auto_click:
            en_search = f"{en_search}#auto_click"
        search_urls.append(en_search)
    
    # Use pipe "|" to separate different search URLs, frontend can try in order
    return "|".join(search_urls)

def update_douban_url(movie_id, douban_url, title_search_url=None):
    """
    Update movie's Douban link
    
    Parameters:
        movie_id: Movie ID
        douban_url: Douban link
        title_search_url: Title search link (optional)
    """
    max_retries = 5
    retry_delay = 1
    
    # Handle input types
    if isinstance(douban_url, list):
        # If it's a list of URLs, join with pipe
        final_url = "|".join(douban_url)
    elif title_search_url:
        # If title search URL is provided, join with pipe
        final_url = f"{douban_url}|{title_search_url}"
    else:
        # Otherwise use the provided URL directly
        final_url = douban_url
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=20)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE movies 
                SET douban_url = ? 
                WHERE id = ?
            """, (final_url, movie_id))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Successfully updated Douban link for movie ID {movie_id}")
            return True
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database is locked, waiting {retry_delay} seconds before retrying...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise
    
    return False

def add_letterboxd_links():
    """
    Add Letterboxd links for all movies
    """
    try:
        # Ensure required columns exist
        add_required_columns()
        
        # Get all movies with IMDb ID but without Letterboxd links
        movies = get_all_movies(with_douban=True, only_without_letterboxd=True)
        print(f"Found {len(movies)} movies needing Letterboxd links")
        
        updated_count = 0
        for movie in movies:
            movie_id = movie[0]
            title_cn = movie[2] if movie[2] else movie[1]  # Prefer Chinese title
            imdb_id = movie[3]
            
            print(f"Processing: {title_cn} (IMDB: {imdb_id})")
            
            # Create Letterboxd link
            letterboxd_url = create_letterboxd_url(imdb_id)
            
            # Update database
            if letterboxd_url and update_letterboxd_url(movie_id, letterboxd_url):
                updated_count += 1
            
            # Brief delay to avoid database burden
            time.sleep(0.1)
        
        print(f"🎬 Done! Added Letterboxd links for {updated_count} movies")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def show_link_stats():
    """
    Show statistics for Douban and Letterboxd links
    """
    max_retries = 5
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=20)
            cursor = conn.cursor()
            
            # Total movie count
            cursor.execute("SELECT COUNT(*) FROM movies")
            total_movies = cursor.fetchone()[0]
            
            # Movies with IMDb ID
            cursor.execute("SELECT COUNT(*) FROM movies WHERE imdb_id IS NOT NULL")
            with_imdb = cursor.fetchone()[0]
            
            # Movies with Douban links
            cursor.execute("SELECT COUNT(*) FROM movies WHERE douban_url IS NOT NULL AND douban_url != ''")
            with_douban = cursor.fetchone()[0]
            
            # Movies with search links
            cursor.execute("SELECT COUNT(*) FROM movies WHERE douban_url LIKE '%search%'")
            with_search = cursor.fetchone()[0]
            
            # Movies with direct links (without 'search')
            cursor.execute("SELECT COUNT(*) FROM movies WHERE douban_url IS NOT NULL AND douban_url != '' AND douban_url NOT LIKE '%search%'")
            with_direct = cursor.fetchone()[0]
            
            # Movies with multiple search links
            cursor.execute("SELECT COUNT(*) FROM movies WHERE douban_url LIKE '%|%'")
            with_multiple = cursor.fetchone()[0]
            
            # Movies with Letterboxd links
            cursor.execute("SELECT COUNT(*) FROM movies WHERE letterboxd_url IS NOT NULL AND letterboxd_url != ''")
            with_letterboxd = cursor.fetchone()[0]
            
            # Show 5 sample movies with Douban links
            cursor.execute("""
                SELECT id, title_cn, title_en, imdb_id, douban_url, letterboxd_url
                FROM movies 
                WHERE (douban_url IS NOT NULL AND douban_url != '') OR (letterboxd_url IS NOT NULL AND letterboxd_url != '')
                LIMIT 5
            """)
            
            sample_movies = cursor.fetchall()
            
            # Now we can close the connection
            conn.close()
            
            print("\n====== Movie Link Statistics ======")
            print(f"Total movies: {total_movies}")
            print(f"Movies with IMDb ID: {with_imdb} ({with_imdb/total_movies*100:.1f}%)")
            print(f"Movies with Douban links: {with_douban} ({with_douban/total_movies*100:.1f}%)")
            print(f"Movies with search links: {with_search} ({with_search/with_douban*100:.1f}% if any)")
            print(f"Movies with direct links: {with_direct} ({with_direct/with_douban*100:.1f}% if any)")
            print(f"Movies with multiple search links: {with_multiple} ({with_multiple/with_douban*100:.1f}% if any)")
            print(f"Movies with Letterboxd links: {with_letterboxd} ({with_letterboxd/total_movies*100:.1f}%)")
            
            print("\n----- Movie Samples -----")
            
            for movie in sample_movies:
                movie_id, title_cn, title_en, imdb_id, douban_url, letterboxd_url = movie
                title = title_cn if title_cn else title_en
                print(f"ID: {movie_id}, Title: {title}, IMDb: {imdb_id}")
                
                if douban_url:
                    # If there are multiple links, display them separately
                    if "|" in douban_url:
                        links = douban_url.split("|")
                        for i, link in enumerate(links):
                            print(f"  Douban Link {i+1}: {link}")
                    else:
                        print(f"  Douban Link: {douban_url}")
                
                if letterboxd_url:
                    print(f"  Letterboxd Link: {letterboxd_url}")
            
            print("==========================\n")
            
            return True
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database is locked, waiting {retry_delay} seconds before retrying...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise
    
    return False

def add_simple_links():
    """
    Add simple search links for movies without Douban links
    """
    try:
        # Ensure required columns exist
        add_required_columns()
        
        # Get movies needing Douban links
        movies = get_all_movies(only_without_douban=True)
        print(f"Found {len(movies)} movies needing Douban links")
        
        updated_count = 0
        for movie in movies:
            movie_id = movie[0]
            title_en = movie[1]
            title_cn = movie[2]
            imdb_id = movie[3]
            
            # Choose best title for display (prefer Chinese title)
            search_title = title_cn if title_cn else title_en
            print(f"Processing: {search_title} (IMDB: {imdb_id})")
            
            # Create Douban links
            douban_url, title_search_url = create_simple_search_url(imdb_id, search_title)
            
            # Update database
            if update_douban_url(movie_id, douban_url, title_search_url):
                updated_count += 1
            
            # Brief delay to avoid database burden
            time.sleep(0.1)
        
        print(f"🎬 Done! Added simple Douban search links for {updated_count} movies")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def add_smart_links(auto_click=False):
    """
    Add smart search links for all movies
    
    Parameters:
        auto_click: Whether to add auto-click marker (for frontend auto-clicking first search result)
    """
    try:
        # Ensure required columns exist
        add_required_columns()
        
        # Get all movies with IMDb ID
        movies = get_all_movies(with_douban=True)
        print(f"Found {len(movies)} movies to update Douban links")
        
        updated_count = 0
        for movie in movies:
            movie_id = movie[0]
            title_cn = movie[2] if movie[2] else movie[1]  # Prefer Chinese title
            
            print(f"Processing: {title_cn} (ID: {movie_id})")
            
            # Create smart search link, optionally with auto-click marker
            smart_url = create_smart_search_url(movie, auto_click)
            
            # Update database
            if update_douban_url(movie_id, smart_url):
                updated_count += 1
            
            # Brief delay to avoid database burden
            time.sleep(0.1)
        
        link_type = "with auto-click markers " if auto_click else ""
        print(f"🎬 Done! Added {link_type}smart Douban search links for {updated_count} movies")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def add_auto_click_flag():
    """
    Add auto-click markers to existing Douban links
    """
    try:
        # Get all movies with IMDb ID
        movies = get_all_movies(with_douban=True)
        print(f"Found {len(movies)} movies to update Douban links")
        
        updated_count = 0
        for movie in movies:
            movie_id = movie[0]
            title_cn = movie[2] if movie[2] else movie[1]  # Prefer Chinese title
            douban_url = movie[6]
            
            if not douban_url:
                continue
                
            print(f"Processing: {title_cn} (ID: {movie_id})")
            
            # Add auto-click marker to existing links
            urls = douban_url.split('|')
            new_urls = []
            
            for url in urls:
                if '#auto_click' not in url:
                    url = f"{url}#auto_click"
                new_urls.append(url)
            
            # Update database
            if update_douban_url(movie_id, '|'.join(new_urls)):
                updated_count += 1
            
            # Brief delay to avoid database burden
            time.sleep(0.1)
        
        print(f"🎬 Done! Added auto-click markers to Douban links for {updated_count} movies")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def update_all_letterboxd_links():
    """
    Update all existing Letterboxd links to the correct format
    """
    try:
        # Ensure required columns exist
        add_required_columns()
        
        # Get all movies with IMDb ID
        movies = get_all_movies(with_douban=True)
        print(f"Found {len(movies)} movies with IMDb IDs")
        
        updated_count = 0
        for movie in movies:
            movie_id = movie[0]
            title_cn = movie[2] if movie[2] else movie[1]  # Prefer Chinese title
            imdb_id = movie[3]
            
            print(f"Processing: {title_cn} (IMDB: {imdb_id})")
            
            # Create Letterboxd link
            letterboxd_url = create_letterboxd_url(imdb_id)
            
            # Update database
            if letterboxd_url and update_letterboxd_url(movie_id, letterboxd_url):
                updated_count += 1
            
            # Brief delay to avoid database burden
            time.sleep(0.1)
        
        print(f"🎬 Done! Updated Letterboxd links for {updated_count} movies")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def print_help():
    """
    Print help information
    """
    print("\n====== Movie Link Management Tool ======")
    print("Usage: python douban_link_manager.py [command] [options]")
    print("\nAvailable commands:")
    print("  stats           - Show link statistics")
    print("  smartlinks      - Add smart search links for all movies (includes multiple search methods)")
    print("  simplelinks     - Add simple search links for movies without Douban links (IMDb and title only)")
    print("  autoflag        - Add auto-click markers to existing Douban links")
    print("  letterboxd      - Add Letterboxd links for all movies")
    print("  updateletter    - Update all existing Letterboxd links to the correct format")
    print("  help            - Show help information")
    print("\nOptions:")
    print("  --auto-click    - Add auto-click markers (use with smartlinks command)")
    print("\nExamples:")
    print("  python douban_link_manager.py stats")
    print("  python douban_link_manager.py smartlinks --auto-click")
    print("  python douban_link_manager.py simplelinks")
    print("  python douban_link_manager.py autoflag")
    print("  python douban_link_manager.py letterboxd")
    print("  python douban_link_manager.py updateletter")
    print("==========================\n")

def main():
    parser = argparse.ArgumentParser(description="Movie Link Management Tool", add_help=False)
    parser.add_argument('command', nargs='?', default='help', 
                        help='Command to execute: stats, smartlinks, simplelinks, autoflag, letterboxd, updateletter, help')
    parser.add_argument('--auto-click', action='store_true',
                        help='Add auto-click markers (for smartlinks command)')
    
    args = parser.parse_args()
    
    if args.command == 'stats':
        show_link_stats()
    elif args.command == 'smartlinks':
        add_smart_links(args.auto_click)
    elif args.command == 'simplelinks':
        add_simple_links()
    elif args.command == 'autoflag':
        add_auto_click_flag()
    elif args.command == 'letterboxd':
        add_letterboxd_links()
    elif args.command == 'updateletter':
        update_all_letterboxd_links()
    elif args.command == 'help':
        print_help()
    else:
        print(f"Unknown command: {args.command}")
        print_help()

if __name__ == "__main__":
    main() 