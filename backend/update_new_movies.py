import os
import sqlite3
import requests
import time
import json
import re
import argparse
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Get API keys
TMDB_API_KEY = os.getenv('TMDB_API_KEY')
if not TMDB_API_KEY:
    raise ValueError("Please set TMDB_API_KEY in your .env file")

# OMDb API key
OMDB_API_KEY = "85a51227"

# Database path
script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "movies.db")

# API base URLs
TMDB_BASE_URL = "https://api.themoviedb.org/3"
OMDB_BASE_URL = "http://www.omdbapi.com/"

def clean_title(title):
    """
    Clean movie title, remove format markers, prefixes, etc.
    """
    if not title:
        return ""
        
    # Remove format markers, like [DCP], [35mm], etc.
    title = re.sub(r'\s*\[[^\]]+\]', '', title)
    
    # Remove quotes
    title = re.sub(r'[\'"]([^\'"]+)[\'"]', r'\1', title)
    
    # Remove prefixes, like "ACE Presents: ", "Jean-Luc Godard's ", etc.
    title = re.sub(r'^[^:]+:\s*', '', title)
    title = re.sub(r'^.*\'s\s+', '', title)
    title = re.sub(r'^The Making of\s+', '', title)
    
    # Remove director names before title
    if 'Godard' in title:
        title = re.sub(r'^.*Godard\'?s?\s+', '', title)
    
    # Special handling for common filmmaker names
    common_directors = ['Fellini', 'Hitchcock', 'Kurosawa', 'Tarkovsky', 'Kubrick', 'Spielberg', 
                       'Coppola', 'Scorsese', 'Tarantino', 'Nolan', 'Bergman', 'Anderson', 'Malick']
    for director in common_directors:
        title = re.sub(fr'^{director}\'s?\s+', '', title)
    
    # Remove suffixes like "and La Tour"
    title = re.sub(r'\s+and\s+.*$', '', title)
    
    # Handle movie series, like "Blade Runner: The Final Cut" -> "Blade Runner"
    if ":" in title:
        parts = title.split(":")
        if len(parts[0].split()) > 1:  # Avoid processing short titles
            title = parts[0].strip()
    
    # Handle case for "A WOMAN IS A WOMAN" and similar titles
    title = title.title()  # Convert "A WOMAN IS A WOMAN" to "A Woman Is A Woman"
    
    # Replace "The Beekeper" with "The Beekeeper" (common typo)
    if title.lower() == "the beekeper":
        title = "The Beekeeper"
    
    return title.strip()

def search_movie(title, year=None):
    """
    Search for a movie in TMDB
    """
    original_title = title
    
    # Try cleaning the title
    clean_title_text = clean_title(title)
    print(f"Searching movie: original title '{original_title}' -> cleaned '{clean_title_text}'")
    
    # Handle special cases (movies that need exact title search)
    special_cases = {
        "A Woman Is A Woman": "Une femme est une femme",
        "One Plus One": "Sympathy for the Devil",
        "The Beekeeper": "The Beekeeper"
    }
    
    if clean_title_text in special_cases:
        alternate_title = special_cases[clean_title_text]
        print(f"Using alternate title for search: '{alternate_title}'")
        clean_title_text = alternate_title
    
    # First attempt: use cleaned title and year
    url = f"{TMDB_BASE_URL}/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": clean_title_text,
        "language": "zh-CN",
        "include_adult": "true"
    }
    
    if year:
        params["year"] = year
    
    # Try multiple languages for better results
    for lang in ["zh-CN", "en-US"]:
        params["language"] = lang
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]
    
    # Second attempt: only use cleaned title, without year limitation
    if year:
        params.pop("year")
        params["language"] = "en-US"
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]
    
    # Third attempt: further simplify title (take first two words)
    simple_title = " ".join(clean_title_text.split()[:2])
    if simple_title != clean_title_text and len(simple_title.split()) > 1:
        params["query"] = simple_title
        print(f"Trying simplified title: '{simple_title}'")
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]
    
    # Fourth attempt: try using the original title without cleaning
    params["query"] = original_title
    print(f"Trying original title: '{original_title}'")
    response = requests.get(url, params=params)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            return results[0]
    
    return None

def get_movie_details(tmdb_id):
    """
    Get detailed movie information, including IMDB ID, director info, etc.
    """
    # Get Chinese details
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "append_to_response": "external_ids,videos,credits",
        "language": "zh-CN"  # Get Chinese details
    }
    
    response = requests.get(url, params=params)
    zh_details = None
    if response.status_code == 200:
        zh_details = response.json()
    
    # Then get English details
    params["language"] = "en-US"
    response = requests.get(url, params=params)
    en_details = None
    if response.status_code == 200:
        en_details = response.json()
    
    if zh_details:
        # Add English overview to Chinese details
        if en_details and "overview" in en_details:
            zh_details["overview_en"] = en_details["overview"]
        
        # Get director information
        if "credits" in zh_details and "crew" in zh_details["credits"]:
            directors = [person for person in zh_details["credits"]["crew"] if person["job"] == "Director"]
            if directors:
                zh_details["zh_directors"] = directors
                
                # Get English names of directors
                if en_details and "credits" in en_details and "crew" in en_details["credits"]:
                    en_directors = [person for person in en_details["credits"]["crew"] if person["job"] == "Director"]
                    zh_details["en_directors"] = en_directors
                
        return zh_details
    return None

def get_omdb_info(title, year=None, imdb_id=None):
    """
    Get movie information from OMDb API
    """
    params = {
        "apikey": OMDB_API_KEY,
        "r": "json",
        "plot": "full"
    }
    
    # Prioritize IMDb ID
    if imdb_id:
        params["i"] = imdb_id
    else:
        params["t"] = clean_title(title)
        if year:
            params["y"] = year
    
    response = requests.get(OMDB_BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get("Response") == "True":
            print(f"OMDb found movie: {data.get('Title')} ({data.get('Year')})")
            return data
    
    # If not found, try using only the first two words of the title
    if not imdb_id and title and ' ' in title:
        simplified_title = ' '.join(clean_title(title).split()[:2])
        if simplified_title != title:
            params["t"] = simplified_title
            if year:
                params["y"] = year
            
            print(f"Trying simplified title search in OMDb: '{simplified_title}'")
            response = requests.get(OMDB_BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get("Response") == "True":
                    print(f"OMDb found movie: {data.get('Title')} ({data.get('Year')})")
                    return data
    
    # Handle special cases
    special_cases = {
        "The Beekeeper": "The Beekeeper",
        "A Woman Is A Woman": "Une femme est une femme",
        "Weird Medicine": "Weird Medicine"
    }
    
    clean_title_text = clean_title(title) if title else ""
    if clean_title_text in special_cases:
        alt_title = special_cases[clean_title_text]
        if alt_title != clean_title_text:
            print(f"Trying special case title in OMDb: '{alt_title}'")
            params["t"] = alt_title
            response = requests.get(OMDB_BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get("Response") == "True":
                    print(f"OMDb found movie with special case: {data.get('Title')} ({data.get('Year')})")
                    return data
    
    return None

def get_recently_added_movies(days=7):
    """
    Get movies added within the specified number of days
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Calculate the date threshold
    threshold_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    cursor.execute("""
        SELECT id, title_en, year, director, title_cn, imdb_id
        FROM movies 
        WHERE created_at >= ?
        AND (
            tmdb_id IS NULL OR 
            imdb_id IS NULL OR 
            director IS NULL OR 
            director = '' OR
            title_cn IS NULL OR
            overview_en IS NULL OR
            overview_en = ''
        )
        ORDER BY id
    """, (threshold_date,))
    
    movies = cursor.fetchall()
    conn.close()
    
    return movies

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

def create_douban_search_url(imdb_id, title=None):
    """
    Create Douban search link
    
    Parameters:
        imdb_id: IMDb ID
        title: Movie title (optional)
    """
    from urllib.parse import quote
    
    # Create search URL
    search_urls = []
    
    # 1. Search by IMDb ID
    if imdb_id:
        search_urls.append(f"https://www.douban.com/search?cat=1002&q=imdb:{imdb_id}")
    
    # 2. Search by title
    if title and title.strip():
        search_urls.append(f"https://www.douban.com/search?cat=1002&q={quote(title.strip())}")
    
    # Join with pipe separator
    return "|".join(search_urls)

def update_movie_info(movie_id, tmdb_data, movie_details, original_director):
    """
    Update movie information in the database with TMDB data
    """
    # Extract data
    title_cn = tmdb_data.get("title", "")
    overview_cn = tmdb_data.get("overview", "")
    overview_en = tmdb_data.get("overview_en", "")
    vote_average = tmdb_data.get("vote_average")
    poster_path = tmdb_data.get("poster_path")
    tmdb_id = tmdb_data.get("id")
    imdb_id = None
    
    # Get IMDb ID from external IDs
    if "external_ids" in movie_details and movie_details["external_ids"].get("imdb_id"):
        imdb_id = movie_details["external_ids"].get("imdb_id")
    
    # Get director information
    director_en = original_director  # Keep original director if no new data
    director_cn = None
    
    if "zh_directors" in movie_details and movie_details["zh_directors"]:
        director_cn = ", ".join([d.get("name", "") for d in movie_details["zh_directors"]])
    
    if "en_directors" in movie_details and movie_details["en_directors"]:
        director_en = ", ".join([d.get("name", "") for d in movie_details["en_directors"]])
    
    # Get trailer
    trailer_url = None
    if "videos" in movie_details and "results" in movie_details["videos"]:
        videos = movie_details["videos"]["results"]
        trailers = [v for v in videos if v.get("type") == "Trailer" and v.get("site") == "YouTube"]
        if trailers:
            trailer_url = f"https://www.youtube.com/watch?v={trailers[0].get('key')}"
    
    # Create Letterboxd URL if IMDb ID is available
    letterboxd_url = create_letterboxd_url(imdb_id) if imdb_id else None
    
    # Create Douban search URL if IMDb ID or title is available
    douban_url = create_douban_search_url(imdb_id, title_cn or movie_details.get("title")) if imdb_id or title_cn else None
    
    # Update database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    update_fields = []
    update_values = []
    
    # Only update fields with non-None values
    if title_cn:
        update_fields.append("title_cn = ?")
        update_values.append(title_cn)
        
    if overview_cn:
        update_fields.append("overview_cn = ?")
        update_values.append(overview_cn)
        
    if overview_en:
        update_fields.append("overview_en = ?")
        update_values.append(overview_en)
        
    if vote_average:
        update_fields.append("rating = ?")
        update_values.append(vote_average)
        
    if poster_path:
        update_fields.append("image_url = ?")
        update_values.append(f"https://image.tmdb.org/t/p/w500{poster_path}")
        
    if tmdb_id:
        update_fields.append("tmdb_id = ?")
        update_values.append(tmdb_id)
        
    if imdb_id:
        update_fields.append("imdb_id = ?")
        update_values.append(imdb_id)
        
    if director_en:
        update_fields.append("director = ?")
        update_values.append(director_en)
        
    if director_cn:
        update_fields.append("director_cn = ?")
        update_values.append(director_cn)
        
    if trailer_url:
        update_fields.append("trailer_url = ?")
        update_values.append(trailer_url)
        
    if letterboxd_url:
        update_fields.append("letterboxd_url = ?")
        update_values.append(letterboxd_url)
        
    if douban_url:
        update_fields.append("douban_url = ?")
        update_values.append(douban_url)
    
    if update_fields:
        query = f"UPDATE movies SET {', '.join(update_fields)} WHERE id = ?"
        update_values.append(movie_id)
        
        cursor.execute(query, update_values)
        conn.commit()
        
        print(f"Updated movie ID {movie_id} with TMDB data")
        conn.close()
        return True
    
    conn.close()
    return False

def update_with_omdb(movie_id, omdb_data, existing_info):
    """
    Update movie information in the database with OMDb data
    """
    # Extract data from existing info
    _, title_en, year, director, title_cn, imdb_id = existing_info
    
    # Extract data from OMDb
    title_from_omdb = omdb_data.get("Title")
    year_from_omdb = omdb_data.get("Year")
    imdb_id_from_omdb = omdb_data.get("imdbID")
    director_from_omdb = omdb_data.get("Director")
    plot = omdb_data.get("Plot")
    imdb_rating = omdb_data.get("imdbRating")
    
    # Use existing data if OMDb data is not available
    if not director or director.strip() == "N/A":
        director = director_from_omdb if director_from_omdb and director_from_omdb != "N/A" else director
    
    if not imdb_id:
        imdb_id = imdb_id_from_omdb
    
    # Create Letterboxd URL if IMDb ID is available
    letterboxd_url = create_letterboxd_url(imdb_id) if imdb_id else None
    
    # Create Douban search URL if IMDb ID or title is available
    douban_url = create_douban_search_url(imdb_id, title_cn or title_en) if imdb_id or title_cn or title_en else None
    
    # Update database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    update_fields = []
    update_values = []
    
    # Only update fields with non-None values
    if imdb_id:
        update_fields.append("imdb_id = ?")
        update_values.append(imdb_id)
        
    if director and director != "N/A":
        update_fields.append("director = ?")
        update_values.append(director)
        
    if plot and plot != "N/A":
        update_fields.append("overview_en = ?")
        update_values.append(plot)
        
    if imdb_rating and imdb_rating != "N/A":
        update_fields.append("rating = ?")
        update_values.append(float(imdb_rating))
        
    if letterboxd_url:
        update_fields.append("letterboxd_url = ?")
        update_values.append(letterboxd_url)
        
    if douban_url:
        update_fields.append("douban_url = ?")
        update_values.append(douban_url)
    
    if update_fields:
        query = f"UPDATE movies SET {', '.join(update_fields)} WHERE id = ?"
        update_values.append(movie_id)
        
        cursor.execute(query, update_values)
        conn.commit()
        
        print(f"Updated movie ID {movie_id} with OMDb data")
        conn.close()
        return True
    
    conn.close()
    return False

def update_recent_movies(days=7):
    """
    Update information for recently added movies
    """
    # Get recently added movies
    movies = get_recently_added_movies(days)
    print(f"Found {len(movies)} recently added movies needing updates")
    
    if not movies:
        print("No recent movies need updating.")
        return
    
    # Update with TMDB and OMDb
    updated_tmdb_count = 0
    updated_omdb_count = 0
    
    for movie in movies:
        movie_id, title_en, year, director, title_cn, imdb_id = movie
        
        print(f"\nProcessing: {title_en or title_cn} (ID: {movie_id})")
        
        # Special case for "The Beekeper"
        if title_en and "Beekeper" in title_en:
            title_en = title_en.replace("Beekeper", "Beekeeper")
            print(f"Corrected title to: {title_en}")
        
        # Special case for "A WOMAN IS A WOMAN"
        if title_en and "A WOMAN IS A WOMAN" in title_en.upper():
            clean_title_text = "Une femme est une femme"
            print(f"Using alternative title: {clean_title_text}")
            
            # Try direct search for this movie
            omdb_data = get_omdb_info("Une femme est une femme", 1961)
            if omdb_data:
                if update_with_omdb(movie_id, omdb_data, movie):
                    updated_omdb_count += 1
                    time.sleep(0.5)
                continue
        
        # Try to find the movie in TMDB
        tmdb_movie = None
        if title_cn:
            tmdb_movie = search_movie(title_cn, year)
            
        if not tmdb_movie and title_en:
            tmdb_movie = search_movie(title_en, year)
        
        if tmdb_movie:
            print(f"Found in TMDB: {tmdb_movie.get('title')} (ID: {tmdb_movie.get('id')})")
            movie_details = get_movie_details(tmdb_movie.get('id'))
            if movie_details:
                if update_movie_info(movie_id, tmdb_movie, movie_details, director):
                    updated_tmdb_count += 1
                    time.sleep(0.5)  # Avoid API rate limits
        
        # Try OMDb if TMDB failed or missed some info
        if not tmdb_movie or not imdb_id or not director:
            omdb_data = None
            if imdb_id:
                omdb_data = get_omdb_info(None, None, imdb_id)
            
            if not omdb_data and title_en:
                omdb_data = get_omdb_info(title_en, year)
                
            if not omdb_data and title_cn:
                omdb_data = get_omdb_info(title_cn, year)
            
            # Additional attempt with manual search for specific titles
            if not omdb_data and title_en and "Beekeeper" in title_en:
                omdb_data = get_omdb_info("The Beekeeper", 2024)
            
            if omdb_data:
                if update_with_omdb(movie_id, omdb_data, movie):
                    updated_omdb_count += 1
                    time.sleep(0.5)  # Avoid API rate limits
    
    print(f"\nUpdate summary:")
    print(f"- TMDB updates: {updated_tmdb_count} of {len(movies)}")
    print(f"- OMDb updates: {updated_omdb_count} of {len(movies)}")
    print(f"- Total movies updated: {updated_tmdb_count + updated_omdb_count} of {len(movies)}")

def main():
    parser = argparse.ArgumentParser(description="Update information for recently added movies")
    parser.add_argument('--days', type=int, default=7, 
                        help='Number of days to look back for recently added movies (default: 7)')
    args = parser.parse_args()
    
    update_recent_movies(args.days)

if __name__ == "__main__":
    main() 