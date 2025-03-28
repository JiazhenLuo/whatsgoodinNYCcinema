import os
import sqlite3
import requests
import time
import json
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get TMDB API key
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
    # Remove format markers, like [DCP], [35mm], etc.
    title = re.sub(r'\s*\[[^\]]+\]', '', title)
    
    # Remove prefixes, like "ACE Presents: ", "Jean-Luc Godard's ", etc.
    title = re.sub(r'^[^:]+:\s*', '', title)
    title = re.sub(r'^.*\'s\s+', '', title)
    
    # Remove suffixes like "and La Tour"
    title = re.sub(r'\s+and\s+.*$', '', title)
    
    # Handle movie series, like "Blade Runner: The Final Cut" -> "Blade Runner"
    if ":" in title:
        parts = title.split(":")
        if len(parts[0].split()) > 1:  # Avoid processing short titles
            title = parts[0].strip()
    
    return title.strip()

def search_movie(title, year=None):
    """
    Search for a movie in TMDB
    """
    original_title = title
    
    # Try cleaning the title
    clean_title_text = clean_title(title)
    print(f"Searching movie: original title '{original_title}' -> cleaned '{clean_title_text}'")
    
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
        
    response = requests.get(url, params=params)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            return results[0]
    
    # Second attempt: only use cleaned title, without year limitation
    if year:
        params.pop("year")
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
    if not imdb_id and ' ' in title:
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
    
    return None

def get_movies_without_director_or_imdb():
    """
    Get movies from the database without director information or IMDB ID
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, title_en, year, director, title_cn, imdb_id
        FROM movies 
        WHERE imdb_id IS NULL OR director IS NULL OR director = ''
        ORDER BY id
    """)
    
    movies = cursor.fetchall()
    conn.close()
    
    return movies

def get_movies_without_tmdb():
    """
    Get movies from the database without TMDB information
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, title_en, year, director, title_cn
        FROM movies 
        WHERE tmdb_id IS NULL
        ORDER BY id
    """)
    
    movies = cursor.fetchall()
    conn.close()
    
    return movies

def get_movies_without_chinese_overview():
    """
    Get movies from the database without Chinese overview but with English overview
    """
    conn = sqlite3.connect(db_path)
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
    
    return movies

def add_director_column():
    """
    Add director_cn column to the database if it doesn't exist
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if director_cn column exists
    cursor.execute("PRAGMA table_info(movies)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if "director_cn" not in columns:
        print("Adding director_cn column to movies table...")
        cursor.execute("ALTER TABLE movies ADD COLUMN director_cn TEXT")
        conn.commit()
    
    conn.close()

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
    
    # Update database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE movies
        SET 
            title_cn = ?,
            overview_cn = ?,
            overview_en = ?,
            vote_average = ?,
            poster_path = ?,
            tmdb_id = ?,
            imdb_id = ?,
            director = ?,
            director_cn = ?
        WHERE id = ?
    """, (
        title_cn, overview_cn, overview_en, vote_average, poster_path, 
        tmdb_id, imdb_id, director_en, director_cn, movie_id
    ))
    
    conn.commit()
    conn.close()
    
    print(f"Updated movie ID {movie_id} with TMDB data")
    return True

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
    
    # Update database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE movies
        SET 
            imdb_id = ?,
            director = ?,
            overview_en = ?,
            imdbRating = ?
        WHERE id = ?
    """, (
        imdb_id, director, plot, imdb_rating, movie_id
    ))
    
    conn.commit()
    conn.close()
    
    print(f"Updated movie ID {movie_id} with OMDb data")
    return True

def update_chinese_overview():
    """
    Update Chinese overview for movies that have English overview but no Chinese overview
    """
    movies = get_movies_without_chinese_overview()
    print(f"Found {len(movies)} movies without Chinese overview")
    
    updated_count = 0
    for movie in movies:
        movie_id, title_en, year, director, title_cn, imdb_id, overview_en = movie
        
        print(f"\nProcessing: {title_cn or title_en} (ID: {movie_id})")
        
        # Try to get Chinese overview from TMDB if we have tmdb_id
        tmdb_id = None
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT tmdb_id FROM movies WHERE id = ?", (movie_id,))
        result = cursor.fetchone()
        if result and result[0]:
            tmdb_id = result[0]
        conn.close()
        
        overview_cn = None
        
        # Try to get from TMDB with different Chinese language codes
        if tmdb_id:
            # Try different Chinese language codes in order of preference
            language_codes = ["zh-CN", "zh-TW", "zh-HK", "zh"]
            
            for lang_code in language_codes:
                if overview_cn:
                    break
                    
                url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
                params = {
                    "api_key": TMDB_API_KEY,
                    "language": lang_code
                }
                
                try:
                    response = requests.get(url, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get("overview") and data.get("overview").strip():
                            overview_cn = data.get("overview")
                            print(f"Found Chinese overview from TMDB using language: {lang_code}")
                            break
                except Exception as e:
                    print(f"Error accessing TMDB API with language {lang_code}: {str(e)}")
                
                # Brief delay to avoid API rate limits
                time.sleep(0.5)
            
            # If still no overview, try using translations endpoint
            if not overview_cn:
                try:
                    trans_url = f"{TMDB_BASE_URL}/movie/{tmdb_id}/translations"
                    trans_params = {
                        "api_key": TMDB_API_KEY
                    }
                    
                    response = requests.get(trans_url, trans_params)
                    if response.status_code == 200:
                        data = response.json()
                        translations = data.get("translations", [])
                        
                        # Look for Chinese translations
                        for trans in translations:
                            if trans.get("iso_639_1") == "zh" and trans.get("data", {}).get("overview"):
                                overview_cn = trans.get("data", {}).get("overview")
                                print(f"Found Chinese overview from TMDB translations API")
                                break
                except Exception as e:
                    print(f"Error accessing TMDB translations API: {str(e)}")
        
        # Update the database if we have a Chinese overview
        if overview_cn:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE movies
                SET overview_cn = ?
                WHERE id = ?
            """, (overview_cn, movie_id))
            
            conn.commit()
            conn.close()
            
            print(f"Updated Chinese overview for movie ID {movie_id}")
            updated_count += 1
            
            # Brief delay to avoid API rate limits
            time.sleep(0.5)
    
    print(f"\nUpdate summary:")
    print(f"- Updated Chinese overviews: {updated_count} of {len(movies)}")
    # Print reminder for remaining movies
    if len(movies) - updated_count > 0:
        print(f"- {len(movies) - updated_count} movies still need Chinese overviews")
        print("  Consider adding these manually as special cases in the update_special_cases() function.")

def update_special_cases():
    """
    Update information for special cases like shorts and collections
    that might not be available in regular movie databases
    """
    special_cases = {
        39: {  # This Long Century Presents: Ari Marcopoulos
            "title_cn": "本世纪长镜头：阿里·马尔科普洛斯",
            "overview_en": "A special presentation by filmmaker and photographer Ari Marcopoulos, exploring his visual works and cinematic contributions.",
            "overview_cn": "由电影制作人和摄影师阿里·马尔科普洛斯(Ari Marcopoulos)带来的特别展示，探索他的视觉作品和电影贡献。"
        },
        70: {  # Weird Medicine Shorts
            "title_cn": "奇怪医学短片系列",
            "overview_en": "A collection of short films exploring unusual medical practices, experimental treatments, and the strange history of medicine.",
            "overview_cn": "一系列探索不寻常医疗实践、实验性治疗和医学奇特历史的短片集。"
        }
    }
    
    updated_count = 0
    for movie_id, info in special_cases.items():
        print(f"\nProcessing special case: ID {movie_id}")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        update_fields = []
        update_values = []
        
        for field, value in info.items():
            if value:
                update_fields.append(f"{field} = ?")
                update_values.append(value)
        
        if update_fields:
            query = f"UPDATE movies SET {', '.join(update_fields)} WHERE id = ?"
            update_values.append(movie_id)
            
            cursor.execute(query, update_values)
            conn.commit()
            
            print(f"Updated special case movie ID {movie_id}")
            updated_count += 1
            
        conn.close()
    
    print(f"\nUpdate summary:")
    print(f"- Updated special cases: {updated_count} of {len(special_cases)}")

def import_metrograph_data():
    """
    Import Metrograph movie data from JSON file into the database.
    """
    # 获取Metrograph JSON文件路径
    metrograph_json_path = os.path.join(script_dir, "database", "metrograph_movies.json")
    
    if not os.path.exists(metrograph_json_path):
        print(f"❌ Metrograph data file not found: {metrograph_json_path}")
        return 0, 0
    
    try:
        # 加载Metrograph电影数据
        print(f"加载Metrograph数据: {metrograph_json_path}...")
        with open(metrograph_json_path, 'r', encoding='utf-8') as f:
            movies_data = json.load(f)
        
        print(f"找到 {len(movies_data)} 部电影需要导入")
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        imported_movies = 0
        imported_screenings = 0
        
        for movie_data in movies_data:
            title_en = movie_data.get('title_en')
            if not title_en:
                print(f"跳过没有标题的电影: {movie_data}")
                continue
                
            # 检查电影是否已存在
            cursor.execute("""
                SELECT id FROM movies 
                WHERE title_en = ? AND cinema = 'Metrograph'
            """, (title_en,))
            
            existing_movie = cursor.fetchone()
            
            if existing_movie:
                movie_id = existing_movie['id']
                print(f"电影已存在: {title_en} (ID: {movie_id})")
                
                # 更新现有电影数据
                update_fields = []
                update_values = []
                
                if movie_data.get('director'):
                    update_fields.append("director = ?")
                    update_values.append(movie_data.get('director'))
                
                if movie_data.get('year'):
                    update_fields.append("year = ?")
                    update_values.append(movie_data.get('year'))
                
                if movie_data.get('overview_en'):
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
                
                if movie_data.get('duration'):
                    update_fields.append("duration = ?")
                    update_values.append(movie_data.get('duration'))
                
                # 增加Q&A等信息
                if movie_data.get('has_qa'):
                    update_fields.append("has_qa = ?")
                    update_values.append(movie_data.get('has_qa'))
                    
                    if movie_data.get('qa_details'):
                        update_fields.append("qa_details = ?")
                        update_values.append(movie_data.get('qa_details'))
                
                if movie_data.get('has_introduction'):
                    update_fields.append("has_introduction = ?")
                    update_values.append(movie_data.get('has_introduction'))
                    
                    if movie_data.get('introduction_details'):
                        update_fields.append("introduction_details = ?")
                        update_values.append(movie_data.get('introduction_details'))
                
                if update_fields:
                    query = f"UPDATE movies SET {', '.join(update_fields)} WHERE id = ?"
                    update_values.append(movie_id)
                    
                    cursor.execute(query, update_values)
                    conn.commit()
                    print(f"  - 更新电影信息: {title_en}")
            else:
                # 插入新电影
                cursor.execute("""
                    INSERT INTO movies (
                        title_en, director, year, cinema, detail_url, 
                        image_url, overview_en, trailer_url, duration,
                        has_qa, qa_details, has_introduction, introduction_details
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    title_en,
                    movie_data.get('director', ''),
                    movie_data.get('year'),
                    'Metrograph',
                    movie_data.get('detail_url', ''),
                    movie_data.get('image_url', ''),
                    movie_data.get('overview_en', ''),
                    movie_data.get('trailer_url', ''),
                    movie_data.get('duration', ''),
                    movie_data.get('has_qa', False),
                    movie_data.get('qa_details', ''),
                    movie_data.get('has_introduction', False),
                    movie_data.get('introduction_details', '')
                ))
                
                movie_id = cursor.lastrowid
                conn.commit()
                imported_movies += 1
                print(f"  - 导入新电影: {title_en} (ID: {movie_id})")
            
            # 处理放映信息
            if movie_data.get('show_dates'):
                # 首先删除该电影在Metrograph的旧放映信息
                cursor.execute("""
                    DELETE FROM screenings 
                    WHERE movie_id = ? AND cinema = 'Metrograph'
                """, (movie_id,))
                
                # 添加新的放映信息
                for date_info in movie_data.get('show_dates', []):
                    for time_info in date_info.get('times', []):
                        screening_date = date_info.get('date')
                        screening_time = time_info.get('time')
                        sold_out = time_info.get('sold_out', False)
                        ticket_url = time_info.get('ticket_url', '')
                        
                        if screening_date and screening_time:
                            # 检查日期是否已经是YYYY-MM-DD格式
                            if not screening_date.startswith('20'):
                                # 尝试解析日期
                                try:
                                    from datetime import datetime
                                    parsed_date = datetime.strptime(screening_date, "%Y-%m-%d")
                                    screening_date = parsed_date.strftime("%Y-%m-%d")
                                except ValueError:
                                    try:
                                        # 尝试其他日期格式
                                        parsed_date = datetime.strptime(screening_date, "%A %B %d, %Y")
                                        screening_date = parsed_date.strftime("%Y-%m-%d")
                                    except ValueError:
                                        print(f"  - 警告: 无法解析日期: {screening_date}")
                                        continue
                            
                            # 添加放映信息
                            cursor.execute("""
                                INSERT INTO screenings (
                                    movie_id, title_en, cinema, date, time, sold_out, ticket_url
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                movie_id, 
                                title_en, 
                                'Metrograph', 
                                screening_date, 
                                screening_time, 
                                sold_out, 
                                ticket_url
                            ))
                            imported_screenings += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Metrograph数据导入完成!")
        print(f"  - 导入/更新电影: {imported_movies}")
        print(f"  - 导入放映场次: {imported_screenings}")
        
        return imported_movies, imported_screenings
        
    except Exception as e:
        print(f"❌ 导入Metrograph数据出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0, 0

def main():
    # Add Chinese director name field
    add_director_column()
    
    # First, get movies that need TMDB info
    movies_for_tmdb = get_movies_without_tmdb()
    print(f"Found {len(movies_for_tmdb)} movies needing TMDB updates")
    
    # Update TMDB info
    updated_tmdb_count = 0
    for movie in movies_for_tmdb:
        movie_id = movie[0]
        title_en = movie[1]
        year = movie[2]
        director = movie[3]
        title_cn = movie[4]
        
        print(f"\nProcessing: {title_en or title_cn} (ID: {movie_id})")
        
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
        
    # Get movies without director or IMDb ID
    movies_without_info = get_movies_without_director_or_imdb()
    print(f"\nFound {len(movies_without_info)} movies needing director or IMDb ID updates")
    
    # Update missing information using OMDb
    updated_omdb_count = 0
    for movie in movies_without_info:
        movie_id = movie[0]
        title_en = movie[1]
        year = movie[2]
        director = movie[3]
        title_cn = movie[4]
        imdb_id = movie[5]
        
        print(f"\nProcessing for OMDb: {title_en or title_cn} (ID: {movie_id})")
        
        omdb_data = None
        if imdb_id:
            omdb_data = get_omdb_info(None, None, imdb_id)
        
        if not omdb_data and title_en:
            omdb_data = get_omdb_info(title_en, year)
            
        if not omdb_data and title_cn:
            omdb_data = get_omdb_info(title_cn, year)
        
        if omdb_data:
            if update_with_omdb(movie_id, omdb_data, movie):
                updated_omdb_count += 1
                time.sleep(0.5)  # Avoid API rate limits
    
    # Update Chinese overviews
    update_chinese_overview()
    
    # Update special cases
    update_special_cases()
    
    # Import Metrograph data
    print("\nImporting Metrograph data...")
    imported_movies, imported_screenings = import_metrograph_data()
    
    print(f"\nOverall update summary:")
    print(f"- TMDB updates: {updated_tmdb_count} of {len(movies_for_tmdb)}")
    print(f"- OMDb updates: {updated_omdb_count} of {len(movies_without_info)}")
    print(f"- Metrograph movies imported/updated: {imported_movies}")
    print(f"- Metrograph screenings imported: {imported_screenings}")

if __name__ == "__main__":
    main() 