"""
Service for updating movie information from external sources like TMDB and OMDb.
"""
import re
import time
import requests
import sqlite3
from ..models.movie import Movie
from ..config.settings import TMDB_API_KEY, TMDB_BASE_URL, OMDB_API_KEY, OMDB_BASE_URL, DB_PATH

class MovieUpdater:
    """
    Service to update movie information using external APIs.
    """
    
    @staticmethod
    def get_db_connection():
        """
        获取数据库连接
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    
    @staticmethod
    def is_english(text):
        """
        检查文本是否只包含ASCII字符（基本上是英文）
        """
        if not text:
            return True
        return all(ord(char) < 128 for char in text)
    
    @staticmethod
    def clean_title(title):
        """
        轻度清理标题，主要处理多余空格，用于显示目的
        保留前缀、后缀和格式标记
        """
        if not title:
            return ""
            
        # 处理多余空格
        title = re.sub(r'\s+', ' ', title).strip()
        
        return title.strip()
    
    @staticmethod
    def search_clean_title(title):
        """
        深度清理标题，移除前缀、后缀和格式标记，用于搜索目的
        """
        if not title:
            return ""
            
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
        
        # 移除括号内容
        title = re.sub(r'\s*\(.*?\)\s*', '', title)
        
        # 移除特殊字符
        title = re.sub(r'[^\w\s]', ' ', title)
        
        # 处理多余空格
        title = re.sub(r'\s+', ' ', title).strip()
        
        return title.strip()
    
    @staticmethod
    def search_movie_with_variants(title, year=None, director=None):
        """
        使用多种变体策略搜索电影
        """
        if not title:
            return None
            
        print(f"尝试搜索电影: '{title}'")
        tmdb_movie = None
        
        # 清理标题
        cleaned_title = MovieUpdater.search_clean_title(title)
        
        # 1. 先尝试原始标题搜索
        tmdb_movie = MovieUpdater.search_movie(title, year)
        if tmdb_movie:
            return tmdb_movie
            
        # 2. 尝试清理后的标题搜索
        if cleaned_title != title:
            tmdb_movie = MovieUpdater.search_movie(cleaned_title, year)
            if tmdb_movie:
                return tmdb_movie
                    
        # 3. 尝试不使用年份限制
        tmdb_movie = MovieUpdater.search_movie(title, None)
        if tmdb_movie:
            return tmdb_movie
            
        # 4. 尝试只使用标题的前两个词
        if ' ' in cleaned_title:
            short_title = ' '.join(cleaned_title.split()[:2])
            if short_title != cleaned_title:
                print(f"尝试简化标题: '{short_title}'")
                tmdb_movie = MovieUpdater.search_movie(short_title, None)
                if tmdb_movie:
                    return tmdb_movie
        
        return None
    
    @staticmethod
    def search_movie(title, year=None):
        """
        Search for a movie in TMDB API.
        """
        if not title:
            return None
            
        url = f"{TMDB_BASE_URL}/search/movie"
        search_title = MovieUpdater.search_clean_title(title)
        
        print(f"Searching movie: original title '{title}' -> cleaned for search '{search_title}'")
        
        params = {
            "api_key": TMDB_API_KEY,
            "query": search_title,
            "language": "zh-CN",
        }
        
        if year:
            params["year"] = year
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get("results") and len(data["results"]) > 0:
                return data["results"][0]
                
        # If no results or error, try using only the first two words of the title
        if ' ' in search_title:
            simplified_title = ' '.join(search_title.split()[:2])
            if simplified_title != search_title:
                print(f"Trying simplified title: '{simplified_title}'")
                params["query"] = simplified_title
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("results") and len(data["results"]) > 0:
                        return data["results"][0]
        
        return None
    
    @staticmethod
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
    
    @staticmethod
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
            params["t"] = MovieUpdater.search_clean_title(title)
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
            simplified_title = ' '.join(MovieUpdater.search_clean_title(title).split()[:2])
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
    
    @staticmethod
    def update_movie_with_tmdb(movie_id, tmdb_data, movie_details, original_director=None):
        """
        Update movie information in the database with TMDB data
        """
        # 获取原始电影信息以便可能需要的回退
        original_movie = Movie.get_movie_by_id(movie_id)
        original_title_en = original_movie.get("title_en", "") if original_movie else ""
        
        # Extract data
        title_cn = tmdb_data.get("title", "")
        overview_cn = tmdb_data.get("overview", "")
        overview_en = tmdb_data.get("overview_en", "")
        vote_average = tmdb_data.get("vote_average")
        poster_path = tmdb_data.get("poster_path")
        tmdb_id = tmdb_data.get("id")
        imdb_id = None
        
        # 检查中文标题是否实际上是英文
        # 如果获取的"中文"标题实际上是英文，则使用原始英文标题（保留所有格式元素）
        if MovieUpdater.is_english(title_cn) and original_title_en:
            print(f"获取的中文标题 '{title_cn}' 实际是英文，使用原始英文标题 '{original_title_en}' 作为中文标题")
            title_cn = original_title_en
        
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
        
        # Get trailer URL
        trailer_url = None
        if "videos" in movie_details and movie_details["videos"].get("results"):
            trailers = [v for v in movie_details["videos"]["results"] if v.get("type") == "Trailer" and v.get("site") == "YouTube"]
            if trailers:
                trailer_key = trailers[0].get("key")
                if trailer_key:
                    trailer_url = f"https://www.youtube.com/watch?v={trailer_key}"
        
        # Create image URL if available
        image_url = None
        if poster_path:
            image_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
        
        # Prepare data for update
        update_data = {
            "title_cn": title_cn,
            "overview_en": overview_en,
            "overview_cn": overview_cn,
            "director": director_en,
            "director_cn": director_cn,
            "tmdb_id": tmdb_id,
            "rating": vote_average
        }
        
        # Add non-empty fields
        if imdb_id:
            update_data["imdb_id"] = imdb_id
        if trailer_url:
            update_data["trailer_url"] = trailer_url
        if image_url:
            update_data["image_url"] = image_url
        
        # Update database
        result = Movie.update_movie(movie_id, update_data)
        
        if result:
            print(f"Updated movie ID {movie_id} with TMDB data")
        
        return result
    
    @staticmethod
    def update_movie_with_omdb(movie_id, omdb_data):
        """
        Update movie information with OMDb data
        """
        if not omdb_data:
            return False
            
        imdb_id = omdb_data.get("imdbID")
        director = omdb_data.get("Director", "").replace("N/A", "")
        plot = omdb_data.get("Plot", "").replace("N/A", "")
        imdb_rating = omdb_data.get("imdbRating", "").replace("N/A", "")
        
        if imdb_rating:
            try:
                imdb_rating = float(imdb_rating)
            except ValueError:
                imdb_rating = None
        
        # Only update if we have meaningful data
        if not (imdb_id or director or plot or imdb_rating):
            return False
        
        # Prepare data for update
        update_data = {}
        
        if imdb_id:
            update_data["imdb_id"] = imdb_id
        if director:
            update_data["director"] = director
        if plot:
            update_data["overview_en"] = plot
        if imdb_rating:
            update_data["rating"] = imdb_rating
        
        # Update database
        result = Movie.update_movie(movie_id, update_data)
        
        if result:
            print(f"Updated movie ID {movie_id} with OMDb data")
        
        return result
    
    @staticmethod
    def update_chinese_overview(movie_id):
        """
        Update Chinese overview for a movie using TMDB API
        """
        movie = Movie.get_movie_by_id(movie_id)
        if not movie:
            return False
            
        if movie.get("overview_cn"):
            return False  # Already has Chinese overview
            
        if not movie.get("overview_en"):
            return False  # No English overview to work with
            
        tmdb_id = movie.get("tmdb_id")
        if not tmdb_id:
            return False  # No TMDB ID to use
            
        overview_cn = None
        
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
            update_data = {"overview_cn": overview_cn}
            result = Movie.update_movie(movie_id, update_data)
            
            if result:
                print(f"Updated Chinese overview for movie ID {movie_id}")
            
            # Brief delay to avoid API rate limits
            time.sleep(0.5)
            
            return result
            
        return False
    
    @staticmethod
    def get_movie_by_tmdb_id(tmdb_id):
        """直接通过TMDB ID获取电影信息"""
        if not tmdb_id:
            return None
            
        from ..config.settings import TMDB_API_KEY, TMDB_BASE_URL
        import requests
            
        url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "zh-CN"
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"通过TMDB ID搜索时出错: {e}")
        
        return None
    
    @staticmethod
    def search_movie_by_imdb(imdb_id):
        """通过IMDb ID在TMDB搜索电影"""
        if not imdb_id:
            return None
            
        from ..config.settings import TMDB_API_KEY, TMDB_BASE_URL
        import requests
            
        url = f"{TMDB_BASE_URL}/find/{imdb_id}"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "zh-CN",
            "external_source": "imdb_id"
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                movie_results = data.get("movie_results", [])
                if movie_results:
                    return movie_results[0]
        except Exception as e:
            print(f"通过IMDb ID搜索时出错: {e}")
        
        return None 