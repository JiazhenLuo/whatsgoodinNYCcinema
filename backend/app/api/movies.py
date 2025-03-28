"""
API routes for movie operations.
"""
from flask import Blueprint, request
from ..models.movie import Movie
from ..services.movie_updater import MovieUpdater
from ..config.settings import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from .json_fix import jsonify

movies_bp = Blueprint('movies', __name__)

@movies_bp.route('/', methods=['GET'])
def get_movies():
    """
    Get all movies with pagination.
    
    Query parameters:
        page (int): Page number (1-indexed)
        limit (int): Number of items per page
    """
    page = max(1, int(request.args.get('page', 1)))
    limit = min(MAX_PAGE_SIZE, int(request.args.get('limit', DEFAULT_PAGE_SIZE)))
    
    movies = Movie.get_all_movies(page=page, limit=limit)
    total = Movie.count_all_movies()
    
    return jsonify({
        'data': movies,
        'meta': {
            'page': page,
            'limit': limit,
            'total': total,
            'total_pages': (total + limit - 1) // limit
        }
    })

@movies_bp.route('/<int:movie_id>', methods=['GET'])
def get_movie(movie_id):
    """
    Get a movie by ID.
    """
    movie = Movie.get_movie_by_id(movie_id)
    
    if not movie:
        return jsonify({'error': 'Movie not found'}), 404
    
    return jsonify({'data': movie})

@movies_bp.route('/recent', methods=['GET'])
def get_recent_movies():
    """
    Get recently added movies.
    
    Query parameters:
        days (int): Number of days to look back
        page (int): Page number (1-indexed)
        limit (int): Number of items per page
    """
    days = int(request.args.get('days', 7))
    page = max(1, int(request.args.get('page', 1)))
    limit = min(MAX_PAGE_SIZE, int(request.args.get('limit', DEFAULT_PAGE_SIZE)))
    
    movies = Movie.get_recent_movies(days=days, page=page, limit=limit)
    
    return jsonify({'data': movies})

@movies_bp.route('/<int:movie_id>/refresh', methods=['POST'])
def refresh_movie(movie_id):
    """
    Refresh movie information from external APIs.
    """
    movie = Movie.get_movie_by_id(movie_id)
    
    if not movie:
        return jsonify({'error': 'Movie not found'}), 404
    
    # 提取电影信息
    title_en = movie.get('title_en')
    title_cn = movie.get('title_cn')
    year = movie.get('year')
    director = movie.get('director')
    
    updated = False
    
    # 使用多种策略查找电影
    tmdb_movie = None
    
    # 策略1: 如果有tmdb_id，直接获取电影信息
    if movie.get('tmdb_id'):
        tmdb_id = movie.get('tmdb_id')
        print(f"使用TMDB ID搜索: {tmdb_id}")
        tmdb_movie = MovieUpdater.get_movie_by_tmdb_id(tmdb_id)
    
    # 策略2: 如果有imdb_id，通过imdb_id查找
    if not tmdb_movie and movie.get('imdb_id'):
        imdb_id = movie.get('imdb_id')
        print(f"使用IMDb ID搜索: {imdb_id}")
        tmdb_movie = MovieUpdater.search_movie_by_imdb(imdb_id)
    
    # 策略3: 使用智能搜索（包含多变体策略）
    if not tmdb_movie and title_en:
        print(f"使用智能搜索: {title_en}")
        tmdb_movie = MovieUpdater.search_movie_with_variants(title_en, year, director)
    
    # 策略4: 使用中文标题搜索
    if not tmdb_movie and title_cn:
        tmdb_movie = MovieUpdater.search_movie(title_cn, year)
    
    if tmdb_movie:
        movie_details = MovieUpdater.get_movie_details(tmdb_movie.get('id'))
        if movie_details:
            MovieUpdater.update_movie_with_tmdb(movie_id, tmdb_movie, movie_details, director)
            updated = True
    
    # 尝试使用OMDb API更新
    omdb_data = None
    imdb_id = movie.get('imdb_id')
    
    if imdb_id:
        omdb_data = MovieUpdater.get_omdb_info(None, None, imdb_id)
    
    if not omdb_data and title_en:
        omdb_data = MovieUpdater.get_omdb_info(title_en, year)
        
    if not omdb_data and title_cn:
        omdb_data = MovieUpdater.get_omdb_info(title_cn, year)
    
    if omdb_data:
        MovieUpdater.update_movie_with_omdb(movie_id, omdb_data)
        updated = True
    
    # 如果需要，更新中文简介
    if movie.get('overview_en') and not movie.get('overview_cn'):
        if MovieUpdater.update_chinese_overview(movie_id):
            updated = True
    
    if updated:
        updated_movie = Movie.get_movie_by_id(movie_id)
        return jsonify({
            'message': 'Movie information refreshed from external APIs',
            'data': updated_movie
        })
    else:
        return jsonify({
            'message': 'No new information found for this movie',
            'data': movie
        }) 