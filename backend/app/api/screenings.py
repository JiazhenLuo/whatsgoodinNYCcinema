"""
API routes for screening operations.
"""
from flask import Blueprint, request
from ..models.screening import Screening
from ..config.settings import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from .json_fix import jsonify

screenings_bp = Blueprint('screenings', __name__)

@screenings_bp.route('/', methods=['GET'])
def get_screenings():
    """
    Get all screenings with pagination.
    
    Query parameters:
        page (int): Page number (1-indexed)
        limit (int): Number of items per page
    """
    page = max(1, int(request.args.get('page', 1)))
    limit = min(MAX_PAGE_SIZE, int(request.args.get('limit', DEFAULT_PAGE_SIZE)))
    
    screenings = Screening.get_all_screenings(page=page, limit=limit)
    total = Screening.count_all_screenings()
    
    return jsonify({
        'data': screenings,
        'meta': {
            'page': page,
            'limit': limit,
            'total': total,
            'total_pages': (total + limit - 1) // limit
        }
    })

@screenings_bp.route('/<int:screening_id>', methods=['GET'])
def get_screening(screening_id):
    """
    Get a screening by ID.
    """
    screening = Screening.get_screening_by_id(screening_id)
    
    if not screening:
        return jsonify({'error': 'Screening not found'}), 404
    
    return jsonify({'data': screening})

@screenings_bp.route('/upcoming', methods=['GET'])
def get_upcoming_screenings():
    """
    Get upcoming screenings.
    
    Query parameters:
        days (int): Number of days to look ahead
        page (int): Page number (1-indexed)
        limit (int): Number of items per page
    """
    days = int(request.args.get('days', 7))
    page = max(1, int(request.args.get('page', 1)))
    limit = min(MAX_PAGE_SIZE, int(request.args.get('limit', DEFAULT_PAGE_SIZE)))
    
    screenings = Screening.get_upcoming_screenings(days=days, page=page, limit=limit)
    
    return jsonify({'data': screenings})

@screenings_bp.route('/by-cinema/<cinema>', methods=['GET'])
def get_screenings_by_cinema(cinema):
    """
    Get screenings by cinema.
    
    Query parameters:
        page (int): Page number (1-indexed)
        limit (int): Number of items per page
    """
    page = max(1, int(request.args.get('page', 1)))
    limit = min(MAX_PAGE_SIZE, int(request.args.get('limit', DEFAULT_PAGE_SIZE)))
    
    screenings = Screening.get_screenings_by_cinema(cinema, page=page, limit=limit)
    
    return jsonify({'data': screenings})

@screenings_bp.route('/by-movie/<int:movie_id>', methods=['GET'])
def get_screenings_by_movie(movie_id):
    """
    Get screenings by movie ID.
    
    Query parameters:
        page (int): Page number (1-indexed)
        limit (int): Number of items per page
    """
    page = max(1, int(request.args.get('page', 1)))
    limit = min(MAX_PAGE_SIZE, int(request.args.get('limit', DEFAULT_PAGE_SIZE)))
    
    screenings = Screening.get_screenings_by_movie_id(movie_id)
    
    return jsonify({'data': screenings}) 