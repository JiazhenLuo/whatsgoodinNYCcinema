"""
Application settings and configurations.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment
ENV = os.getenv('FLASK_ENV', 'development')
DEBUG = ENV == 'development'

# Base directory paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
APP_DIR = BASE_DIR / "app"

# Database
DB_PATH = BASE_DIR / "database" / "movies.db"

# API settings
API_VERSION = 'v1'
API_PREFIX = f'/api/{API_VERSION}'

# API keys
TMDB_API_KEY = os.getenv('TMDB_API_KEY')
OMDB_API_KEY = "85a51227"  # Hard-coded as in original scripts

# API URLs
TMDB_BASE_URL = "https://api.themoviedb.org/3"
OMDB_BASE_URL = "http://www.omdbapi.com/"

# Data directories
DATA_DIR = BASE_DIR / "data"
JSON_DATA_DIR = DATA_DIR / "json"

# Pagination
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100 