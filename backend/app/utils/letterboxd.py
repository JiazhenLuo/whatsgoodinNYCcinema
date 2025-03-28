"""
Utility for generating Letterboxd links for movies.
"""
import urllib.parse
import re

def generate_letterboxd_url(title_en=None, year=None):
    """
    Generate a Letterboxd URL for a movie.
    
    Args:
        title_en: English title of the movie
        year: Release year of the movie
    
    Returns:
        URL string to the movie on Letterboxd
    """
    if not title_en:
        return None
    
    # Clean the title - remove special characters and replace spaces with hyphens
    clean_title = re.sub(r'[^\w\s-]', '', title_en.lower())
    clean_title = re.sub(r'[\s]+', '-', clean_title)
    
    # Build the URL
    url = f"https://letterboxd.com/film/{clean_title}"
    
    # Add year if available
    if year:
        url += f"-{year}"
    
    return url 