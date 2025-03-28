"""
Utility for generating Douban search links for movies.
"""
import urllib.parse

def generate_douban_search_url(title_cn=None, title_en=None, year=None):
    """
    Generate a Douban search URL for a movie.
    
    Args:
        title_cn: Chinese title of the movie
        title_en: English title of the movie
        year: Release year of the movie
    
    Returns:
        URL string to search for the movie on Douban
    """
    # Use Chinese title first if available
    search_query = title_cn if title_cn else title_en
    
    if not search_query:
        return None
    
    # Add year to search query if available
    if year:
        search_query += f" {year}"
    
    # URL encode the search query
    encoded_query = urllib.parse.quote(search_query)
    
    return f"https://www.douban.com/search?q={encoded_query}" 