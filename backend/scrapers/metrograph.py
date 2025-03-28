from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import re

def scrape_metrograph_with_playwright():
    url = "https://metrograph.com/nyc/"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Run headless browser
        page = browser.new_page()
        page.goto(url, timeout=60000)  # Navigate to Metrograph movies page
        page.wait_for_load_state("networkidle")  # Wait for JavaScript to fully load
        html_content = page.content()  # Get full HTML
        browser.close()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, "html.parser")
    movies = []

    # Find all movie information containers
    for movie_section in soup.find_all("div", class_="homepage-in-theater-movie"):
        title_element = movie_section.find("h3", class_="movie_title")
        link_element = title_element.find("a") if title_element else None
        image_element = movie_section.find("img")
        director_element = movie_section.find("h5", string=lambda text: text and "Director:" in text)
        
        # Get year information (usually in h5 tag after director info)
        year_element = None
        if director_element:
            year_element = director_element.find_next_sibling("h5")
        
        # Parse data
        title = title_element.get_text(strip=True) if title_element else None
        detail_url = f"https://metrograph.com{link_element['href']}" if link_element else None
        image_url = image_element["src"] if image_element else None
        director = director_element.get_text(strip=True).replace("Director: ", "") if director_element else None
        
        # Extract year
        year = None
        if year_element:
            year_text = year_element.get_text(strip=True)
            year_match = re.search(r'^(\d{4})\s*/\s*\d+min', year_text)
            if year_match:
                year = int(year_match.group(1))

        # Get all screening dates and times
        show_dates = []
        date_picker = movie_section.find("ul", class_="film_day_chooser")

        if date_picker:
            # Loop through all available dates
            for date_option in date_picker.find_all("a", {"data-day": True}):
                date_text = date_option.get_text(strip=True)
                data_day = date_option["data-day"]

                # Find movie time information for the corresponding date
                time_container = movie_section.find("div", id=f"day_{data_day}")
                if not time_container:
                    continue

                times = []
                for time_link in time_container.find_all("a"):
                    time_text = time_link.get_text(strip=True)
                    ticket_url = time_link["href"] if time_link.has_attr("href") else None
                    sold_out = "sold out" in time_link.get("title", "").lower()

                    times.append({
                        "time": time_text,
                        "ticket_url": ticket_url,
                        "sold_out": sold_out
                    })

                show_dates.append({"date": date_text, "times": times})

        # Store in list
        if title:
            movies.append({
                "title_en": title,
                "show_dates": show_dates,  # List of dates + times
                "detail_url": detail_url,
                "image_url": image_url,
                "director": director,
                "year": year,  # Add year information
                "cinema": "Metrograph"
            })

    return movies

# Run scraper
movies = scrape_metrograph_with_playwright()

# Save as JSON
with open("metrograph_movies.json", "w", encoding="utf-8") as f:
    json.dump(movies, f, indent=4, ensure_ascii=False)

print("✅ Scraping completed, data saved to metrograph_movies.json")