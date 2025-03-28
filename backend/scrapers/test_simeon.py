import asyncio
from bs4 import BeautifulSoup
import re

# From the user query
html_content = """<div class="movie-info"><h1>Siméon</h1><div class="showtimes">
<h5>SELECT SHOWTIME BELOW TO PURCHASE TICKETS</h5><div class="date_picker_holder">Friday April 4</div><div id="day_Friday_April_4" class="film_day"><h5 class="sr-only">Friday April 4</h5> <a href="https://t.metrograph.com/Ticketing/visSelectTickets.aspx?cinemacode=9999&amp;txtSessionId=26491" title="Buy Tickets">6:35pm</a></div></div>
<h5> Director: Euzhan Palcy</h5><h5>1992 / 115min / DCP</h5><p> </p><p>A music teacher (Jean-Claude Duverger) in a small Martinique village, his star student, guitarist Isidore (Jacob Desvarieux, the late leader and co-founder of Kassav')and Isidore's 10-year-old daughter Orélie (Lucinda Messager), are united by a love of up-tempo, percussion-driven Zouk music and a dream of introducing it to the wider world in Palcy's magical-realist musical fantasy. A warm, vibrant, and infinitely charming tale of loss and (literally) supernatural perseverance, featuring a cast loaded with top tier talent from the Francophone Caribbean, including legendary Guadeloupe group Kassav', Pascal Légitimus of the French trio of humorists Les Inconnus, and jazz pianist Alain Jean-Marie. Presented in a stunning 4K restoration and now widely considered Palcy's masterpiece, after the triumph of her sophomore feature A Dry White Season, which made her the first Black woman director to have a film produced by a major Hollywood studio.</p>
<p>Q&amp;A with filmmaker Euzhan Palcy moderated by Director Fits founder Hagop Kourounian on Friday, April 4th</p><p></p><p><a href="/series/?vista_series_id=0000000390">Part of Director Fits x Metrograph: Euzhan Palcy x2</a></p></div>"""

def extract_movie_details():
    """
    Extract details from the HTML content
    """
    details = {}
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract movie title
    title_tag = soup.select_one('h1')
    if title_tag:
        details["title"] = title_tag.get_text(strip=True)
    
    # Extract director
    director_tag = soup.find('h5', string=re.compile(r'Director', re.IGNORECASE))
    if director_tag:
        director_text = director_tag.get_text(strip=True)
        director_match = re.search(r'Director:\s*([^,]+)', director_text, re.IGNORECASE)
        if director_match:
            details["director"] = director_match.group(1).strip()
    
    # Extract year and duration from h5 tags
    h5_tags = soup.find_all('h5')
    for h5 in h5_tags:
        # Match format like "1992 / 115min / DCP"
        year_duration_match = re.search(r'(\d{4})\s*/\s*(\d+)min', h5.get_text(strip=True))
        if year_duration_match:
            details["year"] = int(year_duration_match.group(1))
            details["duration"] = f"{year_duration_match.group(2)} min"
            break
    
    # Extract overview from paragraphs
    overview_tag = None
    p_tags = soup.select('p')
    for p in p_tags:
        if p.get_text(strip=True) and not p.get_text(strip=True).startswith("Q&A"):
            overview_tag = p
            break
    
    if overview_tag:
        details["overview"] = overview_tag.get_text(strip=True)
    
    # Check for Q&A information
    has_qa = False
    qa_details = ""
    
    for p in p_tags:
        text = p.get_text(strip=True)
        qa_match = re.search(r'Q\s*&\s*A\s+with|Q\s*&\s*A\s+featuring|Q\s*&\s*A\s+by|Q\s*&\s*A\s+moderated', text, re.IGNORECASE)
        if qa_match or "Q&A" in text:
            has_qa = True
            qa_details = text
            break
    
    details["has_qa"] = has_qa
    if has_qa:
        details["qa_details"] = qa_details
    
    return details

def main():
    details = extract_movie_details()
    print("Extraction Results:")
    print("-" * 40)
    for key, value in details.items():
        if key == "overview":
            print(f"{key}: {value[:50]}...")
        else:
            print(f"{key}: {value}")
    
    print("\nQ&A Detection:", "✅ Successful" if details.get("has_qa") else "❌ Failed")
    print("Year Extraction:", "✅ Successful" if details.get("year") else "❌ Failed")

if __name__ == "__main__":
    main() 