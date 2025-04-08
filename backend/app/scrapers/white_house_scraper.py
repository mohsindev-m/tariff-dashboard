import time
import random
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("combined_scraper_debug.log"),
        logging.StreamHandler()
    ]
)
# ------------------------------
# Configuration and Constants
# ------------------------------
BASE_URL = "https://www.whitehouse.gov/presidential-actions/"
MAX_PAGES = 10 
REQUEST_DELAY = 1  


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 13_6 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1"
]

# Create a persistent session with a retry strategy for robustness
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=0.3,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def get_random_headers():
    """Generate dynamic headers with a random user agent."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"
    }
    logging.debug(f"Generated headers: {headers}")
    return headers

def fetch_page(url):
    """Fetch page content with retries, error handling, and logging."""
    headers = get_random_headers()
    try:
        logging.info(f"Fetching URL: {url}")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logging.info(f"Success: {url} responded with {response.status_code}")
        logging.debug(f"Response headers: {response.headers}")
        return response.content
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def parse_post_list(content, base_url):
    """
    Parse the listing page to extract post details:
      - Title
      - URL (link to full content)
      - Publication date
      - Categories (as comma-separated string)
    """
    soup = BeautifulSoup(content, "html.parser")
    posts = []

    for li in soup.find_all("li", class_="wp-block-post"):
        try:
            title_tag = li.find("h2", class_="wp-block-post-title")
            a_tag = title_tag.find("a") if title_tag else None
            title = a_tag.get_text(strip=True) if a_tag else "No Title"
            url = urljoin(base_url, a_tag["href"]) if a_tag and a_tag.has_attr("href") else None
            time_tag = li.find("time")
            pub_date = time_tag["datetime"] if time_tag and time_tag.has_attr("datetime") else "Unknown Date"
            categories = []
            cat_div = li.find("div", class_="taxonomy-category")
            if cat_div:
                for cat in cat_div.find_all("a"):
                    categories.append(cat.get_text(strip=True))
            post_data = {
                "title": title,
                "url": url,
                "pub_date": pub_date,
                "categories": ", ".join(categories)
            }
            posts.append(post_data)
            logging.debug(f"Extracted post: {post_data}")
        except Exception as e:
            logging.error(f"Error parsing post in listing: {e}")
    logging.info(f"Parsed {len(posts)} posts from the current page.")
    return posts

def scrape_full_text(post_url):
    """
    Scrape the full text content from an individual post page.
    First attempt extraction from a div with the class 'entry-content'
    (used in your manual scraper) and if not found, fallback to 'wp-block-group'
    (used in your enterprise scraper).
    """
    content = fetch_page(post_url)
    if not content:
        return ""
    soup = BeautifulSoup(content, "html.parser")
    
    # Try primary selector from the second scraper
    entry_div = soup.find("div", class_="entry-content")
    if entry_div:
        paragraphs = entry_div.find_all("p")
        full_text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)
        logging.info(f"Scraped full text from {post_url} using 'entry-content' (length: {len(full_text)} characters)")
        return full_text

    # Fallback: use the selector from the first scraper
    group_div = soup.find("div", class_="wp-block-group")
    if group_div:
        full_text = group_div.get_text(separator=" ", strip=True)
        logging.info(f"Scraped full text from {post_url} using 'wp-block-group' (length: {len(full_text)} characters)")
        return full_text

    logging.info(f"No content container found for {post_url}")
    return ""

def find_next_page(content, current_url):
    """
    Discover the next page URL from pagination links.
    Checks first for navigation inside a <nav> with class 'pagination',
    then looks for any <a> with "Next" in its text.
    """
    soup = BeautifulSoup(content, "html.parser")
    next_link = None
    pagination = soup.find("nav", class_="pagination")
    if pagination:
        a_next = pagination.find("a", string=lambda s: s and "Next" in s)
        if a_next and a_next.has_attr("href"):
            next_link = urljoin(current_url, a_next["href"])
    else:
        a_next = soup.find("a", string=lambda s: s and "Next" in s)
        if a_next and a_next.has_attr("href"):
            next_link = urljoin(current_url, a_next["href"])
    if next_link:
        logging.info(f"Found next page: {next_link}")
    else:
        logging.info("No next page found.")
    return next_link

def enterprise_scraper(start_url, max_pages):
    """
    Scrape multiple pages by processing the listing, 
    following internal links for full content, and handling pagination.
    """
    current_url = start_url
    all_posts = []
    page_count = 0

    while current_url and page_count < max_pages:
        logging.info(f"Processing page {page_count + 1}: {current_url}")
        page_content = fetch_page(current_url)
        if not page_content:
            logging.error(f"Failed to fetch content from {current_url}. Stopping pagination.")
            break

        posts = parse_post_list(page_content, current_url)
        for post in posts:
            if post["url"]:
                logging.debug(f"Scraping full text for post: {post['title']} from {post['url']}")
                post["full_text"] = scrape_full_text(post["url"])
            else:
                post["full_text"] = ""
            all_posts.append(post)
            time.sleep(REQUEST_DELAY)  # Pause between post requests

        next_page = find_next_page(page_content, current_url)
        current_url = next_page
        page_count += 1
        time.sleep(REQUEST_DELAY)  # Pause between page requests

    logging.info(f"Scraping complete. Total posts collected: {len(all_posts)}")
    return all_posts

def store_to_excel(data, filename="scraped_data.xlsx"):
    """Store scraped data into an Excel file using pandas."""
    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    logging.info(f"Data successfully written to {filename}")

if __name__ == "__main__":
    scraped_posts = enterprise_scraper(BASE_URL, MAX_PAGES)
    store_to_excel(scraped_posts)
    print("Scraping complete.")