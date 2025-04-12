#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Improved White House Scraper for Tariff Dashboard

This module provides an enhanced scraper for White House presidential actions,
with specific focus on identifying and extracting tariff and trade-related content.
"""

import os
import time
import random
import logging
import json
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("whitehouse_scraper.log"),
        logging.StreamHandler()
    ]
)

# ------------------------------
# Configuration and Constants
# ------------------------------
BASE_URL = "https://www.whitehouse.gov/presidential-actions/"
MAX_PAGES = 10 
REQUEST_DELAY = 1.5  # Slightly increased delay to be more respectful

# Keywords to identify tariff and trade-related content
TARIFF_KEYWORDS = [
    'tariff', 'trade', 'import', 'export', 'commerce', 'duty', 'duties',
    'customs', 'wto', 'trade deficit', 'reciprocal', 'section 301', 'trade act',
    'trade agreement', 'trade policy', 'nafta', 'usmca', 'trade representative'
]

# User agents for request rotation
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

def create_session():
    """Create a persistent session with a retry strategy for robustness."""
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
    return session

def get_random_headers():
    """Generate dynamic headers with a random user agent."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"
    }
    logging.debug(f"Generated headers: {headers}")
    return headers

def fetch_page(url, session):
    """Fetch page content with retries, error handling, and logging."""
    headers = get_random_headers()
    try:
        logging.info(f"Fetching URL: {url}")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logging.info(f"Success: {url} responded with {response.status_code}")
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

def scrape_full_text(post_url, session):
    """
    Scrape the full text content from an individual post page.
    First attempt extraction from a div with the class 'entry-content'
    and if not found, fallback to 'wp-block-group'.
    """
    content = fetch_page(post_url, session)
    if not content:
        return ""
    
    soup = BeautifulSoup(content, "html.parser")
    
    # Try primary selector
    entry_div = soup.find("div", class_="entry-content")
    if entry_div:
        paragraphs = entry_div.find_all("p")
        full_text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)
        logging.info(f"Scraped full text from {post_url} using 'entry-content' (length: {len(full_text)} characters)")
        return full_text

    # Fallback selector
    group_div = soup.find("div", class_="wp-block-group")
    if group_div:
        full_text = group_div.get_text(separator=" ", strip=True)
        logging.info(f"Scraped full text from {post_url} using 'wp-block-group' (length: {len(full_text)} characters)")
        return full_text

    # Second fallback: try to get any content
    main_content = soup.find("main")
    if main_content:
        full_text = main_content.get_text(separator=" ", strip=True)
        logging.info(f"Scraped full text from {post_url} using 'main' (length: {len(full_text)} characters)")
        return full_text

    logging.warning(f"No content container found for {post_url}")
    return ""

def find_next_page(content, current_url):
    """
    Discover the next page URL from pagination links.
    Checks first for navigation inside a <nav> with class 'pagination',
    then looks for any <a> with "Next" in its text.
    """
    soup = BeautifulSoup(content, "html.parser")
    next_link = None
    
    # Try to find pagination navigation
    pagination = soup.find("nav", class_="pagination")
    if pagination:
        a_next = pagination.find("a", string=lambda s: s and "Next" in s)
        if a_next and a_next.has_attr("href"):
            next_link = urljoin(current_url, a_next["href"])
    else:
        # Fallback: look for any "Next" link
        a_next = soup.find("a", string=lambda s: s and "Next" in s)
        if a_next and a_next.has_attr("href"):
            next_link = urljoin(current_url, a_next["href"])
    
    if next_link:
        logging.info(f"Found next page: {next_link}")
    else:
        logging.info("No next page found.")
    
    return next_link

def is_tariff_related(post):
    """
    Determine if a post is related to tariffs or trade based on keywords
    in the title or full text.
    
    Args:
        post: Dictionary containing post details
        
    Returns:
        Boolean indicating if the post is tariff/trade related
    """
    # Convert title and full text to lowercase for case-insensitive matching
    title = post.get("title", "").lower()
    full_text = post.get("full_text", "").lower()
    
    # Check if any keyword appears in the title or full text
    for keyword in TARIFF_KEYWORDS:
        if keyword in title or keyword in full_text:
            logging.info(f"Post identified as tariff-related: '{post['title']}' (keyword: {keyword})")
            return True
    
    return False

def extract_tariff_data(post):
    """
    Extract structured tariff-related data from the post text.
    
    Args:
        post: Dictionary containing post details
        
    Returns:
        Dictionary with extracted tariff information
    """
    full_text = post.get("full_text", "")
    title = post.get("title", "")
    
    tariff_data = {
        "id": post.get("url", "").split("/")[-2] if post.get("url") else None,
        "title": title,
        "publication_date": post.get("pub_date"),
        "url": post.get("url"),
        "countries_mentioned": [],
        "tariff_rates": [],
        "effective_date": None,
        "relevant_excerpt": ""
    }
    
    # Extract countries mentioned
    # This is a simple approach - in production, you'd use a more sophisticated NER
    common_countries = [
        "China", "Mexico", "Canada", "Japan", "Germany", "South Korea",
        "United Kingdom", "Vietnam", "Taiwan", "India", "Russia", "Brazil",
        "France", "Italy", "Australia", "Argentina", "Israel"
    ]
    
    for country in common_countries:
        if country in title or country in full_text:
            tariff_data["countries_mentioned"].append(country)
    
    # Extract tariff rates
    # Look for patterns like X% tariff, X percent tariff
    tariff_patterns = [
        r'(\d+(?:\.\d+)?)(?:\s*)?%(?:\s*)?(?:tariff|duty|tax)',  # e.g., "25% tariff"
        r'tariff(?:\s*)?(?:of|at)?(?:\s*)?(\d+(?:\.\d+)?)(?:\s*)?%',  # e.g., "tariff of 25%"
        r'(\d+(?:\.\d+)?)(?:\s*)?percent(?:\s*)?(?:tariff|duty|tax)',  # e.g., "25 percent tariff"
    ]
    
    for pattern in tariff_patterns:
        matches = re.finditer(pattern, full_text, re.IGNORECASE)
        for match in matches:
            rate = match.group(1)
            tariff_data["tariff_rates"].append(float(rate))
            
            # Extract a relevant excerpt around the tariff rate mention
            start = max(0, match.start() - 150)
            end = min(len(full_text), match.end() + 150)
            excerpt = full_text[start:end].strip()
            if excerpt:
                tariff_data["relevant_excerpt"] = excerpt
    
    # Extract effective date
    date_patterns = [
        r'effective(?:\s+\w+){0,3}\s+(\w+\s+\d{1,2},?\s+\d{4})',  # effective on January 1, 2025
        r'take(?:\s+\w+){0,2}\s+effect(?:\s+\w+){0,3}\s+(\w+\s+\d{1,2},?\s+\d{4})',  # takes effect on January 1, 2025
        r'beginning(?:\s+\w+){0,2}\s+(\w+\s+\d{1,2},?\s+\d{4})',  # beginning on January 1, 2025
        r'implement(?:\s+\w+){0,3}\s+(\w+\s+\d{1,2},?\s+\d{4})'  # implemented on January 1, 2025
    ]
    
    for pattern in date_patterns:
        matches = re.finditer(pattern, full_text, re.IGNORECASE)
        for match in matches:
            tariff_data["effective_date"] = match.group(1)
            break
        if tariff_data["effective_date"]:
            break
    
    return tariff_data

def scrape_whitehouse_tariff_actions(start_url=BASE_URL, max_pages=MAX_PAGES):
    """
    Scrape White House presidential actions related to tariffs and trade.
    
    Args:
        start_url: URL to start scraping from
        max_pages: Maximum number of pages to scrape
        
    Returns:
        List of dictionaries containing tariff-related actions
    """
    session = create_session()
    current_url = start_url
    all_posts = []
    tariff_posts = []
    page_count = 0

    while current_url and page_count < max_pages:
        logging.info(f"Processing page {page_count + 1}: {current_url}")
        page_content = fetch_page(current_url, session)
        if not page_content:
            logging.error(f"Failed to fetch content from {current_url}. Stopping pagination.")
            break

        posts = parse_post_list(page_content, current_url)
        for post in posts:
            if post["url"]:
                logging.debug(f"Scraping full text for post: {post['title']} from {post['url']}")
                post["full_text"] = scrape_full_text(post["url"], session)
            else:
                post["full_text"] = ""
                
            all_posts.append(post)
            
            # Check if post is tariff-related
            if is_tariff_related(post):
                tariff_data = extract_tariff_data(post)
                tariff_posts.append(tariff_data)
            
            time.sleep(REQUEST_DELAY)  # Pause between post requests

        next_page = find_next_page(page_content, current_url)
        current_url = next_page
        page_count += 1
        time.sleep(REQUEST_DELAY)  # Pause between page requests

    logging.info(f"Scraping complete. Total posts: {len(all_posts)}, Tariff-related posts: {len(tariff_posts)}")
    return tariff_posts

def store_to_excel(data, filename="whitehouse_tariff_data.xlsx"):
    """Store scraped data into an Excel file using pandas."""
    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    logging.info(f"Data successfully written to {filename}")

def store_to_json(data, output_dir="data"):
    """
    Store the tariff data in JSON format, including a latest version
    for the dashboard to access.
    
    Args:
        data: List of dictionaries containing tariff information
        output_dir: Directory to save the data
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save timestamped version
    timestamped_filename = os.path.join(output_dir, f"whitehouse_tariff_data_{timestamp}.json")
    with open(timestamped_filename, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "data": data
        }, f, indent=2)
    
    # Save latest version for dashboard
    latest_filename = os.path.join(output_dir, "whitehouse_data_latest.json")
    with open(latest_filename, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "data": data
        }, f, indent=2)
    
    logging.info(f"Data successfully written to {timestamped_filename} and {latest_filename}")
    
    return latest_filename

def run_whitehouse_scraper():
    """
    Main function to run the White House scraper and store the results.
    """
    logging.info("Starting White House tariff scraper...")
    tariff_posts = scrape_whitehouse_tariff_actions()
    
    # Store data in multiple formats
    store_to_excel(tariff_posts)
    latest_file = store_to_json(tariff_posts)
    
    logging.info(f"White House scraping completed. Found {len(tariff_posts)} tariff-related posts.")
    return latest_file

if __name__ == "__main__":
    run_whitehouse_scraper()
    print("Scraping complete.")