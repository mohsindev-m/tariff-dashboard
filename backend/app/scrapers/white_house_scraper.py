import os
import json
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("WhiteHouseSeleniumScraper")

# Set debug flag to log structure details
DEBUG_STRUCTURE = True

# Updated URL for White House Statements & Releases
BASE_URL = "https://www.whitehouse.gov/briefing-room/statements-releases/"

def init_driver():
    """Initialize and return a headless Selenium Chrome driver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    # Add additional options if needed
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def scrape_press_releases(keyword="tariff", pages=1):
    """
    Use Selenium to scrape White House press releases that mention a given keyword.
    
    Parameters:
        keyword (str): Keyword to filter press releases (case-insensitive).
        pages (int): Number of pages to scrape.
        
    Returns:
        list of dict: List of press releases with title, URL, date, and summary.
    """
    driver = init_driver()
    press_releases = []

    for page in range(1, pages + 1):
        # Construct URL for the current page
        url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"
        logger.info(f"Loading URL: {url}")
        try:
            driver.get(url)
            # Wait for JavaScript to load content (adjust time as necessary)
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to load page {page}: {e}")
            continue
        
        # Get rendered page source
        html = driver.page_source
        if DEBUG_STRUCTURE:
            # Log the first 500 characters of the rendered HTML
            logger.debug(f"Rendered HTML snippet (page {page}): {html[:500]}")
        
        # Attempt to find press release items.
        # Inspect the rendered HTML (using your browser's dev tools) to determine the correct selector.
        # Common candidates might be <article>, <div class="listing-item">, etc.
        items = driver.find_elements(By.CSS_SELECTOR, "article")  # Adjust this selector as needed
        logger.info(f"Found {len(items)} article elements on page {page}.")
        
        for idx, item in enumerate(items, start=1):
            try:
                # Log raw HTML snippet for debugging (first 300 characters)
                if DEBUG_STRUCTURE:
                    logger.debug(f"Article {idx} HTML snippet: {item.get_attribute('outerHTML')[:300]}")
                
                # Extract title (try h2 or h3 inside the item)
                try:
                    title_elem = item.find_element(By.XPATH, ".//h2 | .//h3")
                    title = title_elem.text.strip()
                except Exception:
                    title = "No Title"
                
                # Extract URL from the title link if available
                try:
                    link_elem = title_elem.find_element(By.TAG_NAME, "a")
                    url_link = link_elem.get_attribute("href")
                except Exception:
                    url_link = ""
                
                # Extract publication date from a <time> element
                try:
                    time_elem = item.find_element(By.TAG_NAME, "time")
                    pub_date = time_elem.get_attribute("datetime").strip()
                except Exception:
                    pub_date = ""
                
                # Extract summary text from a <p> or div with summary-like class
                try:
                    summary_elem = item.find_element(By.XPATH, ".//p")
                    summary = summary_elem.text.strip()
                except Exception:
                    summary = ""
                
                # Check if the keyword is in the title or summary
                if keyword.lower() not in title.lower() and keyword.lower() not in summary.lower():
                    continue
                
                press_releases.append({
                    "title": title,
                    "url": url_link,
                    "date": pub_date,
                    "summary": summary
                })
            except Exception as e:
                logger.error(f"Error processing article {idx} on page {page}: {e}")
    
    driver.quit()
    return press_releases

def save_press_releases(data, filename="whitehouse_press_releases.json"):
    """
    Save the scraped press releases to a timestamped JSON file.
    
    Parameters:
        data (list): List of press release dictionaries.
        filename (str): Base filename.
        
    Returns:
        str: Filepath of the saved file.
    """
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join("data", f"{timestamp}_{filename}")
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logger.info(f"Press releases saved to {filepath}")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
    return filepath

def main():
    releases = scrape_press_releases(keyword="tariff", pages=2)
    logger.info(f"Scraped {len(releases)} press releases containing the keyword 'tariff'.")
    if releases:
        save_press_releases(releases)

if __name__ == "__main__":
    main()
