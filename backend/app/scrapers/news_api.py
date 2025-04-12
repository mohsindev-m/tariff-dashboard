#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Improved NewsAPI Scraper for Tariff Dashboard

This module provides an enhanced scraper for news articles related to tariffs
using the NewsAPI service, with specific focus on identifying and extracting
tariff and trade-related content with rich metadata.
"""

import os
import re
import json
import logging
import requests
import pandas as pd
from datetime import datetime
from itertools import product
from bs4 import BeautifulSoup
from textblob import TextBlob

# Configure logging
logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("newsapi_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("NewsAPI_Scraper")

def fetch_articles_for_query(api_key, query, language="en", sort_by="publishedAt", page_size=25, page=1):
    """
    Fetch articles using a specific query string.
    The query string should include both a primary keyword and a signal word.
    
    Args:
        api_key: NewsAPI API key
        query: Search query string
        language: Article language (default: English)
        sort_by: Sorting method (default: most recently published)
        page_size: Number of results per page (default: 25, max: 100)
        page: Page number (default: 1)
        
    Returns:
        List of article dictionaries
    """
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": language,
        "sortBy": sort_by,
        "pageSize": min(page_size, 100),  # Ensure within API limits
        "page": page,
        "apiKey": api_key
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            articles = data.get("articles", [])
            total_results = data.get("totalResults", 0)
            logger.info(f"Query '{query}' fetched {len(articles)} articles (total available: {total_results}).")
            return articles, total_results
        else:
            logger.error(f"Failed to fetch articles for query: {query} ({response.status_code}) - {response.text}")
            return [], 0
    except requests.RequestException as e:
        logger.error(f"Request error when fetching articles: {e}")
        return [], 0

def fetch_articles_by_combinations(api_key, primary_keywords, signal_words, max_articles_per_combo=10):
    """
    Loop over every combination of a primary keyword and a signal word,
    fetch articles that contain both (using a query like: '"primary" "signal"'),
    and merge the results while deduplicating by article URL.
    
    Args:
        api_key: NewsAPI API key
        primary_keywords: List of primary search terms (e.g., "tariff")
        signal_words: List of action words (e.g., "imposed")
        max_articles_per_combo: Maximum articles to fetch per combination
        
    Returns:
        List of unique article dictionaries
    """
    all_articles = {}
    for primary, signal in product(primary_keywords, signal_words):
        query = f'"{primary}" "{signal}"'
        articles, total = fetch_articles_for_query(
            api_key, 
            query, 
            page_size=max_articles_per_combo
        )
        
        for article in articles:
            url = article.get("url")
            if url and url not in all_articles:
                # Add a source identifier field for tracking
                article["query_source"] = query
                all_articles[url] = article
    
    combined_articles = list(all_articles.values())
    logger.info(f"Combined total after deduplication: {len(combined_articles)} articles.")
    return combined_articles

def clean_html_content(html_content):
    """
    Clean HTML content to extract readable text.
    
    Args:
        html_content: HTML string to clean
        
    Returns:
        Cleaned text without HTML tags
    """
    if not html_content:
        return ""
    
    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script, style elements and comments
        for element in soup(['script', 'style']):
            element.decompose()
        
        # Get text and clean whitespace
        text = soup.get_text(separator=' ', strip=True)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    except Exception as e:
        logger.warning(f"Error cleaning HTML content: {e}")
        return html_content

def analyze_sentiment(text):
    """
    Analyze the sentiment of a text and return a classification.
    
    Args:
        text: Text to analyze
        
    Returns:
        Sentiment classification ('positive', 'negative', or 'neutral')
        and score (-1.0 to 1.0)
    """
    try:
        # Use TextBlob for sentiment analysis
        blob = TextBlob(text)
        
        # Get polarity score (-1 to 1)
        polarity = blob.sentiment.polarity
        
        # Classify sentiment
        if polarity > 0.1:
            sentiment = "positive"
        elif polarity < -0.1:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        
        return {
            "classification": sentiment,
            "score": polarity
        }
    except Exception as e:
        logger.warning(f"Error during sentiment analysis: {e}")
        return {"classification": "neutral", "score": 0.0}

def categorize_tariff_articles(articles):
    """
    Categorize articles related to tariffs by country, industry, tariff type, and action.
    Detailed logging is added so you can trace step-by-step how fields are extracted.
    
    Args:
        articles: List of article dictionaries
        
    Returns:
        List of articles with additional categorization metadata
    """
    # Define keywords for countries.
    countries = {
        "United States": ["united states", "u.s.", "usa", "america"],
        "China": ["china", "chinese"],
        "European Union": ["european union", "eu", "europe"],
        "Canada": ["canada", "canadian"],
        "Mexico": ["mexico", "mexican"],
        "Japan": ["japan", "japanese"],
        "South Korea": ["south korea", "korean"],
        "United Kingdom": ["uk", "britain", "british", "united kingdom"],
        "Brazil": ["brazil", "brazilian"],
        "India": ["india", "indian"],
        "Australia": ["australia", "australian"],
        "Vietnam": ["vietnam", "vietnamese"],
        "Taiwan": ["taiwan", "taiwanese"],
        "Russia": ["russia", "russian"],
        "Germany": ["germany", "german"]
    }
    
    # Define keywords for industries.
    industries = {
        "Steel": ["steel", "metal", "metallurgical"],
        "Aluminum": ["aluminum", "aluminium"],
        "Automotive": ["automotive", "car", "vehicle", "auto"],
        "Agriculture": ["agriculture", "farm", "crop", "food", "livestock"],
        "Technology": ["technology", "tech", "electronics"],
        "Energy": ["energy", "oil", "gas", "solar", "renewable"],
        "Textiles": ["textile", "clothing", "apparel", "fabric"],
        "Pharmaceuticals": ["pharmaceutical", "drug", "medicine"],
        "Chemicals": ["chemical", "petrochemical"],
        "Semiconductor": ["semiconductor", "chip", "microchip"]
    }
    
    # Define keywords for tariff types.
    tariff_types = {
        "Reciprocal": ["reciprocal", "reciprocity"],
        "Retaliatory": ["retaliatory", "retaliation", "retaliate"],
        "Protective": ["protective", "protection", "safeguard"],
        "Anti-dumping": ["anti-dumping", "dumping"],
        "Countervailing": ["countervailing", "subsidy", "subsidies"],
        "De Minimis": ["de minimis", "minimum threshold", "duty free"],
        "Section 301": ["section 301", "301 tariff"],
        "Section 232": ["section 232", "232 tariff"]
    }
    
    # Define keywords for actions.
    actions = {
        "Implementation": ["implemented", "imposed", "introduced", "announced", "enacted"],
        "Increase": ["increased", "raised", "hiked"],
        "Removal": ["removed", "eliminated", "dropped", "lifted"],
        "Reduction": ["reduced", "lowered", "cut", "decreased"],
        "Exemption": ["exempted", "exemption", "waived", "waiver"],
        "Response": ["responded", "retaliated", "counter"]
    }
    
    categorized = []
    for article in articles:
        # Ensure that title, description, and content are strings (fallback to empty string)
        title = str(article.get("title") or "")
        description = str(article.get("description") or "")
        content = str(article.get("content") or "")
        
        # Clean HTML from content if present
        content = clean_html_content(content)
        
        # Combine text for analysis
        full_text = " ".join([title, description, content]).lower()
        
        logger.debug(f"Processing article: {title[:60]}...")
        
        # Extract countries
        article_countries = []
        for country, keys in countries.items():
            for keyword in keys:
                if re.search(r'\b' + re.escape(keyword) + r'\b', full_text):
                    if country not in article_countries:
                        article_countries.append(country)
                        logger.debug(f"Found country '{country}' for article.")
                    break
        
        # Extract industries
        article_industries = []
        for industry, keys in industries.items():
            for keyword in keys:
                if re.search(r'\b' + re.escape(keyword) + r'\b', full_text):
                    if industry not in article_industries:
                        article_industries.append(industry)
                        logger.debug(f"Found industry '{industry}' for article.")
                    break
        
        # Extract tariff types
        article_tariff_types = []
        for t_type, keys in tariff_types.items():
            for keyword in keys:
                if re.search(r'\b' + re.escape(keyword) + r'\b', full_text):
                    if t_type not in article_tariff_types:
                        article_tariff_types.append(t_type)
                        logger.debug(f"Found tariff type '{t_type}' for article.")
                    break
        
        # Extract actions
        article_actions = []
        for action, keys in actions.items():
            for keyword in keys:
                if re.search(r'\b' + re.escape(keyword) + r'\b', full_text):
                    if action not in article_actions:
                        article_actions.append(action)
                        logger.debug(f"Found action '{action}' for article.")
                    break
        
        # Extract tariff rates using regex patterns
        rate_patterns = [
            r'(\d+(?:\.\d+)?)\s*percent(?:\s+tariff|\s+duty)?',
            r'(\d+(?:\.\d+)?)\s*%(?:\s+tariff|\s+duty)?',
            r'tariff\s*of\s*(\d+(?:\.\d+)?)\s*%',
            r'(\d+(?:\.\d+)?)\s*%\s*duty'
        ]
        tariff_rates = []
        for pattern in rate_patterns:
            found_rates = re.findall(pattern, full_text)
            if found_rates:
                logger.debug(f"Pattern '{pattern}' found rates: {found_rates}")
            tariff_rates.extend(found_rates)
        
        # Extract implementation dates using regex
        date_patterns = [
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4}'
        ]
        implementation_dates = []
        for pattern in date_patterns:
            found_dates = re.findall(pattern, full_text)
            if found_dates:
                logger.debug(f"Pattern '{pattern}' found dates: {found_dates}")
            implementation_dates.extend(found_dates)
        
        # Analyze sentiment
        sentiment_data = analyze_sentiment(title + " " + description)
        
        # Format the data for output
        categorized_article = {
            "source": article.get("source", {}).get("name", "Unknown Source"),
            "title": title,
            "description": description,
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "countries": article_countries,
            "industries": article_industries,
            "tariff_types": article_tariff_types,
            "actions": article_actions,
            "tariff_rates": tariff_rates,
            "implementation_dates": implementation_dates,
            "sentiment": sentiment_data
        }
        
        categorized.append(categorized_article)
    
    logger.info(f"Categorized {len(categorized)} articles with tariff information.")
    return categorized

def save_articles_to_json(data, output_dir="data"):
    """
    Saves the categorized articles to a JSON file, including a latest version
    for the dashboard to access.
    
    Args:
        data: List of categorized article dictionaries
        output_dir: Directory to save the data files
        
    Returns:
        Path to the latest data file
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Prepare data with timestamp
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "data": data
    }
    
    # Save timestamped version
    timestamped_filename = os.path.join(output_dir, f"news_data_{timestamp}.json")
    with open(timestamped_filename, 'w', encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    
    # Save latest version for dashboard
    latest_filename = os.path.join(output_dir, "news_data_latest.json")
    with open(latest_filename, 'w', encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    
    logger.info(f"Articles successfully saved to {timestamped_filename} and {latest_filename}")
    return latest_filename

def run_news_scraper(max_articles_per_combo=10):
    """
    Main function to run the NewsAPI scraper and save the results.
    
    Args:
        max_articles_per_combo: Maximum number of articles to fetch per keyword combination
        
    Returns:
        Path to the latest data file
    """
    # Get API key from environment variable
    API_KEY = os.getenv("NEWSAPI_KEY")
    if not API_KEY:
        logger.error("No NewsAPI key provided. Set NEWSAPI_KEY environment variable.")
        return None
    
    # Define search keywords
    primary_keywords = [
        "tariff",
        "import duty",
        "trade war",
        "trade deficit",
        "customs duty",
        "trade barrier",
        "de minimis"
    ]
    
    signal_words = [
        "imposed", "announced", "implemented", "removed", 
        "increased", "decreased", "retaliated", "responded", 
        "exempted", "eliminated"
    ]
    
    logger.info("Starting NewsAPI tariff article scraper...")
    
    # Fetch articles
    articles = fetch_articles_by_combinations(
        API_KEY, 
        primary_keywords, 
        signal_words,
        max_articles_per_combo
    )
    
    # Categorize articles
    categorized_articles = categorize_tariff_articles(articles)
    
    # Save articles
    latest_file = save_articles_to_json(categorized_articles)
    
    logger.info(f"NewsAPI scraping completed. Found {len(categorized_articles)} tariff-related articles.")
    return latest_file

if __name__ == "__main__":
    run_news_scraper()
    print("News scraping complete.")