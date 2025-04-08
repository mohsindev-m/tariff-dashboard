# tariff_data_collector.py
import os
import json
from datetime import datetime

from app.scrapers.bea_scrapper import get_gdp_by_industry
from app.scrapers.white_house_scraper import enterprise_scraper
from app.services.tariff_classification import classify_tariff_content
from app.scrapers.wto_scraper import fetch_indicators, fetch_tariff_data
from app.scrapers.census import get_tariff_dashboard_data, get_latest_trade_year
from app.scrapers.news_api import fetch_articles_by_combinations, categorize_tariff_articles

def collect_white_house_data(max_pages=5):
    """Collect and classify tariff data from White House releases."""
    print("Collecting White House tariff data...")
    
    # Get White House press releases
    posts = enterprise_scraper("https://www.whitehouse.gov/presidential-actions/", max_pages)
    
    # Filter and classify tariff-related posts
    tariff_posts = []
    for post in posts:
        classification = classify_tariff_content(post.get("full_text", ""))
        if classification["is_tariff_related"]:
            tariff_posts.append({
                "source": "White House",
                "title": post.get("title", ""),
                "url": post.get("url", ""),
                "date": post.get("pub_date", ""),
                "content_preview": post.get("full_text", "")[:500] + "...",
                "classification": classification
            })
    
    print(f"Found {len(tariff_posts)} tariff-related White House posts")
    return tariff_posts

def collect_news_data():
    """Collect and process tariff news from News API."""
    print("Collecting news articles about tariffs...")
    
    # Get API key from environment variable
    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        print("Warning: NEWSAPI_KEY environment variable not set")
        return []
    
    # Tariff-specific keywords
    primary_keywords = [
        "tariff", "import duty", "trade war", "trade deficit",
        "customs duty", "trade barrier", "de minimis"
    ]
    
    # Action words related to tariffs
    signal_words = [
        "imposed", "announced", "implemented", "removed", 
        "increased", "decreased", "retaliated", "responded", 
        "exempted", "eliminated"
    ]
    
    # Collect and categorize news articles
    articles = fetch_articles_by_combinations(api_key, primary_keywords, signal_words)
    categorized = categorize_tariff_articles(articles)
    
    # Format the data for our dashboard
    news_data = []
    for item in categorized:
        article = item["article"]
        news_data.append({
            "source": article.get("source", {}).get("name", "News API"),
            "title": article.get("title", ""),
            "url": article.get("url", ""),
            "date": article.get("publishedAt", ""),
            "content_preview": article.get("description", "") or article.get("content", ""),
            "countries": item.get("countries", []),
            "industries": item.get("industries", []),
            "tariff_types": item.get("tariff_types", []),
            "actions": item.get("actions", []),
            "tariff_rates": item.get("tariff_rates", []),
            "implementation_dates": item.get("implementation_dates", [])
        })
    
    print(f"Collected {len(news_data)} tariff-related news articles")
    return news_data

def collect_trade_data():
    """Collect trade statistics from Census API."""
    print("Collecting trade statistics from Census API...")
    
    # Get the latest available year
    year = get_latest_trade_year()
    month = "06"  # June data as a mid-year snapshot
    
    # Get complete dashboard data from Census API
    trade_data = get_tariff_dashboard_data(year, month)
    
    if trade_data:
        print(f"Successfully collected trade data for {year}-{month}")
    else:
        print("Failed to collect trade data")
        trade_data = {}
    
    return trade_data

def collect_industry_data():
    """Collect GDP by industry data from BEA API."""
    print("Collecting industry data from BEA API...")
    
    # Get GDP by industry data
    gdp_data = get_gdp_by_industry(
        table_id="1",     # Value Added by Industry
        frequency="A",    # Annual
        year="LAST5",     # Last 5 years
        industry="ALL"    # All industries
    )
    
    if gdp_data:
        print("Successfully collected industry data")
        return gdp_data
    else:
        print("Failed to collect industry data")
        return {}

def collect_tariff_indicators():
    """Collect tariff indicators from WTO API."""
    print("Collecting tariff indicators from WTO API...")
    
    # Get tariff indicators
    indicators = fetch_indicators(name="tariff")
    
    if indicators and len(indicators) > 0:
        print(f"Found {len(indicators)} tariff indicators")
        
        # Get data for the first indicator
        indicator = indicators[0]
        indicator_code = indicator["code"]
        
        tariff_data = fetch_tariff_data(
            indicator_code,
            reporting_economy="all",
            time_period="2020"
        )
        
        if tariff_data:
            print("Successfully collected tariff data from WTO")
            return {
                "indicators": indicators,
                "data": tariff_data
            }
    
    print("Failed to collect tariff indicators")
    return {}

def compile_complete_dataset():
    """Compile the complete dataset from all sources."""
    print("Compiling complete tariff dashboard dataset...")
    
    # Collect data from all sources
    white_house_data = collect_white_house_data()
    news_data = collect_news_data()
    trade_data = collect_trade_data()
    industry_data = collect_industry_data()
    wto_data = collect_tariff_indicators()
    
    # Combine into a single dataset
    combined_data = {
        "news_and_announcements": {
            "white_house": white_house_data,
            "news_articles": news_data
        },
        "trade_statistics": trade_data,
        "industry_data": industry_data,
        "wto_tariff_data": wto_data,
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "sources": ["White House", "News API", "Census API", "BEA API", "WTO API"]
        }
    }
    
    # Save the compiled dataset
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"data/tariff_dashboard_data_{timestamp}.json"
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(combined_data, f, indent=2)
    
    print(f"Complete dataset saved to {filepath}")
    return combined_data

if __name__ == "__main__":
    compile_complete_dataset()