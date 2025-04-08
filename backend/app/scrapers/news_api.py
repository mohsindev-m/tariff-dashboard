import os
import re
import json
import logging
import requests
from itertools import product

logging.basicConfig(
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("NewsAPI_Scraper")

def fetch_articles_for_query(api_key, query, language="en", sort_by="publishedAt"):
    """
    Fetch articles using a specific query string.
    The query string should include both a primary keyword and a signal word.
    """
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": language,
        "sortBy": sort_by,
        "apiKey": api_key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        articles = response.json().get("articles", [])
        logger.info(f"Query '{query}' fetched {len(articles)} articles.")
        return articles
    else:
        logger.error(f"Failed to fetch articles for query: {query} ({response.status_code}) - {response.text}")
        return []

def fetch_articles_by_combinations(api_key, primary_keywords, signal_words):
    """
    Loop over every combination of a primary keyword and a signal word,
    fetch articles that contain both (using a query like: '"primary" "signal"'),
    and merge the results while deduplicating by article URL.
    """
    all_articles = {}
    for primary, signal in product(primary_keywords, signal_words):
        query = f'"{primary}" "{signal}"'
        articles = fetch_articles_for_query(api_key, query)
        for article in articles:
            url = article.get("url")
            if url and url not in all_articles:
                all_articles[url] = article
    combined_articles = list(all_articles.values())
    logger.info(f"Combined total after deduplication: {len(combined_articles)} articles.")
    return combined_articles

def categorize_tariff_articles(articles):
    """
    Categorize articles related to tariffs by country, industry, tariff type, and action.
    Detailed logging is added so you can trace step-by-step how fields are extracted.
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
        
        categorized.append({
            "article": article,
            "countries": article_countries,
            "industries": article_industries,
            "tariff_types": article_tariff_types,
            "actions": article_actions,
            "tariff_rates": tariff_rates,
            "implementation_dates": implementation_dates
        })
    
    logger.info(f"Categorized {len(categorized)} articles with tariff information.")
    return categorized

def save_articles_to_json(data, filename="data/processed/tariff_news_articles.json"):
    """
    Saves the categorized articles to a JSON file.
    Ensures the output directory exists.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Articles successfully saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving articles: {e}")

if __name__ == "__main__":
    API_KEY = os.getenv("NEWSAPI_KEY")
    
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
    
    articles = fetch_articles_by_combinations(API_KEY, primary_keywords, signal_words)
    categorized_articles = categorize_tariff_articles(articles)
    save_articles_to_json(categorized_articles)
