import re

def classify_tariff_content(text):
    if not text:
        return {
            "is_tariff_related": False,
            "affected_countries": [],
            "affected_industries": [],
            "tariff_types": [],
            "tariff_rates": [],
            "implementation_dates": []
        }
    
    # Initialize classification structure
    classification = {
        "is_tariff_related": False,
        "affected_countries": [],
        "affected_industries": [],
        "tariff_types": [],
        "tariff_rates": [],
        "implementation_dates": []
    }
    
    # Tariff-related keywords
    tariff_keywords = [
        "tariff", "import duty", "trade deficit", "section 301",
        "reciprocal", "customs duty", "trade war", "import tax"
    ]
    
    if any(keyword in text.lower() for keyword in tariff_keywords):
        classification["is_tariff_related"] = True
    else:
        return classification 
    
    countries = [
        "China", "Mexico", "Canada", "European Union", "EU", "Japan", 
        "South Korea", "Brazil", "India", "Vietnam", "Taiwan", 
        "Australia", "United Kingdom", "UK", "Germany", "France"
    ]
    
    # List of industries to check for
    industries = [
        "steel", "aluminum", "automotive", "agriculture", "technology",
        "semiconductor", "solar", "energy", "textile", "chemical",
        "pharmaceutical", "machinery", "electronics"
    ]
    
    # Tariff types
    tariff_types = [
        "reciprocal", "retaliatory", "protective", "punitive",
        "countervailing", "anti-dumping", "safeguard", "section 301",
        "section 232", "de minimis"
    ]
    
    # Check for countries
    for country in countries:
        if re.search(r'\b' + re.escape(country) + r'\b', text, re.IGNORECASE):
            classification["affected_countries"].append(country)
    
    # Check for industries
    for industry in industries:
        if re.search(r'\b' + re.escape(industry) + r'\b', text, re.IGNORECASE):
            classification["affected_industries"].append(industry)
    
    # Check for tariff types
    for tariff_type in tariff_types:
        if re.search(r'\b' + re.escape(tariff_type) + r'\b', text, re.IGNORECASE):
            classification["tariff_types"].append(tariff_type)
    
    # Extract tariff rates (look for percentage patterns)
    rate_patterns = [
        r'(\d+(?:\.\d+)?\s*percent\s*tariff)',
        r'(\d+(?:\.\d+)?%\s*tariff)',
        r'tariff\s*of\s*(\d+(?:\.\d+)?%)',
        r'duty\s*of\s*(\d+(?:\.\d+)?%)'
    ]
    
    for pattern in rate_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        classification["tariff_rates"].extend(matches)
    
    # Look for implementation dates
    date_patterns = [
        r'effective\s*on\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})',
        r'beginning\s*on\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})',
        r'starting\s*on\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})'
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        classification["implementation_dates"].extend(matches)
    
    return classification