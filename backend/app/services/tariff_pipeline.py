import re
import os
import json
import sqlite3
import logging
from datetime import datetime

# Import scraper functions and configuration
from app.core.config import settings
from app.scrapers.census import get_tariff_dashboard_data
from app.scrapers.white_house_scraper import enterprise_scraper
from app.scrapers.wto_scraper import fetch_tariff_data, fetch_indicators
from app.scrapers.bea_scrapper import get_gdp_by_industry, get_international_transactions
from app.scrapers.news_api import fetch_articles_by_combinations, categorize_tariff_articles

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.FileHandler("tariff_pipeline.log"), logging.StreamHandler()]
)
logger = logging.getLogger("TariffPipeline")


class TariffDataPipeline:
    def __init__(self, data_dir=settings.DATA_DIR, db_path=settings.DB_PATH):
        self.data_dir = data_dir
        self.db_path = db_path
        self.news_api_key = settings.NEWSAPI_KEY
        self.census_api_key = settings.CENSUS_API_KEY
        self.bea_api_key = settings.BEA_API_KEY
        # WTO API key is available via settings.WTO_API_KEY if needed.

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._setup_database()

        # In-memory dictionaries to cache profiles and time series.
        self.tariff_measures = []
        self.country_profiles = {}
        self.industry_profiles = {}
        self.time_series_data = {}

        logger.info("Tariff Data Pipeline initialized.")

    def _setup_database(self):
        """Set up (or recreate) the SQLite database with a clean schema."""
        try:
            # Force remove the existing database for a clean start.
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.info(f"Existing database {self.db_path} removed.")

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Create tables
            cursor.execute('''
                CREATE TABLE tariff_measures (
                    id TEXT PRIMARY KEY,
                    source_type TEXT,
                    source_url TEXT,
                    title TEXT,
                    publication_date TEXT,
                    implementation_date TEXT,
                    expiration_date TEXT,
                    tariff_type TEXT,
                    affected_countries TEXT,
                    affected_industries TEXT,
                    tariff_rates TEXT,
                    full_text TEXT,
                    extracted_highlights TEXT,
                    status TEXT,
                    last_updated TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE country_profiles (
                    country_code TEXT PRIMARY KEY,
                    country_name TEXT,
                    region TEXT,
                    latest_trade_deficit REAL,
                    trade_deficit_trend TEXT,
                    total_exports REAL,
                    total_imports REAL,
                    tariff_measures TEXT,
                    affected_industries TEXT,
                    supply_chain_risk REAL,
                    tariff_impact REAL,
                    jobs_impact REAL,
                    last_updated TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE industry_profiles (
                    industry_code TEXT PRIMARY KEY,
                    industry_name TEXT,
                    sector TEXT,
                    countries_affected TEXT,
                    initial_tariff REAL,
                    effective_tariff REAL,
                    trade_volume REAL,
                    gva_impact REAL,
                    jobs_impact REAL,
                    last_updated TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE economic_time_series (
                    id TEXT PRIMARY KEY,
                    metric TEXT,
                    country_code TEXT,
                    industry_code TEXT,
                    frequency TEXT,
                    time_points TEXT,
                    values_data TEXT,
                    source TEXT,
                    last_updated TEXT
                )
            ''')
            conn.commit()
            conn.close()
            logger.info("Database setup complete.")
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    # -----------------------------
    # Data Collection Methods
    # -----------------------------

    def collect_whitehouse_data(self, max_pages=10):
        logger.info("Collecting White House data.")
        base_url = "https://www.whitehouse.gov/presidential-actions/"
        posts = enterprise_scraper(base_url, max_pages)
        tariff_posts = []
        for post in posts:
            title = str(post.get("title", "")).lower()
            full_text = str(post.get("full_text", "")).lower()
            tariff_keywords = ["tariff", "trade", "import duty", "export", "customs", "section 301", "section 232"]
            if any(keyword in title or keyword in full_text for keyword in tariff_keywords):
                tariff_posts.append(post)
                logger.debug(f"White House post matches tariff criteria: {post.get('title')}")
        processed = []
        for post in tariff_posts:
            measure = self._process_whitehouse_post(post)
            if measure:
                processed.append(measure)
                self._save_to_db("tariff_measures", measure)
        logger.info(f"Collected and processed {len(processed)} White House tariff measures.")
        return processed

    def _process_whitehouse_post(self, post):
        try:
            title = str(post.get("title") or "")
            url = post.get("url") or ""
            pub_date = post.get("pub_date") or ""
            full_text = str(post.get("full_text") or "")
            unique_id = f"wh_{hash(url)}"

            # Extract tariff type
            tariff_type = "Unknown"
            tariff_patterns = {
                "Reciprocal": r'reciprocal\s+tariff|tariff\s+reciprocity',
                "Retaliatory": r'retaliat(?:ory|ing)\s+tariff',
                "Section 301": r'section\s+301',
                "Section 232": r'section\s+232',
                "Protective": r'protective\s+tariff|safeguard',
                "Anti-dumping": r'anti-dumping|dumping'
            }
            for ttype, pattern in tariff_patterns.items():
                if re.search(pattern, full_text, re.IGNORECASE):
                    tariff_type = ttype
                    logger.debug(f"White House post tariff type: {ttype}")
                    break

            # Extract affected countries
            country_keywords = {
                "United States": r'\b(united states|u\.s\.|usa|america)\b',
                "China": r'\b(china|chinese)\b',
                "European Union": r'\b(european union|eu|europe)\b',
                "Canada": r'\b(canada|canadian)\b',
                "Mexico": r'\b(mexico|mexican)\b',
                "Japan": r'\b(japan|japanese)\b'
            }
            affected_countries = []
            for country, pattern in country_keywords.items():
                if re.search(pattern, full_text, re.IGNORECASE):
                    affected_countries.append(country)
                    logger.debug(f"White House post affected country: {country}")

            # Extract affected industries
            industry_keywords = {
                "Automotive": r'\b(automotive|car|vehicle|auto)\b',
                "Agriculture": r'\b(agriculture|farm|crop|food)\b',
                "Technology": r'\b(technology|tech|electronics)\b'
            }
            affected_industries = []
            for industry, pattern in industry_keywords.items():
                if re.search(pattern, full_text, re.IGNORECASE):
                    affected_industries.append(industry)
                    logger.debug(f"White House post affected industry: {industry}")

            # Extract tariff rates
            rate_patterns = [r'(\d+(?:\.\d+)?)\s*percent', r'(\d+(?:\.\d+)?)\s*%']
            all_rates = []
            for pattern in rate_patterns:
                matches = re.findall(pattern, full_text, re.IGNORECASE)
                if matches:
                    logger.debug(f"Rates found with pattern '{pattern}': {matches}")
                all_rates.extend(matches)
            tariff_rates = {"rates": all_rates} if all_rates else {}

            # Extract implementation dates
            date_patterns = [
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4}'
            ]
            implementation_date = None
            for pattern in date_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    implementation_date = match.group(0)
                    logger.debug(f"White House post implementation date: {implementation_date}")
                    break

            # Extract highlights from text (first few sentences containing keywords)
            sentences = re.split(r'(?<=[.!?])\s+', full_text)
            highlights = [s.strip() for s in sentences if any(kw in s for kw in ["tariff", "percent", "duty", "import", "export", "trade"])][:5]

            measure = {
                "id": unique_id,
                "source_type": "whitehouse",
                "source_url": url,
                "title": title,
                "publication_date": pub_date,
                "implementation_date": implementation_date,
                "expiration_date": None,
                "tariff_type": tariff_type,
                "affected_countries": json.dumps(affected_countries),
                "affected_industries": json.dumps(affected_industries),
                "tariff_rates": json.dumps(tariff_rates),
                "full_text": full_text,
                "extracted_highlights": json.dumps(highlights),
                "status": "active",
                "last_updated": datetime.now().isoformat()
            }
            logger.debug(f"Processed White House measure with id: {unique_id}")
            return measure
        except Exception as e:
            logger.error(f"Error processing White House post: {e}")
            return None

    def collect_news_data(self):
        """Collect and process tariff-related news data using the News API."""
        logger.info("Collecting tariff-related news data from News API.")
        primary_keywords = ["tariff", "import duty", "trade war", "trade deficit", "customs duty", "trade barrier", "de minimis"]
        signal_words = ["imposed", "announced", "implemented", "removed", "increased", "decreased", "retaliated", "responded", "exempted", "eliminated"]

        articles = fetch_articles_by_combinations(self.news_api_key, primary_keywords, signal_words)
        categorized_articles = categorize_tariff_articles(articles)
        processed = []
        for art in categorized_articles:
            measure = self._process_news_article(art)
            if measure:
                processed.append(measure)
                self._save_to_db("tariff_measures", measure)
        logger.info(f"Collected and processed {len(processed)} News API tariff measures.")
        return processed

    def _process_news_article(self, article_data):
        try:
            article = article_data.get("article", {})
            title = str(article.get("title") or "")
            url = article.get("url") or ""
            published_at = article.get("publishedAt") or ""
            description = str(article.get("description") or "")
            content = str(article.get("content") or "")
            full_text = f"{title}. {description} {content}"
            unique_id = f"news_{hash(url)}"
            countries = article_data.get("countries", [])
            industries = article_data.get("industries", [])
            tariff_types = article_data.get("tariff_types", [])
            actions = article_data.get("actions", [])
            tariff_rates = article_data.get("tariff_rates", [])
            implementation_dates = article_data.get("implementation_dates", [])
            tariff_type = tariff_types[0] if tariff_types else "Unknown"
            status = "active" if any(a in ["Implementation", "Increase"] for a in actions) else "inactive"
            highlights = []
            if description:
                highlights.append(description)
            if content:
                sentences = re.split(r'(?<=[.!?])\s+', content)
                for sentence in sentences:
                    if any(kw in sentence.lower() for kw in ["tariff", "percent", "duty", "import", "export", "trade"]):
                        highlights.append(sentence.strip())
                        if len(highlights) >= 3:
                            break

            measure = {
                "id": unique_id,
                "source_type": "news",
                "source_url": url,
                "title": title,
                "publication_date": published_at,
                "implementation_date": implementation_dates[0] if implementation_dates else None,
                "expiration_date": None,
                "tariff_type": tariff_type,
                "affected_countries": json.dumps(countries),
                "affected_industries": json.dumps(industries),
                "tariff_rates": json.dumps({"rates": tariff_rates}),
                "full_text": full_text,
                "extracted_highlights": json.dumps(highlights),
                "status": status,
                "last_updated": datetime.now().isoformat()
            }
            logger.debug(f"Processed news measure with id: {unique_id}")
            return measure
        except Exception as e:
            logger.error(f"Error processing news article: {e}")
            return None

    def _save_to_db(self, table_name, data):
        """Save or update the provided record in the specified database table."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            values = tuple(data.values())
            sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, values)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error saving data to {table_name}: {e}")

    def collect_census_data(self):
        """Collect and process Census Bureau trade data."""
        logger.info("Collecting Census Bureau trade data.")
        try:
            current_year = datetime.now().year
            current_month = datetime.now().month
            if current_year >= 2025:
                year_str = "2024"
                month_str = "12"
            else:
                year_str = str(current_year)
                month_str = f"{max(1, current_month - 1):02d}"
            logger.info(f"Using Census data for {year_str}-{month_str}")
            dashboard_data = get_tariff_dashboard_data(year_str, month_str)
            if not dashboard_data:
                logger.error("Census API returned no data.")
                dashboard_data = {}
            if 'trade_balance' in dashboard_data:
                self._process_trade_balance_data(dashboard_data['trade_balance'])
            if 'sector_data' in dashboard_data:
                self._process_sector_data(dashboard_data['sector_data'])
            if 'time_series' in dashboard_data:
                self._process_time_series_data(dashboard_data['time_series'])
            if 'hs_data' in dashboard_data:
                self._process_hs_data(dashboard_data['hs_data'])
            logger.info("Census data collection complete.")
            return dashboard_data
        except Exception as e:
            logger.error(f"Error collecting Census data: {e}")
            return {}

    def _process_trade_balance_data(self, data):
        logger.info(f"Processing trade balance data for {len(data)} districts.")
        for item in data:
            district = item.get("DISTRICT")
            if not district:
                continue
            country_name = item.get("DIST_NAME", f"District {district}")
            country_code = f"CTY_{district}"
            profile = {
                "country_code": country_code,
                "country_name": country_name,
                "region": "Unknown",
                "latest_trade_deficit": item.get("trade_balance", 0),
                "trade_deficit_trend": json.dumps([item.get("trade_balance", 0)]),
                "total_exports": item.get("exports_value", 0),
                "total_imports": item.get("imports_value", 0),
                "tariff_measures": json.dumps([]),
                "affected_industries": json.dumps([]),
                "supply_chain_risk": 0.0,
                "tariff_impact": 0.0,
                "jobs_impact": 0.0,
                "last_updated": datetime.now().isoformat()
            }
            self._save_to_db("country_profiles", profile)
            self.country_profiles[country_code] = profile

    def _process_sector_data(self, data):
        logger.info(f"Processing sector data for {len(data)} sectors.")
        for item in data:
            sector = item.get("SECTOR")
            if not sector:
                continue
            industry_code = f"IND_{sector.replace(' ', '_').upper()}"
            profile = {
                "industry_code": industry_code,
                "industry_name": sector,
                "sector": sector,
                "countries_affected": json.dumps([]),
                "initial_tariff": 0.0,
                "effective_tariff": 0.0,
                "trade_volume": item.get("ALL_VAL_MO") or item.get("GEN_VAL_MO", 0),
                "gva_impact": 0.0,
                "jobs_impact": 0.0,
                "last_updated": datetime.now().isoformat()
            }
            self._save_to_db("industry_profiles", profile)
            self.industry_profiles[industry_code] = profile

    def _process_time_series_data(self, time_series):
        logger.info(f"Processing time series data with {len(time_series)} records.")
        years, deficits, exports, imports = [], [], [], []
        for item in time_series:
            years.append(item.get("YEAR"))
            deficits.append(item.get("DEFICIT_BILLIONS", 0))
            exports.append(item.get("EXPORTS_BILLIONS", 0))
            imports.append(item.get("IMPORTS_BILLIONS", 0))
        metrics = [("trade_deficit", deficits), ("exports", exports), ("imports", imports)]
        for metric, values in metrics:
            record = {
                "id": f"TS_{metric}",
                "metric": metric,
                "country_code": "USA",
                "industry_code": None,
                "frequency": "annual",
                "time_points": json.dumps(years),
                "values_data": json.dumps(values),
                "source": "Census Bureau",
                "last_updated": datetime.now().isoformat()
            }
            self._save_to_db("economic_time_series", record)
            self.time_series_data[f"TS_{metric}"] = record

    def _process_hs_data(self, hs_data):
        logger.info(f"Processing HS data for {len(hs_data)} chapters.")
        hs_to_sector = {
            "01": "Agriculture", "02": "Agriculture", "03": "Agriculture", "04": "Agriculture",
            "72": "Steel", "73": "Steel", "76": "Aluminum",
            "28": "Chemicals", "29": "Chemicals", "30": "Pharmaceuticals",
            "84": "Technology", "85": "Technology", "90": "Technology",
            "87": "Automotive",
            "50": "Textiles", "51": "Textiles", "52": "Textiles", "53": "Textiles",
            "27": "Energy"
        }
        for item in hs_data:
            hs_chapter = item.get("HS_CHAPTER")
            if not hs_chapter:
                continue
            sector = hs_to_sector.get(hs_chapter, "Other")
            industry_code = f"HS_{hs_chapter}"
            description = item.get("DESCRIPTION") or f"HS Chapter {hs_chapter}"
            profile = {
                "industry_code": industry_code,
                "industry_name": description,
                "sector": sector,
                "countries_affected": json.dumps([]),
                "initial_tariff": 0.0,
                "effective_tariff": 0.0,
                "trade_volume": item.get("ALL_VAL_MO") or item.get("GEN_VAL_MO", 0),
                "gva_impact": 0.0,
                "jobs_impact": 0.0,
                "last_updated": datetime.now().isoformat()
            }
            self._save_to_db("industry_profiles", profile)
            self.industry_profiles[industry_code] = profile

    def collect_bea_data(self):
        logger.info("Collecting BEA economic data.")
        try:
            # Get GDP by industry data
            gdp_data = get_gdp_by_industry(
                table_id="1",
                frequency="A",
                year="2020,2021,2022,2023,2024",
                industry="ALL"
            )
            if gdp_data:
                self._process_gdp_data(gdp_data)
            # Get international transactions data
            ita_data = get_international_transactions(
                indicator="BalGds",
                area_or_country="AllCountries",
                frequency="A",
                year="2020,2021,2022,2023,2024"
            )
            if ita_data:
                self._process_ita_data(ita_data)
            logger.info("BEA data collection complete.")
            return True
        except Exception as e:
            logger.error(f"Error collecting BEA data: {e}")
            return False

    def _process_gdp_data(self, gdp_data):
        logger.info("Processing BEA GDP by industry data.")
        try:
            beaapi = gdp_data.get("BEAAPI", {})
            results = beaapi.get("Results")
            data_rows = None
            if isinstance(results, list):
                if not results or "Data" not in results[0]:
                    logger.error("BEA GDP response missing 'Data' in Results list:\n%s", json.dumps(gdp_data, indent=2))
                    raise Exception("Malformed BEA GDP data")
                data_rows = results[0]["Data"]
            elif isinstance(results, dict) and "Data" in results:
                data_rows = results["Data"]
            else:
                logger.error("BEA GDP response does not have the expected structure:\n%s", json.dumps(gdp_data, indent=2))
                raise Exception("Malformed BEA GDP data")
            industry_gdp = {}
            for row in data_rows:
                industry_code = row.get('Industry')
                if not industry_code:
                    continue
                try:
                    value = float(row.get('DataValue'))
                    industry_gdp[industry_code] = value
                except (ValueError, TypeError):
                    logger.debug(f"Skipping row with non-numeric value: {row.get('DataValue')}")
                    continue
            for code, value in industry_gdp.items():
                std_code = f"BEA_{code}"
                if std_code in self.industry_profiles:
                    self.industry_profiles[std_code]["gdp_value"] = value
                    self._save_to_db("industry_profiles", self.industry_profiles[std_code])
                else:
                    profile = {
                        "industry_code": std_code,
                        "industry_name": f"Industry {code}",
                        "sector": "Unknown",
                        "countries_affected": json.dumps([]),
                        "initial_tariff": 0.0,
                        "effective_tariff": 0.0,
                        "trade_volume": 0.0,
                        "gva_impact": 0.0,
                        "jobs_impact": 0.0,
                        "gdp_value": value,
                        "last_updated": datetime.now().isoformat()
                    }
                    self._save_to_db("industry_profiles", profile)
                    self.industry_profiles[std_code] = profile
            logger.info(f"Processed GDP data for {len(industry_gdp)} industries.")
        except Exception as e:
            logger.error(f"Error processing GDP data: {e}")
            raise

    def _process_ita_data(self, ita_data):
        logger.info("Processing BEA international transactions data.")
        try:
            beaapi = ita_data.get("BEAAPI", {})
            results = beaapi.get("Results")
            data_rows = None
            if isinstance(results, list):
                if not results or "Data" not in results[0]:
                    logger.error("BEA ITA response missing 'Data' in Results list:\n%s", json.dumps(ita_data, indent=2))
                    return
                data_rows = results[0]["Data"]
            elif isinstance(results, dict) and "Data" in results:
                data_rows = results["Data"]
            else:
                logger.error("BEA ITA response does not have expected structure:\n%s", json.dumps(ita_data, indent=2))
                return
            country_balances = {}
            for row in data_rows:
                country = row.get('AreaOrCountry')
                if not country or country == "AllCountries":
                    continue
                try:
                    value = float(row.get('DataValue'))
                except (ValueError, TypeError):
                    continue
                country_balances[country] = value
            for country, balance in country_balances.items():
                std_country = f"BEA_{country}"
                if std_country in self.country_profiles:
                    self.country_profiles[std_country]["latest_trade_deficit"] = balance
                    self._save_to_db("country_profiles", self.country_profiles[std_country])
                else:
                    profile = {
                        "country_code": std_country,
                        "country_name": country,
                        "region": "Unknown",
                        "latest_trade_deficit": balance,
                        "trade_deficit_trend": json.dumps([balance]),
                        "total_exports": 0.0,
                        "total_imports": 0.0,
                        "tariff_measures": json.dumps([]),
                        "affected_industries": json.dumps([]),
                        "supply_chain_risk": 0.0,
                        "tariff_impact": 0.0,
                        "jobs_impact": 0.0,
                        "last_updated": datetime.now().isoformat()
                    }
                    self._save_to_db("country_profiles", profile)
                    self.country_profiles[std_country] = profile
            logger.info(f"Processed ITA data for {len(country_balances)} countries.")
        except Exception as e:
            logger.error(f"Error processing ITA data: {e}")

    def collect_wto_data(self):
        logger.info("Collecting WTO tariff data.")
        try:
            tariff_indicators = fetch_indicators(name="tariff")
            if not tariff_indicators or len(tariff_indicators) == 0:
                logger.error("No tariff indicators found from WTO API.")
                return False
            indicator = tariff_indicators[0]
            indicator_code = indicator.get("code")
            logger.info(f"Using WTO indicator: {indicator_code}")
            tariff_data = fetch_tariff_data(
                indicator_code,
                reporting_economy="all",
                time_period="default",
                max_records=500
            )
            if tariff_data:
                self._process_wto_tariff_data(tariff_data)
            else:
                logger.error("Failed to fetch WTO tariff data.")
            logger.info("WTO data collection complete.")
            return True
        except Exception as e:
            logger.error(f"Error collecting WTO data: {e}")
            return False

    def _process_wto_tariff_data(self, tariff_data):
        logger.info("Processing WTO tariff data.")
        try:
            data_rows = []
            if "Dataset" in tariff_data:
                data_rows = tariff_data["Dataset"]
            elif "data" in tariff_data:
                data_rows = tariff_data["data"]
            if not data_rows:
                logger.warning("No data rows found in WTO tariff response.")
                return
            country_tariffs = {}
            for row in data_rows:
                country = row.get("ReportingEconomy")
                if not country:
                    continue
                try:
                    value = float(row.get("Value"))
                except (ValueError, TypeError):
                    continue
                country_tariffs.setdefault(country, []).append(value)
            processed_count = 0
            for country, rates in country_tariffs.items():
                if not rates:
                    continue
                avg_rate = sum(rates) / len(rates)
                std_country = f"WTO_{country}"
                try:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM country_profiles WHERE country_code = ?", (std_country,))
                    exists = cursor.fetchone()[0] > 0
                    if exists:
                        cursor.execute("""
                            UPDATE country_profiles 
                            SET initial_tariff = ?, last_updated = ?
                            WHERE country_code = ?
                        """, (avg_rate, datetime.now().isoformat(), std_country))
                    else:
                        cursor.execute("""
                            INSERT INTO country_profiles 
                            (country_code, country_name, region, initial_tariff, effective_tariff,
                             supply_chain_risk, tariff_impact, jobs_impact, latest_trade_deficit,
                             trade_deficit_trend, total_exports, total_imports, tariff_measures,
                             affected_industries, last_updated)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            std_country, country, "Unknown",
                            avg_rate, 0.0, 0.0, 0.0, 0.0,
                            0.0, "[]", 0.0, 0.0, "[]", "[]", datetime.now().isoformat()
                        ))
                    conn.commit()
                    conn.close()
                    processed_count += 1
                except Exception as e:
                    logger.error(f"Error saving WTO data for {country}: {e}")
                    if conn:
                        conn.close()
            logger.info(f"Processed WTO tariff data for {processed_count} countries.")
        except Exception as e:
            logger.error(f"Error in _process_wto_tariff_data: {e}")

    # -----------------------------
    # Impact Calculations
    # -----------------------------
    def calculate_impact_metrics(self):
        logger.info("Calculating impact metrics.")
        self._calculate_supply_chain_risk()
        self._calculate_tariff_impact()
        self._calculate_jobs_impact()
        logger.info("Impact metrics calculation complete.")

    def _calculate_supply_chain_risk(self):
        logger.info("Calculating supply chain risk index.")
        for country_code, profile in self.country_profiles.items():
            try:
                exports = float(profile.get("total_exports", 0))
                imports = float(profile.get("total_imports", 0))
                if imports == 0:
                    risk_index = 0
                else:
                    import_dependency = imports / (exports + imports + 1)
                    industry_count = len(json.loads(profile.get("affected_industries", "[]")))
                    industry_factor = min(industry_count / 10.0, 1.0) if industry_count else 0.1
                    risk_index = min(import_dependency * (2 - industry_factor) * 100, 100)
                profile["supply_chain_risk"] = risk_index
                self._save_to_db("country_profiles", profile)
            except Exception as e:
                logger.error(f"Error calculating supply chain risk for {country_code}: {e}")

    def _calculate_tariff_impact(self):
        logger.info("Calculating tariff impact on GDP.")
        for country_code, profile in self.country_profiles.items():
            try:
                initial = float(profile.get("initial_tariff", 0))
                # Apply an economic model (here a 5% multiplier)
                impact = initial * 0.05
                profile["tariff_impact"] = impact
                self._save_to_db("country_profiles", profile)
            except Exception as e:
                logger.error(f"Error calculating tariff impact for {country_code}: {e}")

    def _calculate_jobs_impact(self):
        logger.info("Calculating jobs impact.")
        for industry_code, profile in self.industry_profiles.items():
            try:
                gva = float(profile.get("gva_impact", 0))
                volume = float(profile.get("trade_volume", 0))
                if volume == 0:
                    continue
                # Simplified model: assume 1% GVA impact yields a 1.5% employment effect.
                employment_elasticity = 1.5
                assumed_employment = 1000000  # Replace with actual industry data when available.
                jobs_impact = assumed_employment * (gva * employment_elasticity) / 100
                profile["jobs_impact"] = jobs_impact
                self._save_to_db("industry_profiles", profile)
            except Exception as e:
                logger.error(f"Error calculating jobs impact for {industry_code}: {e}")
        # (Further aggregation to country level can be added here if needed.)

    # -----------------------------
    # Dashboard Data Preparation
    # -----------------------------
    def prepare_dashboard_data(self):
        logger.info("Preparing unified dashboard data.")
        dashboard_data = {
            "heatmap_data": self._prepare_heatmap_data(),
            "sector_data": self._prepare_sector_data(),
            "time_series": self._prepare_time_series_data(),
            "detail_table": self._prepare_detail_table_data(),
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "data_sources": ["White House", "News API", "Census", "BEA", "WTO"],
                "last_updated": datetime.now().isoformat()
            }
        }
        api_dir = os.path.join(self.data_dir, "api")
        os.makedirs(api_dir, exist_ok=True)
        output_path = os.path.join(api_dir, "dashboard_data.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(dashboard_data, f, indent=2)
        logger.info(f"Dashboard data saved to {output_path}")
        return dashboard_data

    def _prepare_heatmap_data(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT country_code, country_name, region, latest_trade_deficit,
                   total_exports, total_imports, supply_chain_risk, tariff_impact, jobs_impact
            FROM country_profiles
        """)
        rows = cursor.fetchall()
        conn.close()
        heatmap = []
        for row in rows:
            cc, name, region, deficit, exports, imports, risk, impact, jobs = row
            iso = cc.split('_')[-1] if '_' in cc else cc
            heatmap.append({
                "country_code": iso,
                "country_name": name,
                "region": region,
                "trade_deficit": deficit,
                "exports": exports,
                "imports": imports,
                "supply_chain_risk": risk,
                "tariff_impact": impact,
                "jobs_impact": jobs,
                "value": impact
            })
        return heatmap

    def _prepare_sector_data(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sector, SUM(trade_volume) as total_volume, AVG(effective_tariff) as avg_tariff, SUM(jobs_impact) as total_jobs_impact
            FROM industry_profiles
            GROUP BY sector
            ORDER BY total_volume DESC
        """)
        rows = cursor.fetchall()
        conn.close()
        result = []
        for row in rows:
            sector, volume, tariff, jobs = row
            if not sector or sector == "Unknown":
                continue
            result.append({
                "sector": sector,
                "trade_volume": volume,
                "average_tariff": tariff,
                "jobs_impact": jobs,
                "percentage": 0  # Will be recalculated below
            })
        total_volume = sum(item["trade_volume"] for item in result) or 1
        for item in result:
            item["percentage"] = (item["trade_volume"] / total_volume) * 100
        return result

    def _prepare_time_series_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT metric, time_points, values_data FROM economic_time_series WHERE country_code = 'USA'")
            rows = cursor.fetchall()
            conn.close()
            result = {}
            for row in rows:
                metric, pts, vals = row["metric"], row["time_points"], row["values_data"]
                try:
                    years = json.loads(pts)
                    values = json.loads(vals)
                    series = [{"year": str(year), "value": value} for year, value in zip(years, values)]
                    result[metric] = series
                except Exception as e:
                    logger.error(f"Error processing time series for {metric}: {e}")
            formatted = []
            if "trade_deficit" in result:
                for point in result["trade_deficit"]:
                    year = point["year"]
                    data_point = {"year": year}
                    for metric, series in result.items():
                        for item in series:
                            if item["year"] == year:
                                data_point[metric] = item["value"]
                                break
                    formatted.append(data_point)
            if not formatted:
                logger.error("Time series data formatting failed.")
            return formatted
        except Exception as e:
            logger.error(f"Error preparing time series data: {e}")
            return []

    def _prepare_detail_table_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(country_profiles)")
            country_cols = [info[1] for info in cursor.fetchall()]
            cursor.execute("PRAGMA table_info(industry_profiles)")
            industry_cols = [info[1] for info in cursor.fetchall()]
            country_select = ", ".join([
                col if col in country_cols else f"0 as {col}" for col in
                ["country_code", "country_name", "initial_tariff", "effective_tariff", "tariff_impact", "jobs_impact", "supply_chain_risk"]
            ])
            industry_select = ", ".join([
                col if col in industry_cols else f"0 as {col}" for col in
                ["industry_code", "industry_name", "sector", "initial_tariff", "effective_tariff", "gva_impact", "jobs_impact"]
            ])
            cursor.execute(f"SELECT {country_select} FROM country_profiles ORDER BY effective_tariff DESC LIMIT 100")
            countries = [dict(row) for row in cursor.fetchall()]
            cursor.execute(f"SELECT {industry_select} FROM industry_profiles ORDER BY effective_tariff DESC LIMIT 100")
            industries = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return {"countries": countries, "industries": industries}
        except Exception as e:
            logger.error(f"Error preparing detail table data: {e}")
            conn.close()
            return {"countries": [], "industries": []}

    # -----------------------------
    # Full Pipeline Run and API Data Access
    # -----------------------------
    def run_full_pipeline(self):
        logger.info("Starting full tariff data pipeline run.")
        try:
            self.collect_whitehouse_data(max_pages=10)
            self.collect_news_data()
            self.collect_census_data()
            self.collect_bea_data()
            self.collect_wto_data()
            self.calculate_impact_metrics()
            dashboard = self.prepare_dashboard_data()
            logger.info("Pipeline run completed successfully.")
            return dashboard
        except Exception as e:
            logger.error(f"Error during pipeline run: {e}")
            return None

    def get_dashboard_api_data(self):
        try:
            api_path = os.path.join(self.data_dir, "api", "dashboard_data.json")
            if os.path.exists(api_path):
                with open(api_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.info("No cached dashboard data found; running pipeline.")
                return self.run_full_pipeline() or {}
        except Exception as e:
            logger.error(f"Error reading dashboard API data: {e}")
            return {}

def get_pipeline():
    """Factory function for FastAPI dependency injection."""
    return TariffDataPipeline()
