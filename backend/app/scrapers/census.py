import os
import time
import json
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Logger Setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Census_API_Client")

# -----------------------------------------------------------------------------
# Global Variables & Configuration
# -----------------------------------------------------------------------------
API_BASE_URL = "https://api.census.gov/data"
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
logger.info(f"Using Census API key: {CENSUS_API_KEY[:4]}...{CENSUS_API_KEY[-4:] if len(CENSUS_API_KEY) > 8 else ''}")

CACHE_DIR = "cache/census_api"
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
logger.info(f"Using cache directory: {CACHE_DIR}")

CACHE_EXPIRATION = 86400  # 24 hours

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_cache_filename(endpoint, params=None):
    if params is None:
        params = {}
    params_str = json.dumps({k: v for k, v in sorted(params.items()) if k != 'key'})
    endpoint_safe = endpoint.replace("/", "_").replace(".", "_")
    cache_key = f"{endpoint_safe}_{hash(params_str)}"
    return os.path.join(CACHE_DIR, f"{cache_key}.json")

def save_to_cache(data, endpoint, params=None):
    cache_file = get_cache_filename(endpoint, params)
    try:
        with open(cache_file, 'w') as f:
            json.dump({'timestamp': time.time(), 'data': data}, f)
        logger.debug(f"Saved data to cache: {cache_file}")
    except Exception as e:
        logger.warning(f"Failed to cache data: {e}")

def load_from_cache(endpoint, params=None):
    cache_file = get_cache_filename(endpoint, params)
    if not os.path.exists(cache_file):
        logger.debug(f"No cache file found: {cache_file}")
        return None
    try:
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
        if time.time() - cache_data['timestamp'] > CACHE_EXPIRATION:
            logger.debug(f"Cache expired: {cache_file}")
            return None
        logger.info(f"Loaded data from cache: {cache_file}")
        return cache_data['data']
    except Exception as e:
        logger.warning(f"Failed to load from cache: {e}")
        return None

def save_data_to_file(data, filename="census_data.csv"):
    try:
        os.makedirs("data", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join("data", f"{timestamp}_{filename}")
        
        if isinstance(data, pd.DataFrame):
            data.to_csv(filepath, index=False)
            logger.info(f"Saved DataFrame to file: {filepath}")
        else:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logger.info(f"Saved JSON data to file: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to save data to file: {e}")
        return None

def convert_to_dataframe(data):
    try:
        if not data or not isinstance(data, list) or len(data) < 2:
            logger.error("Invalid data format for conversion to DataFrame")
            return None
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        logger.debug(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        
        # Convert numeric columns
        for col in df.columns:
            try:
                # Skip columns that are likely to be string identifiers
                if col in ['DISTRICT', 'DIST_NAME', 'PORT', 'PORT_NAME', 'CTY_CODE', 'CTY_NAME', 'MONTH', 'YEAR', 'NAME']:
                    continue
                df[col] = pd.to_numeric(df[col])
                logger.debug(f"Converted column {col} to numeric type")
            except (ValueError, TypeError):
                logger.debug(f"Kept column {col} as original type (non-numeric)")
        return df
    except Exception as e:
        logger.error(f"Error converting to DataFrame: {e}")
        return None

def test_connection():
    logger.info("Testing connection to Census API...")
    url = "https://api.census.gov/data/2022/acs/acs1.json"
    logger.info(f"Testing connection with URL: {url}")
    try:
        response = requests.get(url, timeout=120)
        logger.info(f"Connection test response status code: {response.status_code}")
        if response.status_code == 200:
            logger.info("Connection successful to Census API")
            return True
        else:
            logger.error(f"Connection test failed with status code: {response.status_code}")
            logger.error(f"Response content: {response.text[:500]}...")
            return False
    except Exception as e:
        logger.error(f"Connection test failed with error: {e}")
        return False

def make_request(url, params=None, max_retries=3, timeout=120, use_cache=True):
    if params is None:
        params = {}
    if CENSUS_API_KEY and "key" not in params:
        params["key"] = CENSUS_API_KEY
    
    endpoint = url.replace(API_BASE_URL, "")
    if use_cache:
        cached_data = load_from_cache(endpoint, params)
        if cached_data is not None:
            return cached_data
    
    display_params = params.copy()
    if "key" in display_params:
        api_key = display_params["key"]
        if len(api_key) > 8:
            display_params["key"] = f"{api_key[:4]}...{api_key[-4:]}"
    logger.info(f"Making request to Census API: {url}")
    logger.info(f"Request parameters: {display_params}")
    
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(url, params=params, timeout=timeout)
            logger.info(f"Response status: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            if use_cache:
                save_to_cache(data, endpoint, params)
            return data
        except requests.exceptions.ChunkedEncodingError as chunk_err:
            attempts += 1
            logger.warning(f"Chunked encoding error on attempt {attempts}/{max_retries}: {chunk_err}")
            if attempts >= max_retries:
                logger.error(f"Failed after {max_retries} attempts: {chunk_err}")
                return None
            wait_time = 2 ** attempts
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            if hasattr(response, 'text'):
                logger.error(f"Response content: {response.text[:500]}...")
            return None
        except requests.exceptions.Timeout as timeout_err:
            attempts += 1
            logger.warning(f"Timeout error on attempt {attempts}/{max_retries}: {timeout_err}")
            if attempts >= max_retries:
                logger.error(f"Failed after {max_retries} attempts: {timeout_err}")
                return None
            wait_time = 2 ** attempts
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
        except Exception as e:
            attempts += 1
            logger.warning(f"Error on attempt {attempts}/{max_retries}: {e}")
            if attempts >= max_retries:
                logger.error(f"Failed after {max_retries} attempts: {e}")
                return None
            wait_time = 2 ** attempts
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

# -----------------------------------------------------------------------------
# Core API Function
# -----------------------------------------------------------------------------
def get_data(dataset, year, variables, geography=None, predicates=None, use_cache=True, include_year_in_path=True):
    """
    Retrieve data from the Census API.
    
    For datasets where the year is part of the URL (e.g., ACS), include_year_in_path=True.
    For time series datasets (e.g., International Trade), set include_year_in_path=False.
    
    Parameters:
        dataset (str): Dataset path (e.g., 'acs/acs1' or 'timeseries/intltrade/exports/enduse').
        year (str): Year to include in the URL path if applicable.
        variables (list or str): Variables to retrieve.
        geography (dict): Geography predicates (e.g., {"for": "state:*"}).
        predicates (dict): Additional predicates (e.g., {"YEAR": "2022", "MONTH": "06"}).
        use_cache (bool): Whether to use caching.
        include_year_in_path (bool): Whether to insert the year in the URL path.
        
    Returns:
        pandas.DataFrame: DataFrame with the retrieved data, or None if retrieval fails.
    """
    if isinstance(variables, list):
        variables = ",".join(variables)
    params = {"get": variables}
    if predicates:
        params.update(predicates)
    if geography:
        params.update(geography)
    
    if include_year_in_path and year:
        url = f"{API_BASE_URL}/{year}/{dataset}"
    else:
        url = f"{API_BASE_URL}/{dataset}"
    
    logger.info(f"Fetching data for dataset: {dataset} with URL: {url}")
    data = make_request(url, params, use_cache=use_cache)
    if data:
        df = convert_to_dataframe(data)
        if df is not None:
            logger.info(f"Successfully retrieved data for {dataset} (rows: {len(df)})")
        return df
    logger.error(f"Failed to retrieve data for {dataset}")
    return None

# -----------------------------------------------------------------------------
# Trade Data Functions
# -----------------------------------------------------------------------------
def get_import_export_data(year, dataset_type="exports", variables=None, predicates=None, month=None, use_cache=True):
    """
    Retrieve international trade export/import data from the Census API.
    
    Uses the "enduse" dataset for exports (and imports) which is a monthly dataset.
    
    Parameters:
        year (str): Year of the data (e.g., "2022").
        dataset_type (str): "exports" or "imports".
        variables (list or str): Variables to retrieve.
        predicates (dict): Additional predicates.
        month (str): Two-digit month (e.g., "06")—if provided, it will be added as a predicate.
        use_cache (bool): Whether to use caching.
    
    Returns:
        pandas.DataFrame: DataFrame with the retrieved data.
    """
    logger.info(f"Getting {dataset_type} data for year {year}")
    if not variables:
        if dataset_type == "exports":
            variables = "DISTRICT,DIST_NAME,ALL_VAL_MO,ALL_VAL_YR"
            logger.info(f"Using default export variables: {variables}")
        else:
            variables = "DISTRICT,DIST_NAME,GEN_VAL_MO,GEN_VAL_YR"
            logger.info(f"Using default import variables: {variables}")
    
    if not predicates:
        predicates = {}
    
    predicates["YEAR"] = f"{year}"
    
    if month:
        predicates["MONTH"] = f"{month}"
        logger.info(f"Including MONTH predicate: {month}")
    
    if dataset_type == "exports":
        dataset = "timeseries/intltrade/exports/enduse"
    else:
        dataset = "timeseries/intltrade/imports/enduse"
    
    return get_data(dataset, year, variables, geography=None, predicates=predicates, use_cache=use_cache, include_year_in_path=False)

def get_harmonized_system_data(year, dataset_type="exports", hs_level="2", variables=None, predicates=None, month=None, use_cache=True):
    """
    Retrieve international trade data by Harmonized System (HS) code using Census API.
    
    Uses the "porths" endpoint for HS-level data.
    
    Parameters:
        year (str): Year of the data (e.g., "2022").
        dataset_type (str): "exports" or "imports".
        hs_level (str): HS level to use (e.g., "2").
        variables (list or str): Variables to retrieve.
        predicates (dict): Additional predicates.
        month (str): Two-digit month if applicable.
        use_cache (bool): Whether to use caching.
    
    Returns:
        pandas.DataFrame: DataFrame with the retrieved data.
    """
    logger.info(f"Getting {dataset_type} HS{hs_level} data for year {year}")
    if not variables:
        if dataset_type == "exports":
            variables = "PORT,PORT_NAME,ALL_VAL_MO,ALL_VAL_YR"
            logger.info(f"Using default export HS variables: {variables}")
        else:
            variables = "PORT,PORT_NAME,GEN_VAL_MO,GEN_VAL_YR"
            logger.info(f"Using default import HS variables: {variables}")
    
    if not predicates:
        predicates = {}
    
    predicates["YEAR"] = f"{year}"
    
    if month:
        predicates["MONTH"] = f"{month}"
        logger.info(f"Including MONTH predicate: {month}")
    
    if dataset_type == "exports":
        dataset = "timeseries/intltrade/exports/porths"
    else:
        dataset = "timeseries/intltrade/imports/porths"
    
    return get_data(dataset, year, variables, geography=None, predicates=predicates, use_cache=use_cache, include_year_in_path=False)

def get_trade_balance_by_country(year, month, top_n=10, use_cache=True):
    """
    Calculate trade balance by aggregating international trade data at the district level.
    
    Parameters:
        year (str): Year (e.g., "2022").
        month (str): Two-digit month (e.g., "06").
        top_n (int): Number of top districts to return.
        use_cache (bool): Whether to use caching.
    
    Returns:
        pandas.DataFrame: DataFrame with trade balance calculations.
    """
    logger.info(f"Calculating trade balance by district for year {year}, month {month}")
    exports_df = get_import_export_data(year, "exports", month=month, use_cache=use_cache)
    if exports_df is None:
        logger.error("Failed to retrieve exports data for trade balance calculation")
        return None
    
    imports_df = get_import_export_data(year, "imports", month=month, use_cache=use_cache)
    if imports_df is None:
        logger.error("Failed to retrieve imports data for trade balance calculation")
        return None
    
    try:
        logger.info("Processing exports data")
        exports_df = exports_df.rename(columns={
            'ALL_VAL_YR': 'exports_value',
            'ALL_VAL_MO': 'exports_value_last_month'
        })
        
        logger.info("Processing imports data")
        imports_df = imports_df.rename(columns={
            'GEN_VAL_YR': 'imports_value',
            'GEN_VAL_MO': 'imports_value_last_month'
        })
        
        logger.info("Merging exports and imports data")
        merged_df = pd.merge(
            exports_df, 
            imports_df, 
            on='DISTRICT', 
            suffixes=('_export', '_import')
        )
        
        merged_df['trade_balance'] = merged_df['exports_value'] - merged_df['imports_value']
        merged_df['trade_balance_last_month'] = merged_df['exports_value_last_month'] - merged_df['imports_value_last_month']
        merged_df['DIST_NAME'] = merged_df['DIST_NAME_export']
        
        result_df = merged_df[[
            'DISTRICT', 'DIST_NAME', 'exports_value', 'imports_value', 
            'trade_balance', 'exports_value_last_month', 
            'imports_value_last_month', 'trade_balance_last_month'
        ]].copy()
        
        # Use .loc to avoid SettingWithCopyWarning
        result_df.loc[:, 'total_trade'] = result_df['exports_value'] + result_df['imports_value']
        result_df = result_df.sort_values('total_trade', ascending=False)
        
        top_districts = result_df.head(top_n)
        logger.info(f"Successfully calculated trade balance for top {top_n} districts")
        save_to_cache(top_districts.to_dict('records'), f"trade_balance_{year}_{month}_{top_n}")
        return top_districts
    except Exception as e:
        logger.error(f"Error calculating trade balance: {e}")
        return None

def get_specific_country_trade_data(country_code, year, month=None, use_cache=True):
    """
    Attempt to retrieve country-specific trade data from the Census API.
    
    Parameters:
        country_code (str): Country code (e.g., "5700" for China)
        year (str): Year of the data (e.g., "2022")
        month (str): Two-digit month (e.g., "06") - required parameter
        use_cache (bool): Whether to use caching
        
    Returns:
        pandas.DataFrame or None: DataFrame with country trade data if available
    """
    if not month:
        logger.warning("Month parameter is required for country trade data")
        logger.info("No country-specific data retrieved (missing month parameter)")
        return None
    
    logger.info(f"Attempting to retrieve trade data for country code {country_code} for {year}-{month}")
    
    try:
        # Try to get exports data by country
        exports_dataset = "timeseries/intltrade/exports/country"
        exports_variables = "CTY_CODE,CTY_NAME,ALL_VAL_MO,ALL_VAL_YR"
        predicates = {"YEAR": year, "MONTH": month, "CTY_CODE": country_code}
        
        exports_df = get_data(
            exports_dataset, 
            year, 
            exports_variables, 
            predicates=predicates, 
            use_cache=use_cache,
            include_year_in_path=False
        )
        
        if exports_df is None or exports_df.empty:
            logger.warning(f"No export data found for country code {country_code}")
            logger.info("Country-level export data not available or no data for specified country")
            return None
            
        logger.info(f"Successfully retrieved country-level export data for {country_code}")
        return exports_df
        
    except Exception as e:
        logger.error(f"Error retrieving country-specific trade data: {e}")
        logger.info("Detailed country-level trade data may not be available via the Census API")
        return None

def get_latest_trade_year():
    """
    Determine the latest available trade year from the Census API.
    
    This function uses a more reliable approach by fetching recent years
    and checking which one returns valid data.
    
    Returns:
        str: The latest year for which trade data is available (e.g., "2022")
    """
    try:
        # Try last few years in reverse order (most recent first)
        current_year = datetime.now().year
        for year in range(current_year, current_year - 3, -1):
            year_str = str(year)
            logger.info(f"Checking if trade data available for {year_str}")
            
            # Try to get a small sample of data for June of that year
            url = f"{API_BASE_URL}/timeseries/intltrade/exports/enduse"
            params = {
                "get": "DISTRICT",
                "YEAR": year_str,
                "MONTH": "06",
                "key": CENSUS_API_KEY
            }
            
            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    logger.info(f"Found latest available trade year: {year_str}")
                    return year_str
            except Exception:
                pass  # Continue to try earlier years
        
        # Fallback to a safe default if no recent year works
        default_year = "2022"
        logger.warning(f"Could not determine latest trade year, using default: {default_year}")
        return default_year
    except Exception as e:
        logger.error(f"Error determining latest trade year: {e}")
        logger.warning("Using default trade year: 2022")
        return "2022"

def get_historical_trade_data(dataset_type="exports", start_year="2018", end_year="2022", use_cache=True):
    """
    Retrieve historical trade data for multiple years.
    
    Parameters:
        dataset_type (str): "exports" or "imports"
        start_year (str): Starting year for data range
        end_year (str): Ending year for data range
        use_cache (bool): Whether to use caching
        
    Returns:
        pandas.DataFrame: Combined DataFrame with historical data
    """
    logger.info(f"Getting historical {dataset_type} data from {start_year} to {end_year}")
    
    # Initialize empty list to store yearly data
    yearly_data = []
    
    # For each year in range, get June data (mid-year snapshot)
    for year in range(int(start_year), int(end_year) + 1):
        year_str = str(year)
        logger.info(f"Fetching {dataset_type} data for {year_str}")
        
        # Get data for June of each year
        df = get_import_export_data(
            year=year_str,
            dataset_type=dataset_type,
            month="06",
            use_cache=use_cache
        )
        
        if df is not None:
            # Add year column for easier tracking
            df['YEAR'] = year_str
            yearly_data.append(df)
            logger.info(f"Added {len(df)} rows for {year_str}")
        else:
            logger.warning(f"No data retrieved for {year_str}")
    
    # Combine all yearly data
    if yearly_data:
        combined_df = pd.concat(yearly_data, ignore_index=True)
        logger.info(f"Combined historical data: {len(combined_df)} rows")
        return combined_df
    else:
        logger.error("No historical data retrieved")
        return None

def get_trade_data_by_hs_chapter(year, month, hs_chapters=None, dataset_type="exports", use_cache=True):
    """
    Get trade data grouped by HS chapter (2-digit HS code).
    """
    logger.info(f"Getting {dataset_type} data by HS chapter for {year}-{month}")
    
    # Get all HS data
    hs_data = get_harmonized_system_data(
        year=year,
        dataset_type=dataset_type,
        hs_level="2",
        month=month,
        use_cache=use_cache
    )
    
    if hs_data is None:
        logger.error(f"Failed to retrieve HS data for {year}-{month}")
        return None
    
    try:
        # Inspect and log the first few PORT values to understand format
        sample_ports = hs_data['PORT'].head(10).tolist()
        logger.info(f"Sample PORT values: {sample_ports}")
        
        # Check for any HS-like pattern (numeric or with HS prefix)
        # Modified to be more flexible in detecting HS codes
        hs_pattern = r'^(HS)?(\d{2})'
        
        # Create a new column with extracted HS chapter codes
        hs_data['HS_CHAPTER'] = hs_data['PORT'].str.extract(hs_pattern, expand=False)[1]
        
        # Filter rows where HS_CHAPTER was successfully extracted
        hs_ports = hs_data.dropna(subset=['HS_CHAPTER'])
        
        if hs_ports.empty:
            logger.warning("No HS-coded ports found in data. Attempting alternative extraction method.")
            
            # Alternative method: Try to extract from PORT_NAME if available
            if 'PORT_NAME' in hs_data.columns:
                # Look for patterns like "HS 01 - Live Animals" or similar in PORT_NAME
                name_pattern = r'HS\s+(\d{2})[-\s:]'
                hs_data['HS_CHAPTER'] = hs_data['PORT_NAME'].str.extract(name_pattern, expand=False)
                hs_ports = hs_data.dropna(subset=['HS_CHAPTER'])
            
            if hs_ports.empty:
                logger.warning("Could not extract HS codes from PORT or PORT_NAME columns")
                return None
            else:
                logger.info(f"Successfully extracted {len(hs_ports)} HS codes from PORT_NAME")
        else:
            logger.info(f"Successfully extracted {len(hs_ports)} HS codes from PORT")
        
        # Filter by specific HS chapters if provided
        if hs_chapters:
            hs_ports = hs_ports[hs_ports['HS_CHAPTER'].isin(hs_chapters)]
            logger.info(f"Filtered to {len(hs_ports)} rows for requested HS chapters")
        
        # Group by HS chapter and sum values
        if dataset_type == "exports":
            value_cols = ['ALL_VAL_MO', 'ALL_VAL_YR']
        else:
            value_cols = ['GEN_VAL_MO', 'GEN_VAL_YR']
        
        grouped = hs_ports.groupby('HS_CHAPTER')[value_cols].sum().reset_index()
        
        # Add HS chapter descriptions
        hs_descriptions = get_hs_chapter_descriptions()
        if hs_descriptions is not None:
            grouped = pd.merge(
                grouped,
                hs_descriptions,
                on='HS_CHAPTER',
                how='left'
            )
        
        logger.info(f"Successfully aggregated data for {len(grouped)} HS chapters")
        return grouped
    except Exception as e:
        logger.error(f"Error processing HS data: {e}")
        return None


def inspect_harmonized_system_data(year, month, use_cache=False):
    """
    Helper function to inspect the structure of harmonized system data.
    This can be useful for debugging.
    
    Parameters:
        year (str): Year of the data
        month (str): Month of the data
        use_cache (bool): Whether to use caching
        
    Returns:
        dict: Analysis of the data structure
    """
    logger.info(f"Inspecting harmonized system data for {year}-{month}")
    
    # Get raw data
    data = get_harmonized_system_data(
        year=year,
        dataset_type="exports",
        hs_level="2",
        month=month,
        use_cache=use_cache
    )
    
    if data is None:
        logger.error("Failed to retrieve harmonized system data")
        return None
    
    analysis = {
        "rows": len(data),
        "columns": data.columns.tolist(),
        "port_prefix_counts": {},
        "port_name_patterns": []
    }
    
    # Analyze PORT values
    if 'PORT' in data.columns:
        port_values = data['PORT'].astype(str)
        
        # Count prefix patterns (first 2 characters)
        for prefix in port_values.str[:2].value_counts().items():
            analysis["port_prefix_counts"][prefix[0]] = prefix[1]
        
        # Sample unique PORT values
        analysis["port_samples"] = port_values.sample(min(10, len(port_values))).tolist()
    
    # Analyze PORT_NAME values if available
    if 'PORT_NAME' in data.columns:
        port_names = data['PORT_NAME'].astype(str)
        
        # Look for patterns in port names
        patterns = {
            "contains_dash": port_names.str.contains(' - ').sum(),
            "contains_colon": port_names.str.contains(':').sum(),
            "starts_with_hs": port_names.str.lower().str.startswith('hs').sum()
        }
        analysis["port_name_patterns"] = patterns
        
        # Sample unique PORT_NAME values
        analysis["port_name_samples"] = port_names.sample(min(10, len(port_names))).tolist()
    
    # Save analysis to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join("data", f"{timestamp}_hs_data_analysis.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=4, ensure_ascii=False)
    
    logger.info(f"Saved data structure analysis to {filepath}")
    return analysis


def get_hs_chapter_descriptions(use_cache=True):
    """
    Get a mapping of HS chapter codes to descriptions.
    This function is just a wrapper for get_hs_chapter_descriptions_from_api
    using the latest year and month.
    
    Parameters:
        use_cache (bool): Whether to use caching
        
    Returns:
        pandas.DataFrame: DataFrame with HS chapter codes and descriptions
    """
    latest_year = get_latest_trade_year()
    month = "06"  # Using June as a default month
    
    return get_hs_chapter_descriptions_from_api(latest_year, month, use_cache)

def get_hs_chapter_descriptions_from_api(year, month, use_cache=True):
    """
    Get a mapping of HS chapter codes to descriptions by analyzing the porths dataset.
    
    Parameters:
        year (str): Year to use for getting data
        month (str): Month to use for getting data
        use_cache (bool): Whether to use caching
        
    Returns:
        pandas.DataFrame: DataFrame with HS chapter codes and descriptions
    """
    cache_file = os.path.join(CACHE_DIR, f"hs_chapter_descriptions_{year}_{month}.json")
    
    # Try to load from cache first
    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            if time.time() - cache_data['timestamp'] < CACHE_EXPIRATION:
                logger.info(f"Loaded HS chapter descriptions from cache")
                return pd.DataFrame(cache_data['data'])
        except Exception as e:
            logger.warning(f"Failed to load HS descriptions from cache: {e}")
    
    logger.info(f"Fetching HS chapter descriptions from Census API for {year}-{month}")
    
    try:
        # Fetch HS data from porths endpoint
        data = get_harmonized_system_data(
            year=year,
            dataset_type="exports",
            hs_level="2",
            month=month,
            use_cache=use_cache
        )
        
        if data is None:
            logger.warning("Failed to retrieve porths data")
            return get_hs_chapter_fallback()
        
        # Look for various patterns in PORT and PORT_NAME columns
        hs_chapters = {}
        
        # Look for patterns in PORT values
        for _, row in data.iterrows():
            port = row['PORT']
            port_name = row['PORT_NAME'] if 'PORT_NAME' in row else None
            
            # Try different pattern matches for PORT and PORT_NAME
            hs_code = None
            description = None
            
            # Pattern 1: HS01, HS02, etc.
            if isinstance(port, str) and port.startswith('HS') and len(port) >= 4:
                hs_code = port[2:4]
            
            # Pattern 2: 01, 02, etc. (direct 2-digit codes)
            elif isinstance(port, str) and port.isdigit() and len(port) >= 2:
                hs_code = port[:2]
                
            # Extract description from PORT_NAME if available
            if port_name and isinstance(port_name, str):
                # Pattern 1: "HS 01 - Live Animals"
                if ' - ' in port_name:
                    description = port_name.split(' - ', 1)[1].strip()
                # Pattern 2: "HS 01: Live Animals"
                elif ': ' in port_name:
                    description = port_name.split(': ', 1)[1].strip()
                # Pattern 3: Just take the whole name if we have a code
                elif hs_code:
                    description = port_name.strip()
            
            # Save any successfully extracted codes and descriptions
            if hs_code and description:
                hs_chapters[hs_code] = description
        
        # If we found any HS chapters, create a DataFrame
        if hs_chapters:
            result = [{'HS_CHAPTER': code, 'DESCRIPTION': desc} for code, desc in hs_chapters.items()]
            result_df = pd.DataFrame(result)
            
            # Cache the results
            try:
                with open(cache_file, 'w') as f:
                    json.dump({
                        'timestamp': time.time(),
                        'data': result
                    }, f)
                logger.info(f"Cached {len(result)} HS chapter descriptions")
            except Exception as e:
                logger.warning(f"Failed to cache HS descriptions: {e}")
            
            logger.info(f"Retrieved {len(result)} HS chapter descriptions from API")
            return result_df
            
        logger.warning("Could not extract HS chapter descriptions from API response")
        return get_hs_chapter_fallback()
    except Exception as e:
        logger.error(f"Error fetching HS chapter descriptions: {e}")
        return get_hs_chapter_fallback()

def get_hs_chapter_fallback():
    """
    Fallback function that returns static HS chapter descriptions if API call fails.
    """
    logger.warning("Using fallback static HS chapter descriptions")
    
    # For brevity, I'm including just a few chapters
    hs_chapters = [
        {'HS_CHAPTER': '01', 'DESCRIPTION': 'Live Animals'},
        {'HS_CHAPTER': '02', 'DESCRIPTION': 'Meat and Edible Meat Offal'},
        # Add more chapters as needed or load from a local file
        {'HS_CHAPTER': '84', 'DESCRIPTION': 'Machinery and Mechanical Appliances'},
        {'HS_CHAPTER': '85', 'DESCRIPTION': 'Electrical Machinery and Equipment'},
        {'HS_CHAPTER': '87', 'DESCRIPTION': 'Vehicles Other Than Railway or Tramway'}
    ]
    
    return pd.DataFrame(hs_chapters)


def get_sector_mapping():
    """
    Returns a mapping from HS chapters to economic sectors for dashboard display.
    
    Returns:
        dict: Mapping of HS chapters to dashboard sectors
    """
    # This mapping groups HS chapters into broader economic sectors
    # for visualization purposes
    sector_mapping = {
        # Agriculture and Food
        '01': 'Agriculture',
        '02': 'Agriculture',
        '03': 'Agriculture',
        '04': 'Agriculture',
        '05': 'Agriculture',
        '06': 'Agriculture',
        '07': 'Agriculture',
        '08': 'Agriculture',
        '09': 'Agriculture',
        '10': 'Agriculture',
        '11': 'Agriculture',
        '12': 'Agriculture',
        '13': 'Agriculture',
        '14': 'Agriculture',
        '15': 'Agriculture',
        '16': 'Agriculture',
        '17': 'Agriculture',
        '18': 'Agriculture',
        '19': 'Agriculture',
        '20': 'Agriculture',
        '21': 'Agriculture',
        '22': 'Agriculture',
        '23': 'Agriculture',
        '24': 'Agriculture',
        
        # Raw Materials & Minerals
        '25': 'Raw Materials',
        '26': 'Raw Materials',
        '27': 'Energy',
        
        # Chemicals & Pharmaceuticals
        '28': 'Chemicals',
        '29': 'Chemicals',
        '30': 'Pharmaceuticals',
        '31': 'Chemicals',
        '32': 'Chemicals',
        '33': 'Chemicals',
        '34': 'Chemicals',
        '35': 'Chemicals',
        '36': 'Chemicals',
        '37': 'Chemicals',
        '38': 'Chemicals',
        
        # Plastics & Rubber
        '39': 'Plastics & Rubber',
        '40': 'Plastics & Rubber',
        
        # Textiles & Apparel
        '41': 'Textiles & Apparel',
        '42': 'Textiles & Apparel',
        '43': 'Textiles & Apparel',
        '50': 'Textiles & Apparel',
        '51': 'Textiles & Apparel',
        '52': 'Textiles & Apparel',
        '53': 'Textiles & Apparel',
        '54': 'Textiles & Apparel',
        '55': 'Textiles & Apparel',
        '56': 'Textiles & Apparel',
        '57': 'Textiles & Apparel',
        '58': 'Textiles & Apparel',
        '59': 'Textiles & Apparel',
        '60': 'Textiles & Apparel',
        '61': 'Textiles & Apparel',
        '62': 'Textiles & Apparel',
        '63': 'Textiles & Apparel',
        '64': 'Textiles & Apparel',
        '65': 'Textiles & Apparel',
        
        # Wood & Paper
        '44': 'Wood & Paper',
        '45': 'Wood & Paper',
        '46': 'Wood & Paper',
        '47': 'Wood & Paper',
        '48': 'Wood & Paper',
        '49': 'Wood & Paper',
        
        # Metals & Metal Products
        '72': 'Metals',
        '73': 'Metals',
        '74': 'Metals',
        '75': 'Metals',
        '76': 'Metals',
        '78': 'Metals',
        '79': 'Metals',
        '80': 'Metals',
        '81': 'Metals',
        '82': 'Metals',
        '83': 'Metals',
        
        # Machinery & Electronics
        '84': 'Machinery & Electronics',
        '85': 'Machinery & Electronics',
        
        # Transportation
        '86': 'Transportation',
        '87': 'Transportation',
        '88': 'Transportation',
        '89': 'Transportation',
        
        # Precision Instruments
        '90': 'Precision Instruments',
        '91': 'Precision Instruments',
        
        # Miscellaneous Manufacturing
        '66': 'Miscellaneous',
        '67': 'Miscellaneous',
        '68': 'Miscellaneous',
        '69': 'Miscellaneous',
        '70': 'Miscellaneous',
        '71': 'Miscellaneous',
        '92': 'Miscellaneous',
        '93': 'Miscellaneous',
        '94': 'Miscellaneous',
        '95': 'Miscellaneous',
        '96': 'Miscellaneous',
        '97': 'Miscellaneous',
        '98': 'Miscellaneous',
        '99': 'Miscellaneous'
    }
    
    return sector_mapping

def get_sector_trade_data(year, month, dataset_type="exports", use_cache=True):
    """
    Get trade data aggregated by economic sector for dashboard display.
    
    Parameters:
        year (str): Year of the data
        month (str): Month of the data
        dataset_type (str): "exports" or "imports"
        use_cache (bool): Whether to use caching
        
    Returns:
        pandas.DataFrame: DataFrame with trade data by sector
    """
    logger.info(f"Getting {dataset_type} data by economic sector for {year}-{month}")
    
    # First get data by HS chapter
    hs_data = get_trade_data_by_hs_chapter(
        year=year,
        month=month,
        dataset_type=dataset_type,
        use_cache=use_cache
    )
    
    if hs_data is None or hs_data.empty:
        logger.error(f"Failed to retrieve HS chapter data for {year}-{month}")
        return None
    
    try:
        # Get sector mapping
        sector_map = get_sector_mapping()
        
        # Add sector to HS data
        hs_data['SECTOR'] = hs_data['HS_CHAPTER'].map(sector_map)
        
        # Fill missing sectors with "Miscellaneous"
        hs_data = hs_data.copy() 
        hs_data['SECTOR'] = hs_data['SECTOR'].fillna('Miscellaneous')
        
        # Determine value columns based on dataset type
        if dataset_type == "exports":
            value_cols = ['ALL_VAL_MO', 'ALL_VAL_YR']
        else:
            value_cols = ['GEN_VAL_MO', 'GEN_VAL_YR']
        
        # Group by sector and sum values
        sector_data = hs_data.groupby('SECTOR')[value_cols].sum().reset_index()
        
        # Sort by value (highest to lowest)
        sector_data = sector_data.sort_values(
            value_cols[0], 
            ascending=False
        )
        
        # Calculate percentages for pie chart
        total = sector_data[value_cols[0]].sum()
        sector_data['PERCENTAGE'] = (sector_data[value_cols[0]] / total * 100).round(1)
        
        logger.info(f"Successfully aggregated data for {len(sector_data)} economic sectors")
        return sector_data
    except Exception as e:
        logger.error(f"Error processing sector data: {e}")
        return None

def get_trade_deficit_time_series(start_year, end_year, use_cache=True):
    """
    Generate a time series of trade deficit data for historical analysis.
    """
    logger.info(f"Generating trade deficit time series from {start_year} to {end_year}")
    
    try:
        # Get historical exports and imports data
        exports_data = get_historical_trade_data(
            dataset_type="exports",
            start_year=start_year,
            end_year=end_year,
            use_cache=use_cache
        )
        
        imports_data = get_historical_trade_data(
            dataset_type="imports",
            start_year=start_year,
            end_year=end_year,
            use_cache=use_cache
        )
        
        if exports_data is None or imports_data is None:
            logger.error("Failed to retrieve historical trade data")
            return None
        
        # Log data structure for debugging
        logger.info(f"Exports data columns: {exports_data.columns.tolist()}")
        logger.info(f"Imports data columns: {imports_data.columns.tolist()}")
        
        # Ensure we have district-level data for consistent aggregation
        if 'DISTRICT' not in exports_data.columns or 'DISTRICT' not in imports_data.columns:
            logger.error("Missing required DISTRICT column in historical data")
            return None
            
        # Aggregate by year for total national values
        exports_by_year = exports_data.groupby('YEAR')['ALL_VAL_YR'].sum().reset_index()
        exports_by_year.rename(columns={'ALL_VAL_YR': 'EXPORTS'}, inplace=True)
        
        imports_by_year = imports_data.groupby('YEAR')['GEN_VAL_YR'].sum().reset_index()
        imports_by_year.rename(columns={'GEN_VAL_YR': 'IMPORTS'}, inplace=True)
        
        # Merge export and import data
        merged_df = pd.merge(
            exports_by_year,
            imports_by_year,
            on='YEAR'
        )
        
        # Calculate trade deficit and balance
        merged_df['DEFICIT'] = merged_df['IMPORTS'] - merged_df['EXPORTS']
        merged_df['BALANCE'] = merged_df['EXPORTS'] - merged_df['IMPORTS']
        
        # Convert to billions for display
        for col in ['EXPORTS', 'IMPORTS', 'DEFICIT', 'BALANCE']:
            merged_df[f'{col}_BILLIONS'] = (merged_df[col] / 1_000_000_000).round(2)
        
        logger.info(f"Generated trade deficit time series with {len(merged_df)} years of data")
        return merged_df
    except Exception as e:
        logger.error(f"Error generating trade deficit time series: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def get_tariff_dashboard_data(year, month, use_cache=True):
    """
    Compile complete dataset needed for tariff dashboard using Census API data.
    
    Parameters:
        year (str): Year for the data
        month (str): Month for the data
        use_cache (bool): Whether to use caching
        
    Returns:
        dict: Dictionary containing all required dashboard datasets
    """
    logger.info(f"Compiling tariff dashboard data for {year}-{month}")
    
    dashboard_data = {}
    
    # 1. Trade Balance by District (for map visualization)
    trade_balance = get_trade_balance_by_country(
        year=year,
        month=month,
        top_n=50,  # Get more districts for comprehensive coverage
        use_cache=use_cache
    )
    if trade_balance is not None:
        dashboard_data['trade_balance'] = trade_balance.to_dict('records')
        logger.info(f"Added trade balance data ({len(trade_balance)} districts)")
    
    # 2. Sector-specific data (for pie chart)
    exports_by_sector = get_sector_trade_data(
        year=year,
        month=month,
        dataset_type="exports",
        use_cache=use_cache
    )
    if exports_by_sector is not None:
        dashboard_data['sector_data'] = exports_by_sector.to_dict('records')
        logger.info(f"Added sector export data ({len(exports_by_sector)} sectors)")
    
    # 3. Historical time series (5-year trend)
    start_year = str(int(year) - 4)
    deficit_time_series = get_trade_deficit_time_series(
        start_year=start_year,
        end_year=year,
        use_cache=use_cache
    )
    if deficit_time_series is not None:
        dashboard_data['time_series'] = deficit_time_series.to_dict('records')
        logger.info(f"Added historical time series data ({len(deficit_time_series)} years)")
    
    # 4. Detailed HS chapter data (for detailed table)
    hs_data = get_trade_data_by_hs_chapter(
        year=year,
        month=month,
        dataset_type="exports",
        use_cache=use_cache
    )
    if hs_data is not None:
        dashboard_data['hs_data'] = hs_data.to_dict('records')
        logger.info(f"Added HS chapter data ({len(hs_data)} chapters)")
    
    # Meta information
    dashboard_data['metadata'] = {
        'data_year': year,
        'data_month': month,
        'generated_at': datetime.now().isoformat(),
        'data_source': 'U.S. Census Bureau API',
    }
    
    logger.info("Tariff dashboard data compilation complete")
    return dashboard_data

# -----------------------------------------------------------------------------
# Main Function
# -----------------------------------------------------------------------------
def main():
    """
    Main function to demonstrate the use of the Census API client.
    """
    if not test_connection():
        logger.error("Failed to connect to the Census API.")
        return
    
    latest_year = get_latest_trade_year()
    logger.info(f"Using latest trade year: {latest_year}")
    
    # Example 1: International Trade Exports by End-Use
    logger.info("Example 1: Getting international trade exports by end-use")
    exports_data = get_import_export_data(year=latest_year, dataset_type="exports", month="06")
    if exports_data is not None:
        logger.info(f"Retrieved exports data with {len(exports_data)} rows")
        save_data_to_file(exports_data, "census_exports_data.csv")
    
    # Example 2: International Trade Imports by End-Use
    logger.info("Example 2: Getting international trade imports by end-use")
    imports_data = get_import_export_data(year=latest_year, dataset_type="imports", month="06")
    if imports_data is not None:
        logger.info(f"Retrieved imports data with {len(imports_data)} rows")
        save_data_to_file(imports_data, "census_imports_data.csv")
    
    # Example 3: Trade Balance by District
    logger.info("Example 3: Calculating trade balance by district")
    trade_balance = get_trade_balance_by_country(year=latest_year, month="06", top_n=25)
    if trade_balance is not None:
        logger.info(f"Calculated trade balance with {len(trade_balance)} rows")
        save_data_to_file(trade_balance, "census_trade_balance.csv")
    
    # Example 4: Exports Data by HS (using the porths endpoint)
    logger.info("Example 4: Getting exports data by Harmonized System (HS) code")
    hs_exports = get_harmonized_system_data(year=latest_year, dataset_type="exports", hs_level="2", month="06")
    if hs_exports is not None:
        logger.info(f"Retrieved HS exports data with {len(hs_exports)} rows")
        save_data_to_file(hs_exports, "census_hs_exports.csv")
    
    # Example 5: Sector-Based Trade Data
    logger.info("Example 5: Getting trade data by economic sector")
    sector_data = get_sector_trade_data(year=latest_year, month="06", dataset_type="exports")
    if sector_data is not None:
        logger.info(f"Retrieved sector trade data with {len(sector_data)} rows")
        save_data_to_file(sector_data, "census_sector_data.csv")
    
    # Example 6: Historical Trade Deficit Data
    logger.info("Example 6: Getting historical trade deficit data")
    start_year = str(int(latest_year) - 4)  # 5-year trend
    deficit_data = get_trade_deficit_time_series(start_year=start_year, end_year=latest_year)
    if deficit_data is not None:
        logger.info(f"Retrieved historical deficit data with {len(deficit_data)} rows")
        save_data_to_file(deficit_data, "census_deficit_timeseries.csv")
    
    # Example 7: Complete Dashboard Data
    logger.info("Example 7: Compiling complete dashboard dataset")
    dashboard_data = get_tariff_dashboard_data(year=latest_year, month="06")
    if dashboard_data:
        logger.info("Dashboard data compilation successful")
        save_data_to_file(dashboard_data, "tariff_dashboard_data.json")

if __name__ == "__main__":
    main()