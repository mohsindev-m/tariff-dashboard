import os
import json
import logging
import requests
from datetime import datetime

# -----------------------------------------------------------------------------
# Logger Setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BEA_API_Client")

# -----------------------------------------------------------------------------
# Global Variables & Configuration
# -----------------------------------------------------------------------------
API_BASE_URL = "https://apps.bea.gov/api/data"
BEA_API_KEY = os.getenv("BEA_API_KEY")
logger.info(f"Using BEA API key: {BEA_API_KEY[:4]}...{BEA_API_KEY[-4:] if len(BEA_API_KEY) > 8 else ''}")

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def save_data_to_file(data, filename="bea_data.json"):
    """
    Saves the provided data into a JSON file within a local 'data' directory.
    The filename is prefixed with a timestamp.
    
    Parameters:
        data (dict or list): The data to save.
        filename (str): Base filename.
        
    Returns:
        str: Path to the saved file.
    """
    try:
        os.makedirs("data", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join("data", f"{timestamp}_{filename}")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Data successfully saved to file: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to save data to file: {e}")
        return None

def normalize_bea_records(data):
    """
    Normalize field names in the BEA data records.
    In particular, rename the key "IndustrYDescription" to "IndustryDescription".
    If the input is a list of records, normalize each record.
    
    Parameters:
        data (dict or list): BEA API response data.
    
    Returns:
        Normalized data with updated field names.
    """
    def normalize_record(record):
        if "IndustrYDescription" in record:
            record["IndustryDescription"] = record.pop("IndustrYDescription")
        return record

    if isinstance(data, dict) and "BEAAPI" in data and "Results" in data["BEAAPI"]:
        results = data["BEAAPI"]["Results"]
        if isinstance(results, list):
            for result in results:
                if "Data" in result and isinstance(result["Data"], list):
                    result["Data"] = [normalize_record(rec) for rec in result["Data"]]
    elif isinstance(data, list):
        data = [normalize_record(rec) for rec in data]
    return data

def test_connection():
    """
    Test connection to the BEA API to verify the API key is valid.
    
    Returns:
        bool: True if connection is successful, False otherwise.
    """
    logger.info("Testing connection to BEA API...")
    try:
        response = get_dataset_list()
        if response and 'BEAAPI' in response:
            logger.info("Connection successful to BEA API")
            return True
        else:
            logger.error("Connection test failed: Invalid response format")
            return False
    except Exception as e:
        logger.error(f"Connection test failed with error: {e}")
        return False

# -----------------------------------------------------------------------------
# Core API Functions
# -----------------------------------------------------------------------------
def make_request(params):
    """
    Make a request to the BEA API with the given parameters.
    
    Parameters:
        params (dict): Dictionary of parameters to send to the API.
        
    Returns:
        dict: JSON response from the API or None if request failed.
    """
    request_params = params.copy()
    request_params['UserID'] = BEA_API_KEY
    if 'ResultFormat' not in request_params:
        request_params['ResultFormat'] = 'json'
    try:
        display_params = request_params.copy()
        if 'UserID' in display_params:
            api_key = display_params['UserID']
            if len(api_key) > 8:
                display_params['UserID'] = f"{api_key[:4]}...{api_key[-4:]}"
        logger.info(f"Making request to BEA API with parameters: {display_params}")
        response = requests.get(API_BASE_URL, params=request_params, timeout=60)
        logger.info(f"Response status: {response.status_code}")
        response.raise_for_status()
        if request_params['ResultFormat'].lower() == 'json':
            return response.json()
        else:
            return response.text
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        logger.error(f"Response content: {response.text[:500]}...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    return None

# -----------------------------------------------------------------------------
# Meta-Data API Methods
# -----------------------------------------------------------------------------
def get_dataset_list():
    params = {
        'method': 'GETDATASETLIST'
    }
    return make_request(params)

def get_parameter_list(dataset_name):
    params = {
        'method': 'GETPARAMETERLIST',
        'DatasetName': dataset_name
    }
    return make_request(params)

def get_parameter_values(dataset_name, parameter_name):
    params = {
        'method': 'GETPARAMETERVALUES',
        'DatasetName': dataset_name,
        'ParameterName': parameter_name
    }
    return make_request(params)

def get_parameter_values_filtered(dataset_name, target_parameter, **filter_params):
    params = {
        'method': 'GETPARAMETERVALUESFILTERED',
        'DatasetName': dataset_name,
        'TargetParameter': target_parameter
    }
    params.update(filter_params)
    return make_request(params)

# -----------------------------------------------------------------------------
# Data Retrieval Methods
# -----------------------------------------------------------------------------
def get_data(dataset_name, **params):
    request_params = {
        'method': 'GETDATA',
        'DatasetName': dataset_name
    }
    request_params.update(params)
    return make_request(request_params)

# -----------------------------------------------------------------------------
# Specialized Dataset Functions
# -----------------------------------------------------------------------------
def get_gdp_by_industry(table_id, frequency, year, industry):
    params = {
        'TableID': table_id,
        'Frequency': frequency,
        'Year': year,
        'Industry': industry
    }
    return get_data('GDPbyIndustry', **params)

def get_regional_data(table_name, line_code, geo_fips, year='LAST5'):
    # Using updated table name "SAGDP9" for Regional GDP.
    params = {
        'TableName': table_name,   # Pass "SAGDP9" when calling this function.
        'LineCode': str(line_code),  # Ensure line_code is a string
        'GeoFips': geo_fips,
        'Year': year
    }
    return get_data('Regional', **params)

def get_nipa_data(table_name, frequency, year):
    params = {
        'TableName': table_name,
        'Frequency': frequency,
        'Year': year
    }
    return get_data('NIPA', **params)

def get_international_transactions(indicator='All', area_or_country='AllCountries', frequency='A', year='All'):
    params = {
        'Indicator': indicator,
        'AreaOrCountry': area_or_country,
        'Frequency': frequency,
        'Year': year
    }
    return get_data('ITA', **params)

def get_international_services_trade(type_of_service='All', trade_direction='All', affiliation='All', area_or_country='AllCountries', year='All'):
    params = {
        'TypeOfService': type_of_service,
        'TradeDirection': trade_direction,
        'Affiliation': affiliation,
        'AreaOrCountry': area_or_country,
        'Year': year
    }
    return get_data('IntlServTrade', **params)

# -----------------------------------------------------------------------------
# Main Function
# -----------------------------------------------------------------------------
def main():
    # Test connection to the BEA API
    if not test_connection():
        logger.error("Failed to connect to the BEA API. Check your API key.")
        return

    # Example 1: Get the list of available datasets
    logger.info("Example 1: Getting the list of available datasets")
    datasets = get_dataset_list()
    if datasets:
        save_data_to_file(datasets, "bea_datasets.json")
        if 'BEAAPI' in datasets and 'Results' in datasets['BEAAPI']:
            results = datasets['BEAAPI']['Results']
            if 'Dataset' in results:
                dataset_list = results['Dataset']
                logger.info(f"Available datasets: {len(dataset_list)}")
                for i, dataset in enumerate(dataset_list[:5]):  # Show first 5 datasets
                    logger.info(f"Dataset {i+1}: {dataset.get('DatasetName')} - {dataset.get('DatasetDescription')}")
    
    # Example 2: Get regional GDP data for all states for the last 5 years
    logger.info("Example 2: Getting regional GDP data for all states for the last 5 years")
    # Using updated table name "SAGDP9"
    regional_data = get_regional_data(
        table_name="SAGDP9",  # Updated table name (e.g., "SAGDP9" instead of "SAGDP9N")
        line_code=1,          # All industry total
        geo_fips="STATE",     # All states
        year="LAST5"          # Last 5 years (if supported)
    )
    if regional_data:
        save_data_to_file(regional_data, "bea_regional_gdp.json")
    
    # Example 3: Get international transactions data
    logger.info("Example 3: Getting international transactions data for the United States")
    ita_data = get_international_transactions(
        indicator="BalGds",       # Balance on goods indicator
        area_or_country="China",  # Transactions with China
        frequency="A",            # Annual data
        year="2015,2016,2017,2018,2019"  # Explicit years
    )
    if ita_data:
        save_data_to_file(ita_data, "bea_ita_china.json")
    
    # Example 4: Get GDP by industry data with explicit years
    logger.info("Example 4: Getting GDP by industry data")
    gdp_industry_data = get_gdp_by_industry(
        table_id="1",              # Value Added by Industry
        frequency="A",             # Annual
        year="2020,2021,2022,2023,2024",  # Explicit last 5 years
        industry="ALL"             # All industries
    )
    if gdp_industry_data:
        normalized_data = normalize_bea_records(gdp_industry_data)
        save_data_to_file(normalized_data, "bea_gdp_by_industry.json")

if __name__ == "__main__":
    main()
