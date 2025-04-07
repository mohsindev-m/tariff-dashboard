import os
import requests
import json
import logging
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
# API base URL
API_BASE_URL = "https://apps.bea.gov/api/data"

BEA_API_KEY = 'EB3C36A8-1BE3-49B5-8F90-347C5281ED01'
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

def test_connection():
    """
    Test connection to the BEA API to verify the API key is valid
    
    Returns:
        bool: True if connection is successful, False otherwise
    """
    logger.info("Testing connection to BEA API...")
    
    # Try a simple dataset list request to test connectivity
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
        params (dict): Dictionary of parameters to send to the API
        
    Returns:
        dict: JSON response from the API or None if request failed
    """
    # Create a copy of params to avoid modifying the original
    request_params = params.copy()
    
    # Add the API key to the parameters
    request_params['UserID'] = BEA_API_KEY
    
    # Set the default result format to JSON if not specified
    if 'ResultFormat' not in request_params:
        request_params['ResultFormat'] = 'json'
    
    try:
        # Log the request parameters (excluding the API key for security)
        display_params = request_params.copy()
        if 'UserID' in display_params:
            api_key = display_params['UserID']
            if len(api_key) > 8:
                display_params['UserID'] = f"{api_key[:4]}...{api_key[-4:]}"
        
        # Make the request
        logger.info(f"Making request to BEA API with parameters: {display_params}")
        
        # Create the request
        response = requests.get(API_BASE_URL, params=request_params, timeout=60)
        
        # Log response status
        logger.info(f"Response status: {response.status_code}")
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse the response
        if request_params['ResultFormat'].lower() == 'json':
            return response.json()
        else:  # XML
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
    """
    Get the list of available datasets from the BEA API.
    
    Returns:
        dict: JSON response containing the list of datasets
    """
    params = {
        'method': 'GETDATASETLIST'
    }
    
    return make_request(params)

def get_parameter_list(dataset_name):
    """
    Get the list of parameters for a specific dataset.
    
    Parameters:
        dataset_name (str): Name of the dataset
        
    Returns:
        dict: JSON response containing the parameters for the dataset
    """
    params = {
        'method': 'GETPARAMETERLIST',
        'DatasetName': dataset_name
    }
    
    return make_request(params)

def get_parameter_values(dataset_name, parameter_name):
    """
    Get the list of valid values for a specific parameter in a dataset.
    
    Parameters:
        dataset_name (str): Name of the dataset
        parameter_name (str): Name of the parameter
        
    Returns:
        dict: JSON response containing the valid values for the parameter
    """
    params = {
        'method': 'GETPARAMETERVALUES',
        'DatasetName': dataset_name,
        'ParameterName': parameter_name
    }
    
    return make_request(params)

def get_parameter_values_filtered(dataset_name, target_parameter, **filter_params):
    """
    Get filtered parameter values for a specific parameter based on other parameters.
    
    Parameters:
        dataset_name (str): Name of the dataset
        target_parameter (str): Target parameter to get values for
        **filter_params: Additional parameters to filter the results
        
    Returns:
        dict: JSON response containing the filtered parameter values
    """
    params = {
        'method': 'GETPARAMETERVALUESFILTERED',
        'DatasetName': dataset_name,
        'TargetParameter': target_parameter
    }
    
    # Add any additional filter parameters
    params.update(filter_params)
    
    return make_request(params)

# -----------------------------------------------------------------------------
# Data Retrieval Methods
# -----------------------------------------------------------------------------
def get_data(dataset_name, **params):
    """
    Get data from a specific dataset with the given parameters.
    
    Parameters:
        dataset_name (str): Name of the dataset
        **params: Additional parameters for the data request
        
    Returns:
        dict: JSON response containing the requested data
    """
    request_params = {
        'method': 'GETDATA',
        'DatasetName': dataset_name
    }
    
    # Add any additional parameters
    request_params.update(params)
    
    return make_request(request_params)

# -----------------------------------------------------------------------------
# Specialized Dataset Functions
# -----------------------------------------------------------------------------
def get_gdp_by_industry(table_id, frequency, year, industry):
    """
    Get GDP by Industry data.
    
    Parameters:
        table_id (str): Table ID (use 'ALL' for all tables)
        frequency (str): Frequency ('A' for annual, 'Q' for quarterly)
        year (str): Year(s) (comma-separated or 'ALL' for all years)
        industry (str): Industry code(s) (comma-separated or 'ALL' for all industries)
        
    Returns:
        dict: JSON response containing the GDP by Industry data
    """
    params = {
        'TableID': table_id,
        'Frequency': frequency,
        'Year': year,
        'Industry': industry
    }
    
    return get_data('GDPbyIndustry', **params)

def get_regional_data(table_name, line_code, geo_fips, year='LAST5'):
    """
    Get Regional data (income, product, and employment by state and local area).
    
    Parameters:
        table_name (str): Published table name
        line_code (str or int): Line code in table (use 'ALL' for all statistics)
        geo_fips (str): Geographic area code(s)
        year (str): Year(s) (comma-separated, 'LAST5', 'LAST10', or 'ALL')
        
    Returns:
        dict: JSON response containing the Regional data
    """
    params = {
        'TableName': table_name,
        'LineCode': line_code,
        'GeoFips': geo_fips,
        'Year': year
    }
    
    return get_data('Regional', **params)

def get_nipa_data(table_name, frequency, year):
    """
    Get National Income and Product Accounts (NIPA) data.
    
    Parameters:
        table_name (str): Table name
        frequency (str): Frequency ('A' for annual, 'Q' for quarterly, 'M' for monthly)
        year (str): Year(s) (comma-separated or 'ALL' for all years)
        
    Returns:
        dict: JSON response containing the NIPA data
    """
    params = {
        'TableName': table_name,
        'Frequency': frequency,
        'Year': year
    }
    
    return get_data('NIPA', **params)

def get_international_transactions(indicator='All', area_or_country='AllCountries', 
                                  frequency='A', year='All'):
    """
    Get International Transactions (ITA) data.
    
    Parameters:
        indicator (str): Indicator code for the type of transaction
        area_or_country (str): Area or country code
        frequency (str): Frequency ('A', 'QSA', 'QNSA')
        year (str): Year(s) (comma-separated or 'ALL' for all years)
        
    Returns:
        dict: JSON response containing the International Transactions data
    """
    params = {
        'Indicator': indicator,
        'AreaOrCountry': area_or_country,
        'Frequency': frequency,
        'Year': year
    }
    
    return get_data('ITA', **params)

def get_international_services_trade(type_of_service='All', trade_direction='All',
                                    affiliation='All', area_or_country='AllCountries',
                                    year='All'):
    """
    Get International Services Trade data.
    
    Parameters:
        type_of_service (str): Type of service
        trade_direction (str): Trade direction
        affiliation (str): Affiliation
        area_or_country (str): Area or country
        year (str): Year(s) (comma-separated or 'ALL' for all years)
        
    Returns:
        dict: JSON response containing the International Services Trade data
    """
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
    """
    Main function to demonstrate the use of the BEA API client.
    """
    # Test connection to the BEA API
    if not test_connection():
        logger.error("Failed to connect to the BEA API. Check your API key.")
        return
    
    # Example 1: Get the list of available datasets
    logger.info("Example 1: Getting the list of available datasets")
    datasets = get_dataset_list()
    if datasets:
        save_data_to_file(datasets, "bea_datasets.json")
        
        # Extract dataset names if available
        if 'BEAAPI' in datasets and 'Results' in datasets['BEAAPI']:
            results = datasets['BEAAPI']['Results']
            if 'Dataset' in results:
                dataset_list = results['Dataset']
                logger.info(f"Available datasets: {len(dataset_list)}")
                for i, dataset in enumerate(dataset_list[:5]):  # Show first 5 datasets
                    logger.info(f"Dataset {i+1}: {dataset.get('DatasetName')} - {dataset.get('DatasetDescription')}")
    
    # Example 2: Get regional data for all states for the last 5 years
    logger.info("Example 2: Getting regional GDP data for all states for the last 5 years")
    regional_data = get_regional_data(
        table_name="SAGDP9N",  # Real GDP by state
        line_code=1,           # All industry total
        geo_fips="STATE",      # All states
        year="LAST5"           # Last 5 years
    )
    if regional_data:
        save_data_to_file(regional_data, "bea_regional_gdp.json")
    
    # Example 3: Get international transactions data
    logger.info("Example 3: Getting international transactions data for the United States")
    ita_data = get_international_transactions(
        indicator="BalGds",       # Balance on goods
        area_or_country="China",  # Transactions with China
        frequency="A",            # Annual data
        year="2015,2016,2017,2018,2019"  # Last 5 years
    )
    if ita_data:
        save_data_to_file(ita_data, "bea_ita_china.json")
    
    # Example 4: Get GDP by industry data
    logger.info("Example 4: Getting GDP by industry data")
    gdp_industry_data = get_gdp_by_industry(
        table_id="1",     # Value Added by Industry
        frequency="A",    # Annual
        year="LAST5",     # Last 5 years
        industry="ALL"    # All industries
    )
    if gdp_industry_data:
        save_data_to_file(gdp_industry_data, "bea_gdp_by_industry.json")

if __name__ == "__main__":
    main()