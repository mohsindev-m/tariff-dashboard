import os
from dotenv import load_dotenv 
import requests
import json
import logging
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WTO_API_Client")


# Updated base URL according to the API definition
API_BASE_URL = "https://api.wto.org/timeseries/v1"
DATA_ENDPOINT = f"{API_BASE_URL}/data"
INDICATORS_ENDPOINT = f"{API_BASE_URL}/indicators"

# Use the provided API key
API_KEY = "36faf295023942b99db1af50883c2398"
logger.info(f"Using API key: {API_KEY[:4]}...{API_KEY[-4:]}")

# Define headers according to the documentation
HEADERS = {
    "Cache-Control": "no-cache",
    "Ocp-Apim-Subscription-Key": API_KEY
}

# -----------------------------------------------------------------------------
# Function: test_connection
# -----------------------------------------------------------------------------
def test_connection():
    """
    Test connection to the WTO API to verify the API key is valid
    
    Returns:
        bool: True if connection is successful, False otherwise
    """
    logger.info("Testing connection to WTO API...")
    
    # Log the headers being used (hiding the full API key)
    display_headers = HEADERS.copy()
    api_key = display_headers.get('Ocp-Apim-Subscription-Key', '')
    if api_key and len(api_key) > 8:
        display_headers['Ocp-Apim-Subscription-Key'] = f"{api_key[:4]}...{api_key[-4:]}"
    logger.info(f"Using headers: {display_headers}")
    
    # Try a very simple endpoint first
    endpoints_to_try = [
        "/years",
        "/topics",
        "/frequencies"
    ]
    
    for endpoint_path in endpoints_to_try:
        try:
            test_endpoint = f"{API_BASE_URL}{endpoint_path}"
            logger.info(f"Testing endpoint: {test_endpoint}")
            
            response = requests.get(test_endpoint, headers=HEADERS, timeout=10)
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                logger.info(f"Connection successful to {endpoint_path} (Status 200)")
                # For years endpoint, show the data
                if endpoint_path == "/years" and response.text:
                    try:
                        years_data = response.json()
                        if isinstance(years_data, list) and len(years_data) > 0:
                            logger.info(f"Available years: {years_data[:5]}...")
                    except:
                        logger.info(f"Response content (first 100 chars): {response.text[:100]}")
                return True
            else:
                logger.warning(f"Connection to {endpoint_path} failed with status code: {response.status_code}")
                logger.warning(f"Response content: {response.text[:500]}")
                # Continue to try other endpoints
        except Exception as e:
            logger.warning(f"Connection test to {endpoint_path} failed with error: {e}")
            # Continue to try other endpoints
    
    logger.error("All connection tests failed")
    return False

# -----------------------------------------------------------------------------
# Function: fetch_tariff_data
# -----------------------------------------------------------------------------
def fetch_tariff_data(indicator_code, reporting_economy="all", partner_economy="default",
                     time_period="default", product_sector="default", include_sub=False,
                     output_format="json", output_mode="full", decimals="default",
                     offset=0, max_records=500, heading="H", lang=1, include_meta=False):
    """
    Fetch tariff data from the WTO Timeseries API (/data endpoint).
    """
    endpoint = DATA_ENDPOINT
    params = {
        "i": indicator_code,
        "r": reporting_economy,
        "p": partner_economy,
        "ps": time_period,
        "pc": product_sector,
        "spc": str(include_sub).lower(),
        "fmt": output_format,
        "mode": output_mode,
        "dec": decimals,
        "off": offset,
        "max": max_records,
        "head": heading,
        "lang": lang,
        "meta": str(include_meta).lower()
    }
    
    logger.info(f"Fetching tariff data for indicator: {indicator_code}")
    logger.info(f"Request parameters: {params}")
    
    try:
        # Log the actual URL being requested (for debugging)
        request = requests.Request('GET', endpoint, params=params, headers=HEADERS)
        prepared_request = request.prepare()
        logger.info(f"Request URL: {prepared_request.url}")
        
        # Make the actual request
        response = requests.get(endpoint, params=params, headers=HEADERS, timeout=30)
        
        # Log response status and headers for debugging
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response headers: {response.headers}")
        
        # Handle 204 No Content separately
        if response.status_code == 204:
            logger.info("No data available for the requested parameters (204 No Content)")
            return None
        
        # Try to get response content for other responses
        try:
            content = response.json() if response.text else {}
            logger.info(f"Response content: {json.dumps(content)[:500]}")
        except:
            logger.info(f"Response text: {response.text[:500]}")
        
        # Now raise for status to handle errors
        response.raise_for_status()
        
        # For 200 responses, parse and return the data
        if response.status_code == 200 and response.text:
            logger.info(f"Tariff data fetched successfully (status code: {response.status_code})")
            return response.json()
        else:
            logger.warning(f"Unexpected response (status: {response.status_code}, content length: {len(response.text) if response.text else 0})")
            return None
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching tariff data: {http_err}")
    except Exception as err:
        logger.error(f"An error occurred while fetching tariff data: {err}")
    return None

# -----------------------------------------------------------------------------
# Function: fetch_indicators
# -----------------------------------------------------------------------------
def fetch_indicators(i="all", name=None, t="all", pc="all", tp="all", frq="all", lang=1):
    """
    Fetch the list of indicators from the WTO Timeseries API (/indicators endpoint).
    
    Parameters:
        i (str): Indicator code filter (default 'all').
        name (str): Indicator name filter.
        t (str): Topics filter (default 'all').
        pc (str): Product classifications filter (default 'all').
        tp (str): Trade partner filter (default 'all').
        frq (str): Frequency filter (default 'all').
        lang (int): Language id (default 1: English).
    
    Returns:
        dict or list: Parsed JSON response or None on failure.
    """
    endpoint = INDICATORS_ENDPOINT
    params = {
        "i": i,
        "t": t,
        "pc": pc,
        "tp": tp,
        "frq": frq,
        "lang": lang
    }
    
    # Only add name parameter if it's provided
    if name:
        params["name"] = name
    
    logger.info("Fetching indicators list")
    logger.debug(f"Request parameters: {params}")
    
    try:
        response = requests.get(endpoint, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        logger.info(f"Indicators fetched successfully (status code: {response.status_code})")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching indicators: {http_err}")
        if hasattr(response, 'text') and response.text:
            logger.error(f"Response content: {response.text[:500]}")
    except Exception as err:
        logger.error(f"An error occurred while fetching indicators: {err}")
    return None

# -----------------------------------------------------------------------------
# Function: fetch_reporters
# -----------------------------------------------------------------------------
def fetch_reporters(name=None, ig="all", reg="all", gp="all", lang=1):
    """
    Fetch the list of reporting economies from the WTO Timeseries API.
    
    Parameters:
        name (str): Reporter name (or part of name).
        ig (str): Individual/Group economies filter ('individual', 'group', or 'all').
        reg (str): Regions filter (comma separated codes).
        gp (str): Groups filter (comma separated codes).
        lang (int): Language id (default 1: English).
    
    Returns:
        list: List of reporting economies or None on failure.
    """
    endpoint = f"{API_BASE_URL}/reporters"
    params = {
        "ig": ig,
        "reg": reg,
        "gp": gp,
        "lang": lang
    }
    if name:
        params["name"] = name
    
    logger.info("Fetching reporting economies")
    try:
        response = requests.get(endpoint, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        logger.info(f"Reporting economies fetched successfully (status code: {response.status_code})")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching reporters: {http_err}")
        if hasattr(response, 'text') and response.text:
            logger.error(f"Response content: {response.text[:500]}")
    except Exception as err:
        logger.error(f"An error occurred while fetching reporters: {err}")
    return None

# -----------------------------------------------------------------------------
# Function: fetch_product_classifications
# -----------------------------------------------------------------------------
def fetch_product_classifications(lang=1):
    """
    Fetch the list of product classifications from the WTO Timeseries API.
    
    Parameters:
        lang (int): Language id (default 1: English).
    
    Returns:
        list: List of product classifications or None on failure.
    """
    endpoint = f"{API_BASE_URL}/product_classifications"
    params = {
        "lang": lang
    }
    
    logger.info("Fetching product classifications")
    try:
        response = requests.get(endpoint, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        logger.info(f"Product classifications fetched successfully (status code: {response.status_code})")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching product classifications: {http_err}")
        if hasattr(response, 'text') and response.text:
            logger.error(f"Response content: {response.text[:500]}")
    except Exception as err:
        logger.error(f"An error occurred while fetching product classifications: {err}")
    return None

# -----------------------------------------------------------------------------
# Function: save_data_to_file
# -----------------------------------------------------------------------------
def save_data_to_file(data, filename="wto_data.json"):
    """
    Saves the provided data into a JSON file within a local 'data' directory.
    The filename is prefixed with a timestamp.
    
    Parameters:
        data (dict or list): The data to save.
        filename (str): Base filename.
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

# -----------------------------------------------------------------------------
# Main Execution Flow
# -----------------------------------------------------------------------------
def main():
    """
    Main function to execute the WTO API client operations.
    """
    # First, test the connection and API key
    if not test_connection():
        logger.error("Connection test failed. Please check your API key and internet connection.")
        logger.info(f"API Key being used (first 4 characters): {API_KEY[:4] if API_KEY and len(API_KEY) > 4 else 'None'}")
        return
    
    # Example 1: Fetch tariff data - using a simpler approach
    logger.info("EXAMPLE 1: Fetching tariff data")
    
    # First, find a valid tariff indicator code
    logger.info("Looking for valid tariff indicators...")
    tariff_indicators = fetch_indicators(name="tariff")
    
    if tariff_indicators and len(tariff_indicators) > 0:
        # Use the first tariff indicator we find
        tariff_indicator = tariff_indicators[0]
        indicator_code = tariff_indicator["code"]
        logger.info(f"Using indicator: {indicator_code} - {tariff_indicator.get('name', 'Unknown')}")
        
        # Use simpler parameters
        reporting_economy = "all"  # Start with all economies
        time_period = "2020"       # Just use a single recent year
        product_sector = "default" # Use default product sector grouping
        
        tariff_data = fetch_tariff_data(
            indicator_code,
            reporting_economy=reporting_economy,
            partner_economy="default",
            time_period=time_period,
            product_sector=product_sector,
            include_sub=False,
            output_format="json",
            output_mode="full",
            decimals="default",
            offset=0,
            max_records=100,  # Reduced for testing
            heading="H",
            lang=1,
            include_meta=False
        )
    else:
        logger.error("No tariff indicators found")
        tariff_data = None
    
    if tariff_data:
        # Check for both possible response formats
        if "data" in tariff_data:
            logger.info(f"Number of tariff records fetched: {len(tariff_data['data'])}")
        elif "Dataset" in tariff_data:
            logger.info(f"Number of tariff records fetched: {len(tariff_data['Dataset'])}")
            # Display some sample data
            if len(tariff_data['Dataset']) > 0:
                sample = tariff_data['Dataset'][0]
                logger.info(f"Sample data - Indicator: {sample.get('Indicator')}")
                logger.info(f"Sample data - Economy: {sample.get('ReportingEconomy')}")
                logger.info(f"Sample data - Year: {sample.get('Year', '(not specified)')}")
                if 'Value' in sample:
                    logger.info(f"Sample data - Value: {sample.get('Value')}")
        else:
            logger.warning("Unexpected data format in the tariff API response.")
            logger.info(f"Response keys: {list(tariff_data.keys())}")
        
        save_data_to_file(tariff_data, "wto_tariff_data.json")
    else:
        logger.error("Failed to fetch tariff data from the WTO API.")
    
    # Example 2: Fetch the list of indicators
    logger.info("EXAMPLE 2: Fetching indicator list")
    indicators_data = fetch_indicators(name="tariff")  # Filtering to indicators with 'tariff' in the name
    
    if indicators_data:
        if isinstance(indicators_data, list):
            logger.info(f"Number of indicators fetched: {len(indicators_data)}")
            # Print all tariff indicator codes and names for reference
            logger.info("Available tariff indicators:")
            for i, indicator in enumerate(indicators_data):
                logger.info(f"Indicator {i+1}: {indicator.get('code')} - {indicator.get('name')}")
                # Print more details for the first few
                if i < 3:
                    logger.info(f"  - Years: {indicator.get('startYear')} to {indicator.get('endYear')}")
                    logger.info(f"  - Classification: {indicator.get('productSectorClassificationCode')}")
                    logger.info(f"  - Reporting economies: {indicator.get('numberReporters')}")
                    logger.info(f"  - Description: {indicator.get('description')}")
        else:
            logger.warning("Unexpected response format for indicators data.")
        save_data_to_file(indicators_data, "wto_indicators_data.json")
    else:
        logger.error("Failed to fetch indicators data from the WTO API.")
    
    # Example 3: Fetch reporting economies
    logger.info("EXAMPLE 3: Fetching reporting economies")
    reporters_data = fetch_reporters()
    
    if reporters_data:
        if isinstance(reporters_data, list):
            logger.info(f"Number of reporting economies fetched: {len(reporters_data)}")
            # Print first 5 reporter names for reference
            for i, reporter in enumerate(reporters_data[:5]):
                logger.info(f"Reporter {i+1}: {reporter.get('code')} - {reporter.get('name')}")
        else:
            logger.warning("Unexpected response format for reporters data.")
        save_data_to_file(reporters_data, "wto_reporters_data.json")
    else:
        logger.error("Failed to fetch reporters data from the WTO API.")

if __name__ == "__main__":
    main()