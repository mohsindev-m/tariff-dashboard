#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WTO Timeseries API Scraper

This module provides a production-ready implementation for fetching tariff
and trade data from the WTO Timeseries API for the tariff dashboard project.
"""

import os
import sys
import json
import time
import logging
import argparse
import pandas as pd
import requests
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("wto_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("wto_scraper")

class WTOTimeseriesAPI:
    """
    Client for the WTO Timeseries API, providing methods for fetching
    tariff and trade data with proper authentication and error handling.
    """
    
    BASE_URL = "http://api.wto.org/timeseries/v1"
    
    def __init__(self, api_key: str = None, retry_attempts: int = 3, 
                 timeout: int = 60, output_dir: str = "data",
                 batch_size: int = 5):
        """
        Initialize the WTO API client.
        
        Args:
            api_key: WTO API subscription key
            retry_attempts: Number of retry attempts for failed requests
            timeout: Request timeout in seconds
            output_dir: Directory to save output files
            batch_size: Number of countries to include in a single request
        """
        self.api_key = api_key or os.environ.get('WTO_API_KEY')
        if not self.api_key:
            logger.warning("No WTO API key provided. Set WTO_API_KEY environment variable.")
        
        self.timeout = timeout
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.session = self._create_session(retry_attempts)
        
        # Cache for API responses to reduce redundant calls
        self._cache = {}
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _create_session(self, retry_attempts: int) -> requests.Session:
        """
        Create a requests session with retry capability.
        
        Args:
            retry_attempts: Maximum number of retries
            
        Returns:
            Configured requests session
        """
        session = requests.Session()
        
        # Configure retry strategy with exponential backoff
        retry_strategy = Retry(
            total=retry_attempts,
            backoff_factor=1.0,  # More aggressive backoff
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make a request to the WTO API with proper error handling.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            
        Returns:
            JSON response from API
            
        Raises:
            Exception: If API request fails
        """
        url = f"{self.BASE_URL}/{endpoint}"
        params = params or {}
        headers = {}
        
        # Add API key to headers if available
        if self.api_key:
            headers['Ocp-Apim-Subscription-Key'] = self.api_key
        
        # Create cache key based on url and params
        cache_key = f"{url}:{json.dumps(params, sort_keys=True)}"
        
        # Return cached result if available
        if cache_key in self._cache:
            logger.debug(f"Using cached result for {url}")
            return self._cache[cache_key]
        
        try:
            logger.info(f"Making request to {url} with params: {params}")
            response = self.session.get(
                url, 
                params=params, 
                headers=headers,
                timeout=self.timeout
            )
            
            # Log response metadata
            logger.debug(f"Response status: {response.status_code}, content-type: {response.headers.get('content-type')}")
            
            # Handle response based on status code
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Log response structure for debugging
                    if isinstance(data, dict):
                        logger.debug(f"Response is a dictionary with keys: {list(data.keys())}")
                        if 'errors' in data:
                            logger.error(f"API returned error response: {data['errors']}")
                            raise Exception(f"API error: {data['errors']}")
                    elif isinstance(data, list):
                        logger.debug(f"Response is a list with {len(data)} items")
                        if len(data) > 0:
                            logger.debug(f"First item type: {type(data[0])}")
                            if isinstance(data[0], dict):
                                logger.debug(f"Sample first item keys: {list(data[0].keys())[:5]}")
                    else:
                        logger.debug(f"Response is of type: {type(data)}")
                        
                    # Cache successful responses
                    self._cache[cache_key] = data
                    return data
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON response. Raw content (first 500 chars): {response.text[:500]}")
                    raise Exception("Invalid JSON response from API")
            elif response.status_code == 401:
                logger.error("Authentication failed - invalid or missing API key")
                raise Exception(f"API authentication failed: {response.text}. Please check your WTO API subscription key.")
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded. Waiting before retry.")
                time.sleep(10)  # Wait before retrying
                return self._make_request(endpoint, params)  # Retry recursively
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text[:500]}")
                raise Exception(f"API request failed: {response.status_code} - {response.text[:500]}")
                
        except requests.exceptions.Timeout:
            logger.error(f"Request to {url} timed out after {self.timeout} seconds")
            raise Exception(f"Request timed out after {self.timeout} seconds")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error while connecting to {url}")
            raise Exception("Connection error - please check your network connection")
        except Exception as e:
            logger.error(f"Unexpected error in API request: {str(e)}")
            raise
    
    def parse_wto_response(self, response):
        """
        Parse the WTO API response data structure to extract data points.
        
        Args:
            response: API response object
            
        Returns:
            List of data points in standardized format
        """
        if not response:
            logger.warning("Empty response to parse")
            return []
            
        # Check if response is a dictionary with a Dataset key
        if isinstance(response, dict) and 'Dataset' in response:
            dataset = response['Dataset']
            logger.info(f"Found {len(dataset)} items in Dataset key")
            return dataset
            
        # If response is already a list, use it directly
        if isinstance(response, list):
            return response
            
        # Try to find any list in the response that could contain data
        if isinstance(response, dict):
            for key, value in response.items():
                if isinstance(value, list) and len(value) > 0:
                    logger.info(f"Found potential data list in key '{key}' with {len(value)} items")
                    # Log first item to check structure
                    if value and isinstance(value[0], dict):
                        logger.debug(f"First item keys: {list(value[0].keys())}")
                    return value
                    
        logger.warning(f"Could not find data points in response, type: {type(response)}")
        return []
    
    def get_indicators(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Get available indicators, optionally filtered by name.
        
        Args:
            name_filter: Optional text to filter indicators by name
            
        Returns:
            List of indicator objects
        """
        params = {
            'lang': 1  # English
        }
        
        if name_filter:
            params['name'] = name_filter
        
        response = self._make_request('indicators', params)
        
        # Log the number of indicators found
        count = len(response) if isinstance(response, list) else 0
        logger.info(f"Found {count} indicators" + (f" matching '{name_filter}'" if name_filter else ""))
        
        return response
    
    def get_tariff_indicators(self) -> List[Dict[str, Any]]:
        """
        Get tariff-related indicators from the WTO API.
        
        Returns:
            List of indicator objects related to tariffs
        """
        # We first try searching for 'tariff' in indicator names
        indicators = self.get_indicators(name_filter='tariff')
        
        # If we don't find many indicators, try additional keywords
        if len(indicators) < 5:
            logger.info("Few tariff indicators found, trying additional keywords...")
            
            # Try additional keywords that might be related to tariffs
            for keyword in ['MFN', 'duty', 'applied rate', 'bound rate']:
                additional = self.get_indicators(name_filter=keyword)
                # Add new indicators not already in our list
                for ind in additional:
                    if not any(existing.get('code') == ind.get('code') for existing in indicators):
                        indicators.append(ind)
            
            logger.info(f"Found total of {len(indicators)} indicators after expansion")
        
        return indicators
    
    def get_reporters(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Get reporting economies (countries).
        
        Args:
            name_filter: Optional text to filter countries by name
            
        Returns:
            List of reporter objects
        """
        params = {
            'ig': 'individual',  # Only individual countries, not groups
            'lang': 1            # English
        }
        
        if name_filter:
            params['name'] = name_filter
        
        return self._make_request('reporters', params)
    
    def get_partners(self, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Get partner economies (countries).
        
        Args:
            name_filter: Optional text to filter countries by name
            
        Returns:
            List of partner objects
        """
        params = {
            'ig': 'individual',  # Only individual countries, not groups
            'lang': 1            # English
        }
        
        if name_filter:
            params['name'] = name_filter
        
        return self._make_request('partners', params)
    
    def get_product_classifications(self) -> List[Dict[str, Any]]:
        """
        Get product classifications.
        
        Returns:
            List of product classification objects
        """
        params = {
            'lang': 1  # English
        }
        
        return self._make_request('product_classifications', params)
    
    def get_products(self, classification_code: str = None, name_filter: str = None) -> List[Dict[str, Any]]:
        """
        Get products/sectors.
        
        Args:
            classification_code: Optional classification code to filter by
            name_filter: Optional text to filter products by name
            
        Returns:
            List of product objects
        """
        params = {
            'lang': 1  # English
        }
        
        if classification_code:
            params['pc'] = classification_code
        
        if name_filter:
            params['name'] = name_filter
        
        return self._make_request('products', params)
    
    def get_tariff_data_batch(self, indicator_code: str, reporter_codes: List[str], 
                           years: Union[List[int], str], product_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get tariff data for a batch of countries.
        
        Args:
            indicator_code: Indicator code (e.g., "ITS_AVG_MFN")
            reporter_codes: List of country codes
            years: List of years or year range (e.g., [2020, 2021] or "2018-2022")
            product_codes: Optional list of product codes
            
        Returns:
            List of tariff data points
        """
        params = {
            'i': indicator_code,
            'r': ','.join(reporter_codes),
            'fmt': 'json',
            'mode': 'full',
            'meta': True,
            'dec': 2,
            'max': 100000,
        }
        
        # Format years parameter
        if isinstance(years, list):
            params['ps'] = ','.join(map(str, years))
        else:
            params['ps'] = years
        
        # Add product codes if specified
        if product_codes:
            params['pc'] = ','.join(product_codes)
        
        logger.info(f"Fetching tariff data with indicator {indicator_code} for batch of {len(reporter_codes)} countries")
        
        try:
            response = self._make_request('data', params)
            
            # Parse the response to extract data points
            data_points = self.parse_wto_response(response)
            
            if data_points:
                logger.info(f"Successfully retrieved {len(data_points)} data points")
                return data_points
            else:
                logger.warning("No data points found in response")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching tariff data batch: {str(e)}")
            return []
    
    def get_tariff_data(self, indicator_code: str, reporter_codes: List[str], 
                      years: Union[List[int], str], product_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get tariff data for specified parameters, processing countries in batches.
        
        Args:
            indicator_code: Indicator code (e.g., "ITS_AVG_MFN")
            reporter_codes: List of country codes
            years: List of years or year range (e.g., [2020, 2021] or "2018-2022")
            product_codes: Optional list of product codes
            
        Returns:
            List of tariff data points
        """
        all_data = []
        
        # Process countries in batches to avoid timeouts
        for i in range(0, len(reporter_codes), self.batch_size):
            batch = reporter_codes[i:i+self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1} with {len(batch)} countries")
            
            # Fetch data for this batch
            batch_data = self.get_tariff_data_batch(indicator_code, batch, years, product_codes)
            
            if batch_data:
                all_data.extend(batch_data)
                logger.info(f"Added {len(batch_data)} data points from batch")
            else:
                logger.warning(f"No data retrieved for batch with countries: {batch}")
            
            # Add a delay between batches to avoid rate limiting
            if i + self.batch_size < len(reporter_codes):
                logger.debug("Sleeping between batches")
                time.sleep(2)
        
        logger.info(f"Total data points collected: {len(all_data)}")
        return all_data
    
    def get_trade_balance_data(self, reporter_codes: List[str], years: Union[List[int], str]) -> List[Dict[str, Any]]:
        """
        Get trade balance data for specified countries and years.
        
        Args:
            reporter_codes: List of country codes
            years: List of years or year range
            
        Returns:
            List of trade balance data points
        """
        # First, get available indicators to find the right trade balance indicator
        indicators = self.get_indicators()
        
        # Look for trade balance indicators
        trade_balance_indicator = None
        for ind in indicators:
            if not isinstance(ind, dict):
                continue
                
            name = ind.get('name', '').lower()
            if 'trade balance' in name or ('trade' in name and 'balance' in name):
                trade_balance_indicator = ind.get('code')
                logger.info(f"Found trade balance indicator: {trade_balance_indicator}")
                break
                
        # If we couldn't find a specific one, try these common codes
        if not trade_balance_indicator:
            common_indicators = ["TS_ML_USD", "ITS_CS_VMT", "ITS_CS_VXT"]
            
            for code in common_indicators:
                # Check if this code exists in our indicators
                for ind in indicators:
                    if isinstance(ind, dict) and ind.get('code') == code:
                        trade_balance_indicator = code
                        logger.info(f"Using common trade indicator: {trade_balance_indicator}")
                        break
                
                if trade_balance_indicator:
                    break
        
        # If we still don't have an indicator, use a default
        if not trade_balance_indicator:
            trade_balance_indicator = "TS_ML_USD"  # Common indicator for merchandise trade balance
            logger.warning(f"Using default trade balance indicator: {trade_balance_indicator}")
        
        return self.get_tariff_data(trade_balance_indicator, reporter_codes, years)
    
    def transform_to_dataframe(self, data_points: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform API response data points to a pandas DataFrame.
        
        Args:
            data_points: List of data points from API
            
        Returns:
            Pandas DataFrame with structured data
        """
        # Check if we have valid data to transform
        if not data_points:
            logger.warning("No data points provided to transform")
            return pd.DataFrame()
            
        # Check if we're dealing with a list or something else
        if not isinstance(data_points, list):
            logger.warning(f"Data points is not a list, got {type(data_points)}")
            
            # Try to handle dictionary response
            if isinstance(data_points, dict):
                logger.debug(f"Dictionary keys: {list(data_points.keys())}")
                
                # Look for data arrays in the dictionary
                for key, value in data_points.items():
                    if isinstance(value, list) and len(value) > 0:
                        logger.info(f"Found data points in key '{key}', using that instead")
                        return self.transform_to_dataframe(value)
            
            # If we can't find any suitable data, return empty DataFrame
            return pd.DataFrame()
        
        # If the list is empty, return empty DataFrame
        if len(data_points) == 0:
            logger.warning("Empty list of data points")
            return pd.DataFrame()
        
        # Check first item to see if it's a dictionary
        if not isinstance(data_points[0], dict):
            logger.warning(f"First data point is not a dictionary, got {type(data_points[0])}")
            return pd.DataFrame()
        
        # Print sample data structure for debugging
        if len(data_points) > 0 and isinstance(data_points[0], dict):
            logger.info(f"Sample data point structure: {json.dumps(data_points[0], indent=2)}")
            logger.info(f"Sample data point keys: {list(data_points[0].keys())}")
        
        # Create a list to hold the transformed data
        transformed_data = []
        
        # WTO API field mapping - corrected based on actual API response
        field_mappings = {
            # Capital case field names from actual API response
            'IndicatorCode': 'indicator_code',
            'Indicator': 'indicator_name',
            'ReportingEconomyCode': 'country_code',
            'ReportingEconomy': 'country_name',
            'PartnerEconomyCode': 'partner_code',
            'PartnerEconomy': 'partner_name',
            'ProductOrSectorCode': 'product_code',
            'ProductOrSector': 'product_name',
            'Year': 'year',
            'Period': 'period', 
            'Frequency': 'frequency',
            'Unit': 'unit',
            'Value': 'value',
            'ValueFlagCode': 'flag',
            
            # Original camelCase field names kept as backup
            'indicatorCode': 'indicator_code',
            'indicator': 'indicator_name',
            'reportingEconomyCode': 'country_code',
            'reportingEconomy': 'country_name',
            'partnerEconomyCode': 'partner_code',
            'partnerEconomy': 'partner_name',
            'productOrSectorCode': 'product_code',
            'productOrSector': 'product_name',
            'year': 'year',
            'period': 'period', 
            'frequency': 'frequency',
            'unit': 'unit',
            'value': 'value',
            'valueFlagCode': 'flag'
        }
        
        count_transformed = 0
        for point in data_points:
            # Skip non-dictionary data points
            if not isinstance(point, dict):
                logger.warning(f"Skipping non-dictionary data point: {point}")
                continue
                
            # Extract relevant fields using mapping
            transformed_point = {}
            for api_field, our_field in field_mappings.items():
                if api_field in point:
                    transformed_point[our_field] = point.get(api_field)
            
            # Add unit and value mapping
            if 'UnitCode' in point:
                transformed_point['unit'] = point.get('Unit')
            
            # Only add points that have at least some key fields
            if transformed_point and ('country_code' in transformed_point or 'indicator_code' in transformed_point):
                transformed_data.append(transformed_point)
                count_transformed += 1
            else:
                logger.debug(f"Skipping point with insufficient data: {point}")
        
        # Log how many points were transformed
        logger.info(f"Successfully transformed {count_transformed} data points out of {len(data_points)}")
        
        # Check if we have any data to create DataFrame
        if not transformed_data:
            logger.warning("No data points were successfully transformed")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(transformed_data)
        
        # Log column info
        logger.debug(f"DataFrame columns: {list(df.columns)}")
        logger.debug(f"DataFrame shape: {df.shape}")
        
        if not df.empty and 'value' in df.columns:
            df = df.rename(columns={'value': 'tariff_value', 'unit': 'tariff_unit'})
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: Pandas DataFrame to save
            filename: Base filename (without extension)
            
        Returns:
            Path to the saved file
        """
        # Add timestamp and extension
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, full_filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        logger.info(f"Data saved to {filepath}")
        
        return filepath
    
    def save_to_json(self, data: Any, filename: str) -> str:
        """
        Save data to JSON file.
        
        Args:
            data: Data to save
            filename: Base filename (without extension)
            
        Returns:
            Path to the saved file
        """
        # Add timestamp and extension
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.json"
        filepath = os.path.join(self.output_dir, full_filename)
        
        # Save to JSON
        with open(filepath, 'w') as f:
            if isinstance(data, pd.DataFrame):
                f.write(data.to_json(orient='records', date_format='iso'))
            else:
                json.dump(data, f, indent=2)
        
        logger.info(f"Data saved to {filepath}")
        
        return filepath
    
    def fetch_country_tariff_profiles(self, countries: List[str], 
                                    years: Union[List[int], str] = "2018-2022") -> pd.DataFrame:
        """
        Fetch comprehensive tariff profiles for specified countries.
        
        Args:
            countries: List of country codes
            years: Years to include (list or range string)
            
        Returns:
            DataFrame with tariff profile data
        """
        logger.info(f"Fetching tariff profiles for {len(countries)} countries")
        
        # Handle empty country list
        if not countries:
            logger.warning("No valid countries provided to fetch_country_tariff_profiles")
            return pd.DataFrame()  # Return empty DataFrame
            
        # Get available tariff indicators first
        tariff_indicators = self.get_tariff_indicators()
        
        # From the log, we can see several MFN-related indicators
        # Let's select an appropriate one for each data type we need
        
        # For tariff data, we'll use one of these indicators (prioritized):
        # 1. TP_A_0150: MFN -  Number of applied tariff lines
        # 2. HS_A_0040: HS MFN - Number of national tariff lines
        
        mfn_candidate_codes = ["TP_A_0150", "HS_A_0040"]
        mfn_indicator = None
        
        for code in mfn_candidate_codes:
            for ind in tariff_indicators:
                if isinstance(ind, dict) and ind.get('code') == code:
                    mfn_indicator = code
                    logger.info(f"Selected MFN indicator: {mfn_indicator} - {ind.get('name')}")
                    break
            if mfn_indicator:
                break
        
        # If we still couldn't find a specific indicator, try a more general approach
        if not mfn_indicator:
            for ind in tariff_indicators:
                if not isinstance(ind, dict):
                    continue
                    
                code = ind.get('code', '')
                name = ind.get('name', '')
                if code.startswith('TP_A_') and 'MFN' in name and 'applied' in name.lower():
                    mfn_indicator = code
                    logger.info(f"Selected alternative MFN indicator: {mfn_indicator} - {name}")
                    break
        
        # If we still couldn't find an indicator, select the first MFN-related one
        if not mfn_indicator:
            for ind in tariff_indicators:
                if not isinstance(ind, dict):
                    continue
                    
                code = ind.get('code', '')
                name = ind.get('name', '')
                if 'MFN' in name:
                    mfn_indicator = code
                    logger.info(f"Selected fallback MFN indicator: {mfn_indicator} - {name}")
                    break
        
        # If still no indicator, use the first available one as last resort
        if not mfn_indicator and tariff_indicators and isinstance(tariff_indicators[0], dict):
            mfn_indicator = tariff_indicators[0].get('code')
            logger.warning(f"Using first available indicator as fallback: {mfn_indicator}")
        
        # For trade data, we need to search for trade balance indicators
        # Since none are visible in the tariff indicators, let's search all indicators
        
        logger.info("Searching for trade balance indicators...")
        all_indicators = self.get_indicators()
        
        trade_balance_indicator = None
        for ind in all_indicators:
            if not isinstance(ind, dict):
                continue
                
            name = ind.get('name', '').lower()
            if 'trade balance' in name or ('trade' in name and 'balance' in name):
                trade_balance_indicator = ind.get('code')
                logger.info(f"Found trade balance indicator: {trade_balance_indicator} - {ind.get('name')}")
                break
                
        # If we couldn't find a trade balance indicator, look for trade-related ones
        if not trade_balance_indicator:
            for ind in all_indicators:
                if not isinstance(ind, dict):
                    continue
                    
                name = ind.get('name', '').lower()
                if 'merchandise trade' in name:
                    trade_balance_indicator = ind.get('code')
                    logger.info(f"Found merchandise trade indicator: {trade_balance_indicator} - {ind.get('name')}")
                    break
        
        # If we couldn't find any indicators, raise an error
        if not mfn_indicator:
            available_codes = [ind.get('code') for ind in tariff_indicators if isinstance(ind, dict)]
            raise Exception(f"Could not find appropriate MFN tariff indicator. Available indicators: {available_codes}")
            
        if not trade_balance_indicator:
            logger.warning("Could not find appropriate trade balance indicator. Creating profiles with tariff data only.")
            
        # Create dataframes to hold the data
        df_tariffs = None
        df_trade = None
        
        # Fetch tariff data
        logger.info(f"Fetching tariff data with indicator {mfn_indicator}")
        try:
            tariff_data = self.get_tariff_data(mfn_indicator, countries, years)
            df_tariffs = self.transform_to_dataframe(tariff_data)
            
            # Log sample of data for debugging
            if not df_tariffs.empty:
                logger.debug(f"Sample tariff data (first 3 rows):\n{df_tariffs.head(3)}")
            else:
                logger.warning("Tariff dataframe is empty after transformation")
        except Exception as e:
            logger.error(f"Error fetching tariff data: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame if we can't get tariff data
        
        # Fetch trade balance data if we found an indicator
        if trade_balance_indicator:
            logger.info(f"Fetching trade data with indicator {trade_balance_indicator}")
            try:
                trade_data = self.get_tariff_data(trade_balance_indicator, countries, years)
                df_trade = self.transform_to_dataframe(trade_data)
                
                # Log sample of data for debugging
                if not df_trade.empty:
                    logger.debug(f"Sample trade data (first 3 rows):\n{df_trade.head(3)}")
                else:
                    logger.warning("Trade dataframe is empty after transformation")
            except Exception as e:
                logger.warning(f"Failed to fetch trade data: {str(e)}")
                df_trade = None
        
        # If we don't have trade data, create a simple tariff profile
        if df_tariffs is not None and (df_trade is None or df_trade.empty):
            logger.info("Creating tariff-only profile")
            if 'value' in df_tariffs.columns:
                df_tariffs = df_tariffs.rename(columns={'value': 'tariff_value', 'unit': 'tariff_unit'})
            return df_tariffs
        
        # If we have both datasets, merge them
        if df_tariffs is not None and df_trade is not None and not df_tariffs.empty and not df_trade.empty:
            # Identify the main dataframe columns for merging
            merge_cols = ['country_code', 'country_name', 'year']
            
            # Rename columns to avoid conflicts
            if 'value' in df_tariffs.columns:
                df_tariffs = df_tariffs.rename(columns={'value': 'tariff_value', 'unit': 'tariff_unit'})
            if 'value' in df_trade.columns:
                df_trade = df_trade.rename(columns={'value': 'trade_balance', 'unit': 'trade_unit'})
            
            # Ensure the merge columns exist in both dataframes
            for col in merge_cols:
                if col not in df_tariffs.columns:
                    logger.warning(f"Column {col} missing from tariff data")
                if col not in df_trade.columns:
                    logger.warning(f"Column {col} missing from trade data")
            
            # Get actual columns that exist in both
            actual_merge_cols = [col for col in merge_cols if col in df_tariffs.columns and col in df_trade.columns]
            
            if not actual_merge_cols:
                logger.warning("No common columns for merging, returning tariff data only")
                return df_tariffs
            
            # Merge the dataframes
            logger.info(f"Merging datasets on columns: {actual_merge_cols}")
            try:
                cols_to_keep = ['trade_balance', 'trade_unit'] + actual_merge_cols
                cols_in_trade = [col for col in cols_to_keep if col in df_trade.columns]
                
                df_merged = pd.merge(
                    df_tariffs,
                    df_trade[cols_in_trade],
                    on=actual_merge_cols,
                    how='outer'
                )
                
                logger.info(f"Fetched tariff profiles with {len(df_merged)} data points")
                return df_merged
            except Exception as e:
                logger.error(f"Error merging dataframes: {str(e)}")
                return df_tariffs  # Return just tariff data if merge fails
        
        # If we only have tariff data, return it
        if df_tariffs is not None and not df_tariffs.empty:
            return df_tariffs
            
        # If something went wrong and we have no data, return empty DataFrame
        logger.error("Failed to fetch any usable data")
        return pd.DataFrame() 
    

    def run_full_extraction(self, countries: List[str] = None, 
                           years: Union[List[int], str] = "2018-2022") -> Dict[str, Any]:
        """
        Run a full data extraction process and save results.
        
        Args:
            countries: List of country codes (default: top trading partners)
            years: Years to include (default: 2018-2022)
            
        Returns:
            Dictionary with file paths and summary information
        """
        start_time = datetime.now()
        logger.info(f"Starting full data extraction at {start_time}")
        
        # Use default countries if none specified (top trading partners)
        if not countries:
            # Use ISO 3-letter codes - WTO doesn't use USA, CHN, etc.
            countries = [
                "840", "156", "484", "124", "392", "276", "704", "410", 
                "356", "826", "250", "380", "528", "076", "158", "372", 
                "756", "458", "764", "702", "643", "056", "360", "724", 
                "036", "376", "032"
            ]
        
        test_mode = False  # Set to False for production
        if test_mode:
            test_countries = countries[:3]  # Just use first 3 countries
            test_years = "2022"  # Just one year
            logger.info(f"TESTING MODE: Using {len(test_countries)} countries and years: {test_years}")
        else:
            test_countries = countries
            test_years = years
        
        result = {
            "timestamp": start_time.isoformat(),
            "files": {},
            "summary": {
                "countries": len(test_countries),
                "time_period": test_years if isinstance(test_years, str) else f"{min(test_years)}-{max(test_years)}",
                "test_mode": test_mode
            }
        }
        
        try:
            # 1. Fetch all indicators first to understand what's available
            logger.info("Fetching all available indicators...")
            all_indicators = self.get_indicators()
            all_indicators_file = self.save_to_json(all_indicators, "all_indicators")
            result["files"]["all_indicators"] = all_indicators_file
            result["summary"]["all_indicator_count"] = len(all_indicators)
            
            # Print summary of available indicators by category
            categories = {}
            for ind in all_indicators:
                cat = ind.get('categoryCode', 'Unknown')
                if cat not in categories:
                    categories[cat] = 0
                categories[cat] += 1
                
            logger.info("Indicator categories summary:")
            for cat, count in categories.items():
                logger.info(f"  - {cat}: {count} indicators")
            
            # 2. Fetch tariff-related indicators
            logger.info("Fetching tariff-related indicators...")
            tariff_indicators = self.get_tariff_indicators()
            indicator_file = self.save_to_json(tariff_indicators, "tariff_indicators")
            result["files"]["indicators"] = indicator_file
            result["summary"]["indicator_count"] = len(tariff_indicators)
            
            # Log all available tariff indicators for debugging
            logger.info("Available tariff indicators:")
            for ind in tariff_indicators:
                if isinstance(ind, dict):  # Make sure ind is a dictionary
                    logger.info(f"  - {ind.get('code')}: {ind.get('name')}")
                else:
                    logger.warning(f"Unexpected indicator format: {ind}")
            
            # 3. Fetch reporters (countries) to get correct codes
            logger.info("Fetching country information...")
            reporters = self.get_reporters()
            reporter_file = self.save_to_json(reporters, "reporters")
            result["files"]["reporters"] = reporter_file
            result["summary"]["reporter_count"] = len(reporters)
            
            # Print the first few reporters to understand format
            logger.info("First 5 reporters for format reference:")
            for i, reporter in enumerate(reporters[:5]):
                if isinstance(reporter, dict):
                    logger.info(f"  - {reporter.get('code')}: {reporter.get('name')}")
                else:
                    logger.warning(f"Unexpected reporter format: {reporter}")
            
            # Collect all reporter codes for reference
            all_reporter_codes = []
            for reporter in reporters:
                if isinstance(reporter, dict) and 'code' in reporter:
                    all_reporter_codes.append(reporter.get('code'))
            
            # Map country codes to ensure we're using valid ones
            valid_countries = []
            for country_code in test_countries:
                # Check if this country code exists in the reporters
                if country_code in all_reporter_codes:
                    valid_countries.append(country_code)
                else:
                    logger.warning(f"Country code {country_code} not found in WTO reporters, skipping")
            
            # If no valid countries, use the first 3 from reporters
            if not valid_countries and reporters and len(reporters) > 3:
                for i in range(min(3, len(reporters))):
                    if isinstance(reporters[i], dict) and 'code' in reporters[i]:
                        valid_countries.append(reporters[i].get('code'))
                logger.info(f"No valid countries found, using first 3 from reporters: {valid_countries}")
            
            logger.info(f"Using {len(valid_countries)} valid country codes: {valid_countries}")
                
            # 4. Fetch country tariff profiles only if we have valid countries
            if valid_countries:
                logger.info(f"Fetching tariff profiles for {len(valid_countries)} countries...")
                profiles = self.fetch_country_tariff_profiles(valid_countries, test_years)
                
                # Save in both formats
                csv_file = self.save_to_csv(profiles, "tariff_profiles")
                json_file = self.save_to_json(profiles, "tariff_profiles")
                
                result["files"]["tariff_profiles_csv"] = csv_file
                result["files"]["tariff_profiles_json"] = json_file
                result["summary"]["data_points"] = len(profiles)
                
                # 5. Save a "latest" version for dashboard use
                latest_file = os.path.join(self.output_dir, "tariff_data_latest.json")
                profiles_dict = json.loads(profiles.to_json(orient='records', date_format='iso'))
                
                with open(latest_file, 'w') as f:
                    json.dump({
                        "timestamp": datetime.now().isoformat(),
                        "data": profiles_dict,
                        "metadata": {
                            "countries": valid_countries,
                            "period": test_years if isinstance(test_years, str) else f"{min(test_years)}-{max(test_years)}",
                            "source": "WTO Timeseries API",
                            "test_mode": test_mode
                        }
                    }, f, indent=2)
                
                result["files"]["latest"] = latest_file
            else:
                logger.warning("No valid countries found, skipping tariff profile extraction")
                result["summary"]["data_points"] = 0
            
            # Calculate execution time
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            result["summary"]["execution_time_seconds"] = duration
            result["summary"]["status"] = "success"
            
            logger.info(f"Full extraction completed in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in full extraction: {str(e)}")
            result["summary"]["status"] = "error"
            result["summary"]["error"] = str(e)
            
            # Include exception traceback for debugging
            import traceback
            result["summary"]["traceback"] = traceback.format_exc()
        
        # Save the execution report
        report_file = os.path.join(self.output_dir, f"extraction_report_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        return result
    
def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="WTO Timeseries API Scraper")
    
    parser.add_argument(
        "--api-key", 
        help="WTO API subscription key (can also use WTO_API_KEY env variable)"
    )
    
    parser.add_argument(
        "--countries", 
        help="Comma-separated list of country codes (e.g., 840,156,484)"
    )
    
    parser.add_argument(
        "--years", 
        help="Years to fetch data for (e.g., 2020,2021,2022 or 2018-2022)"
    )
    
    parser.add_argument(
        "--output-dir", 
        default="data",
        help="Directory to save output files (default: data)"
    )
    
    parser.add_argument(
        "--timeout", 
        type=int,
        default=60,
        help="API request timeout in seconds (default: 60)"
    )
    
    parser.add_argument(
        "--retries", 
        type=int,
        default=3,
        help="Number of retry attempts for failed requests (default: 3)"
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int,
        default=5,
        help="Number of countries to process in a single request (default: 5)"
    )
    
    parser.add_argument(
        "--log-level", 
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()

def main():
    """Main entry point for the scraper"""
    args = parse_arguments()
    
    # Set log level
    logger.setLevel(getattr(logging, args.log_level))
    
    # Get API key from args or environment
    api_key = args.api_key or os.environ.get('WTO_API_KEY')
    
    if not api_key:
        logger.warning("No WTO API key provided. Public access may have rate limitations.")
    
    # Parse countries if provided
    countries = args.countries.split(',') if args.countries else None
    
    # Parse years if provided
    if args.years:
        if '-' in args.years:
            years = args.years  # Year range (e.g., "2018-2022")
        else:
            years = [int(y) for y in args.years.split(',')]  # List of years
    else:
        years = "2018-2022"  # Default to slightly older data that's more likely to be available
    
    try:
        # Initialize the API client
        client = WTOTimeseriesAPI(
            api_key=api_key,
            retry_attempts=args.retries,
            timeout=args.timeout,
            output_dir=args.output_dir,
            batch_size=args.batch_size
        )
        
        # Run the extraction
        result = client.run_full_extraction(countries, years)
        
        # Print summary
        if result["summary"]["status"] == "success":
            print("\nExtraction completed successfully:")
            print(f"- Processed {result['summary']['countries']} countries")
            print(f"- Time period: {result['summary']['time_period']}")
            print(f"- Data points: {result['summary'].get('data_points', 0)}")
            print(f"- Execution time: {result['summary']['execution_time_seconds']:.2f} seconds")
            print("\nOutput files:")
            for file_type, file_path in result["files"].items():
                print(f"- {file_type}: {file_path}")
        else:
            print("\nExtraction failed:")
            print(f"- Error: {result['summary'].get('error', 'Unknown error')}")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"\nError: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()