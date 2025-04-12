#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Census API Scraper for Tariff Dashboard

This module provides a production-ready implementation for fetching trade data
from the U.S. Census Bureau API to enrich the tariff dashboard with trade deficit
information and other economic indicators.
"""

import os
import sys
import json
import time
import logging
import argparse
import requests
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("census_api_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("census_api_scraper")

class CensusAPIClient:
    """
    Client for the U.S. Census Bureau API, providing methods for fetching
    trade deficit data, import/export statistics, and other economic indicators
    relevant to the tariff dashboard.
    """
    
    BASE_URL = "https://api.census.gov/data"
    
    def __init__(self, api_key: str = None, retry_attempts: int = 3, 
                 timeout: int = 30, output_dir: str = "data"):
        """
        Initialize the Census API client.
        
        Args:
            api_key: Census API key
            retry_attempts: Number of retry attempts for failed requests
            timeout: Request timeout in seconds
            output_dir: Directory to save output files
        """
        self.api_key = api_key or os.environ.get('CENSUS_API_KEY')
        if not self.api_key:
            logger.warning("No Census API key provided. Limited to 500 requests per day.")
        
        self.timeout = timeout
        self.output_dir = output_dir
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
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=retry_attempts,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, url: str, params: Dict[str, Any] = None) -> List[List[str]]:
        """
        Make a request to the Census API with proper error handling.
        
        Args:
            url: Full URL for the API endpoint
            params: Query parameters
            
        Returns:
            JSON response from API
            
        Raises:
            Exception: If API request fails
        """
        params = params or {}
        
        # Add API key to parameters if available
        if self.api_key:
            params['key'] = self.api_key
        
        # Create cache key based on url and params
        cache_key = f"{url}:{json.dumps(params, sort_keys=True)}"
        
        # Return cached result if available
        if cache_key in self._cache:
            logger.debug(f"Using cached result for {url}")
            return self._cache[cache_key]
        
        try:
            logger.debug(f"Making request to {url} with params: {params}")
            response = self.session.get(
                url, 
                params=params,
                timeout=self.timeout
            )
            
            # Handle response based on status code
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Cache successful responses
                    self._cache[cache_key] = data
                    return data
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse JSON response from {url}")
                    raise Exception(f"Invalid JSON response from Census API: {response.text[:100]}...")
            elif response.status_code == 204:
                logger.warning(f"No content returned from {url}")
                return []
            elif response.status_code == 400:
                logger.error(f"Bad request to Census API: {response.text}")
                raise Exception(f"Bad request to Census API: {response.text}")
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded. Waiting before retry.")
                time.sleep(30)  # Wait 30 seconds before retrying
                return self._make_request(url, params)  # Retry recursively
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text}")
                raise Exception(f"API request failed: {response.status_code} - {response.text}")
                
        except requests.exceptions.Timeout:
            logger.error(f"Request to {url} timed out after {self.timeout} seconds")
            raise Exception(f"Request timed out after {self.timeout} seconds")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error while connecting to {url}")
            raise Exception("Connection error - please check your network connection")
        except Exception as e:
            logger.error(f"Unexpected error in API request: {str(e)}")
            raise
    
    def get_available_datasets(self) -> List[Dict[str, Any]]:
        """
        Get a list of available datasets from the Census API.
        
        Returns:
            List of dataset objects
        """
        url = f"{self.BASE_URL}.json"
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                datasets = response.json().get('dataset', [])
                logger.info(f"Found {len(datasets)} datasets")
                return datasets
            else:
                logger.error(f"Failed to fetch datasets: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error fetching datasets: {str(e)}")
            return []
    
    def diagnose_api_endpoint(self, endpoint_url: str) -> None:
        """
        Diagnose an API endpoint by fetching and displaying its available variables.
        Useful for debugging purposes when the API structure is unclear.
        
        Args:
            endpoint_url: The API endpoint URL to diagnose
        """
        try:
            # First, try to get variables
            variables_url = f"{endpoint_url}/variables.json"
            response = self.session.get(variables_url, timeout=self.timeout)
            
            if response.status_code == 200:
                variables = response.json().get('variables', {})
                logger.info(f"Available variables for {endpoint_url}:")
                for var_name, var_info in variables.items():
                    logger.info(f" - {var_name}: {var_info.get('label', 'No label')}")
            else:
                logger.warning(f"Could not fetch variables for {endpoint_url}: {response.status_code}")
            
            # Then, try to get example queries
            examples_url = f"{endpoint_url}/examples.html"
            response = self.session.get(examples_url, timeout=self.timeout)
            
            if response.status_code == 200:
                logger.info(f"Examples available at: {examples_url}")
            else:
                logger.warning(f"No examples available at {examples_url}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error diagnosing API endpoint {endpoint_url}: {str(e)}")
    
    def get_historical_trade_balance(self, 
                                  start_year: int = 2020, 
                                  end_year: int = None,
                                  countries: List[str] = None) -> pd.DataFrame:
        """
        Fetch historical trade balance data from the Census API.
        
        Args:
            start_year: Start year for historical data
            end_year: End year for historical data (defaults to current year)
            countries: List of country names to include
            
        Returns:
            DataFrame with historical trade balance data
        """
        # If end_year not specified, use current year
        if not end_year:
            end_year = datetime.now().year - 1  # Use previous year as current may not have data
        
        # Using the Foreign Trade API endpoint
        base_url = f"{self.BASE_URL}/timeseries/intltrade/imports/enduse"
        
        # Create empty DataFrame to hold all results
        all_data = pd.DataFrame()
        
        # We'll need to fetch year by year since the time range format is giving errors
        for year in range(start_year, end_year + 1):
            # Define parameters
            params = {}
            
            # Specify variables to retrieve - Using confirmed variables from the API
            params['get'] = 'YEAR,MONTH,CTY_CODE,CTY_NAME,GEN_VAL_MO'
            
            # Set time parameter for just one year
            params['time'] = str(year)
            
            try:
                # Make the API request
                data = self._make_request(base_url, params)
                
                if not data or len(data) <= 1:
                    logger.warning(f"No trade data returned for {year}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(data[1:], columns=data[0])
                
                # Filter by countries if provided
                if countries and 'CTY_NAME' in df.columns:
                    country_matches = []
                    for country_name in countries:
                        # Case-insensitive partial matching
                        matches = df[df['CTY_NAME'].str.lower().str.contains(country_name.lower(), na=False)]
                        if not matches.empty:
                            country_matches.append(matches)
                    
                    if country_matches:
                        df = pd.concat(country_matches)
                
                # Convert numeric columns
                if 'GEN_VAL_MO' in df.columns:
                    df['GEN_VAL_MO'] = pd.to_numeric(df['GEN_VAL_MO'], errors='coerce')
                
                # Append to the full dataset
                all_data = pd.concat([all_data, df])
                
                logger.info(f"Retrieved trade data for {year} with {len(df)} records")
                
            except Exception as e:
                logger.error(f"Error fetching trade data for {year}: {str(e)}")
        
        # If we have data
        if not all_data.empty:
            # Convert year and month to numeric
            if 'YEAR' in all_data.columns:
                all_data['YEAR'] = pd.to_numeric(all_data['YEAR'], errors='coerce')
            
            if 'MONTH' in all_data.columns:
                all_data['MONTH'] = pd.to_numeric(all_data['MONTH'], errors='coerce')
            
            logger.info(f"Retrieved total of {len(all_data)} trade data records from {start_year} to {end_year}")
            return all_data
        
        logger.warning(f"No trade data retrieved for years {start_year} to {end_year}")
        return pd.DataFrame()
    
    def get_monthly_trade_by_country(self, 
                                   year: int = None, 
                                   month: int = None,
                                   top_n: int = 20) -> pd.DataFrame:
        """
        Fetch monthly trade data by country from the Census API.
        
        Args:
            year: Year to fetch data for (defaults to most recent)
            month: Month to fetch data for (defaults to most recent)
            top_n: Number of top trading partners to include
            
        Returns:
            DataFrame with monthly trade data by country
        """
        # If year/month not specified, use most recent complete month
        if not year or not month:
            today = datetime.now()
            # Use previous month as current month may not be complete
            prev_month = today.replace(day=1) - timedelta(days=1)
            year = year or prev_month.year
            month = month or prev_month.month
        
        # API might not have data for the current year, try the previous year if needed
        if year >= datetime.now().year:
            year = datetime.now().year - 1
        
        # Using the International Trade API endpoint
        base_url = f"{self.BASE_URL}/timeseries/intltrade/imports/enduse"
        
        # Define parameters
        params = {}
        
        # Specify variables to retrieve - Using confirmed variables from the API
        params['get'] = 'YEAR,MONTH,CTY_CODE,CTY_NAME,GEN_VAL_MO'
        
        # Format time parameter for API - Use just the year to avoid format errors
        params['time'] = str(year)
            
        try:
            # Make the API request
            data = self._make_request(base_url, params)
            
            if not data or len(data) <= 1:
                logger.warning(f"No trade data returned for {year}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            # Convert numeric columns
            if 'GEN_VAL_MO' in df.columns:
                df['GEN_VAL_MO'] = pd.to_numeric(df['GEN_VAL_MO'], errors='coerce')
            
            if 'MONTH' in df.columns:
                df['MONTH'] = pd.to_numeric(df['MONTH'], errors='coerce')
                
                # Filter to the requested month
                df = df[df['MONTH'] == month]
            
            # If we filtered everything out, return empty DataFrame
            if df.empty:
                logger.warning(f"No trade data for month {month} in {year}")
                return pd.DataFrame()
            
            # Sort by monthly value and get top N countries
            if 'GEN_VAL_MO' in df.columns:
                df = df.sort_values('GEN_VAL_MO', ascending=False)
                if top_n:
                    df = df.head(top_n)
            
            logger.info(f"Retrieved trade data for {len(df)} countries in {year}-{month}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching monthly trade data: {str(e)}")
            return pd.DataFrame()
    
    def get_state_data(self, 
                     year: int = None, 
                     states: List[str] = None) -> pd.DataFrame:
        """
        Fetch state business data from the Census API County Business Patterns.
        
        Args:
            year: Year to fetch data for (defaults to most recent available)
            states: List of state codes to include
            
        Returns:
            DataFrame with state business data
        """
        # Use 2022 data which we know is available from the diagnostic output
        year = 2022
        
        # Using the CBP endpoint that we confirmed is available
        base_url = f"{self.BASE_URL}/{year}/cbp"
        
        # Define parameters
        params = {}
        
        # Specify variables to retrieve based on available fields in the API
        params['get'] = 'NAME,NAICS2017,NAICS2017_LABEL,PAYANN,EMP'
        
        # Get state level data
        params['for'] = 'state:*'
        
        # Industry level - higher level groupings
        params['NAICS2017'] = '00,31-33,42,44-45,51,52,53,54,55,56,61,62,71,72,81'  # Major sectors
            
        try:
            # Make the API request
            data = self._make_request(base_url, params)
            
            if not data or len(data) <= 1:
                logger.warning(f"No state data returned for {year}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            # Filter states if provided
            if states and 'state' in df.columns:
                df = df[df['state'].isin(states)]
            
            # Convert numeric columns
            numeric_cols = ['PAYANN', 'EMP']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add year column
            df['YEAR'] = year
            
            logger.info(f"Retrieved state data for {len(df)} rows in {year}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching state data: {str(e)}")
            return pd.DataFrame()
    
    def transform_to_dashboard_format(self, 
                                    trade_data: pd.DataFrame, 
                                    data_type: str = 'trade_deficit') -> pd.DataFrame:
        """
        Transform Census API data into a format compatible with the tariff dashboard.
        
        Args:
            trade_data: DataFrame with trade data from Census API
            data_type: Type of trade data ('trade_deficit', 'exports', etc.)
            
        Returns:
            Transformed DataFrame ready for dashboard use
        """
        # Check if we have data to transform
        if trade_data.empty:
            logger.warning("No data to transform for dashboard")
            return pd.DataFrame()
        
        # Create a new DataFrame for dashboard format
        dashboard_df = pd.DataFrame()
        
        # Different transformations based on data type
        if data_type == 'trade_deficit':
            # For trade deficit data - Updated for new column names
            if all(col in trade_data.columns for col in ['CTY_NAME', 'GEN_VAL_MO', 'YEAR']):
                # Group by country and year to sum up monthly values
                grouped = trade_data.groupby(['CTY_NAME', 'YEAR'])['GEN_VAL_MO'].sum().reset_index()
                
                dashboard_df = pd.DataFrame({
                    'country': grouped['CTY_NAME'],
                    'year': grouped['YEAR'],
                    'trade_deficit': grouped['GEN_VAL_MO'],
                    'data_source': 'Census Bureau',
                    'unit': 'USD'
                })
        
        elif data_type == 'monthly_trade':
            # For monthly trade data - Updated for new column names
            if all(col in trade_data.columns for col in ['CTY_NAME', 'GEN_VAL_MO', 'YEAR', 'MONTH']):
                dashboard_df = pd.DataFrame({
                    'country': trade_data['CTY_NAME'],
                    'year': trade_data['YEAR'],
                    'month': trade_data['MONTH'],
                    'import_value': trade_data['GEN_VAL_MO'],
                    'data_source': 'Census Bureau',
                    'unit': 'USD'
                })
                
        elif data_type == 'state_data':
            # For state data - Updated for the new endpoint and variables
            if all(col in trade_data.columns for col in ['NAME', 'NAICS2017_LABEL', 'PAYANN', 'YEAR']):
                dashboard_df = pd.DataFrame({
                    'state': trade_data['NAME'],
                    'industry': trade_data['NAICS2017_LABEL'],
                    'year': trade_data['YEAR'],
                    'annual_payroll': trade_data['PAYANN'],
                    'employment': trade_data.get('EMP', 0),
                    'data_source': 'Census Bureau',
                    'unit': 'USD'
                })
        
        logger.info(f"Transformed {len(dashboard_df)} rows of data for dashboard ({data_type})")
        return dashboard_df
    
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
            data: Data to save (DataFrame or Python object)
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
    
    def run_full_extraction(self, 
                           years: List[int] = None, 
                           countries: List[str] = None,
                           save_output: bool = True) -> Dict[str, Any]:
        """
        Run a comprehensive data extraction from Census API for the tariff dashboard.
        
        Args:
            years: List of years to include in extraction
            countries: List of countries to focus on
            save_output: Whether to save output files
            
        Returns:
            Dictionary with DataFrames and metadata
        """
        start_time = datetime.now()
        logger.info(f"Starting full Census API extraction at {start_time}")
        
        # Diagnose API endpoints to help with debugging
        logger.info("Diagnosing API endpoints...")
        self.diagnose_api_endpoint(f"{self.BASE_URL}/timeseries/intltrade/imports/enduse")
        self.diagnose_api_endpoint(f"{self.BASE_URL}/2022/cbp")
        
        # Default to looking at 2020-2022 instead of more recent years that might not have data
        if not years:
            years = [2020, 2021, 2022]
        
        # Default to major trading partners if countries not specified
        if not countries:
            countries = [
                "China", "Mexico", "Canada", "Japan", "Germany", 
                "South Korea", "United Kingdom", "Vietnam", "Taiwan", "India"
            ]
        
        result = {
            "timestamp": start_time.isoformat(),
            "data": {},
            "files": {},
            "metadata": {
                "years": years,
                "countries": countries
            }
        }
        
        try:
            # 1. Get historical trade balance data for specified years
            logger.info(f"Fetching historical trade balance data for {len(countries)} countries from {min(years)} to {max(years)}")
            trade_balance_df = self.get_historical_trade_balance(
                start_year=min(years),
                end_year=max(years),
                countries=countries
            )
            
            if not trade_balance_df.empty:
                result["data"]["trade_balance"] = trade_balance_df
                
                # Transform for dashboard
                dashboard_trade_balance = self.transform_to_dashboard_format(
                    trade_balance_df, 
                    data_type='trade_deficit'
                )
                result["data"]["dashboard_trade_balance"] = dashboard_trade_balance
                
                # Save output files
                if save_output:
                    csv_file = self.save_to_csv(trade_balance_df, "census_trade_balance")
                    json_file = self.save_to_json(trade_balance_df, "census_trade_balance")
                    dashboard_file = self.save_to_json(dashboard_trade_balance, "dashboard_trade_balance")
                    
                    result["files"]["trade_balance_csv"] = csv_file
                    result["files"]["trade_balance_json"] = json_file
                    result["files"]["dashboard_trade_balance"] = dashboard_file
            
            # 2. Get monthly trade data for most recent available month
            logger.info("Fetching monthly trade data")
            monthly_trade_df = self.get_monthly_trade_by_country(
                year=max(years),  # Use the most recent year from our range
                month=12,  # December - likely to have data
                top_n=50  # Get top 50 trading partners
            )
            
            if not monthly_trade_df.empty:
                result["data"]["monthly_trade"] = monthly_trade_df
                
                # Transform for dashboard
                dashboard_monthly_trade = self.transform_to_dashboard_format(
                    monthly_trade_df, 
                    data_type='monthly_trade'
                )
                result["data"]["dashboard_monthly_trade"] = dashboard_monthly_trade
                
                # Save output files
                if save_output:
                    csv_file = self.save_to_csv(monthly_trade_df, "census_monthly_trade")
                    json_file = self.save_to_json(monthly_trade_df, "census_monthly_trade")
                    dashboard_file = self.save_to_json(dashboard_monthly_trade, "dashboard_monthly_trade")
                    
                    result["files"]["monthly_trade_csv"] = csv_file
                    result["files"]["monthly_trade_json"] = json_file
                    result["files"]["dashboard_monthly_trade"] = dashboard_file
            
            # 3. Get state business data (replacing state export data)
            logger.info("Fetching state business data")
            state_data_df = self.get_state_data()
            
            if not state_data_df.empty:
                result["data"]["state_data"] = state_data_df
                
                # Transform for dashboard
                dashboard_state_data = self.transform_to_dashboard_format(
                    state_data_df, 
                    data_type='state_data'
                )
                result["data"]["dashboard_state_data"] = dashboard_state_data
                
                # Save output files
                if save_output:
                    csv_file = self.save_to_csv(state_data_df, "census_state_data")
                    json_file = self.save_to_json(state_data_df, "census_state_data")
                    dashboard_file = self.save_to_json(dashboard_state_data, "dashboard_state_data")
                    
                    result["files"]["state_data_csv"] = csv_file
                    result["files"]["state_data_json"] = json_file
                    result["files"]["dashboard_state_data"] = dashboard_file
            
            # 4. Save combined dashboard data
            if save_output:
                all_dashboard_data = {
                    "timestamp": datetime.now().isoformat(),
                    "trade_balance": result["data"].get("dashboard_trade_balance", pd.DataFrame()).to_dict(orient='records'),
                    "monthly_trade": result["data"].get("dashboard_monthly_trade", pd.DataFrame()).to_dict(orient='records'),
                    "state_data": result["data"].get("dashboard_state_data", pd.DataFrame()).to_dict(orient='records')
                }
                
                dashboard_file = os.path.join(self.output_dir, "census_data_latest.json")
                with open(dashboard_file, 'w') as f:
                    json.dump(all_dashboard_data, f, indent=2)
                
                result["files"]["dashboard_latest"] = dashboard_file
            
            # Calculate execution time
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            result["metadata"]["execution_time_seconds"] = duration
            result["metadata"]["status"] = "success"
            
            logger.info(f"Full Census API extraction completed in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in Census API extraction: {str(e)}")
            result["metadata"]["status"] = "error"
            result["metadata"]["error"] = str(e)
            
            # Include exception traceback for debugging
            import traceback
            result["metadata"]["traceback"] = traceback.format_exc()
        
        # Save the execution report
        if save_output:
            report_file = os.path.join(self.output_dir, f"census_extraction_report_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
            
            # Convert DataFrames to lists for JSON serialization
            report_data = {
                "timestamp": result["timestamp"],
                "files": result["files"],
                "metadata": result["metadata"]
            }
            
            with open(report_file, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            result["files"]["extraction_report"] = report_file
        
        return result

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Census API Scraper for Tariff Dashboard")
    
    parser.add_argument(
        "--api-key", 
        help="Census API key (can also use CENSUS_API_KEY env variable)"
    )
    
    parser.add_argument(
        "--countries", 
        help="Comma-separated list of countries to focus on"
    )
    
    parser.add_argument(
        "--years", 
        help="Comma-separated list of years to include (e.g., 2019,2020,2021)"
    )
    
    parser.add_argument(
        "--output-dir", 
        default="data",
        help="Directory to save output files (default: data)"
    )
    
    parser.add_argument(
        "--timeout", 
        type=int,
        default=30,
        help="API request timeout in seconds (default: 30)"
    )
    
    parser.add_argument(
        "--retries", 
        type=int,
        default=3,
        help="Number of retry attempts for failed requests (default: 3)"
    )
    
    parser.add_argument(
        "--no-save", 
        action="store_true",
        help="Don't save output files, just return data"
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
    api_key = args.api_key or os.environ.get('CENSUS_API_KEY')
    
    # Parse countries if provided
    countries = args.countries.split(',') if args.countries else None
    
    # Parse years if provided
    years = [int(y) for y in args.years.split(',')] if args.years else None
    
    try:
        # Initialize the API client
        client = CensusAPIClient(
            api_key=api_key,
            retry_attempts=args.retries,
            timeout=args.timeout,
            output_dir=args.output_dir
        )
        
        # Run the extraction
        result = client.run_full_extraction(
            years=years,
            countries=countries,
            save_output=not args.no_save
        )
        
        # Print summary
        if result["metadata"]["status"] == "success":
            print("\nExtraction completed successfully:")
            if "trade_balance" in result["data"]:
                print(f"- Trade balance data points: {len(result['data']['trade_balance'])}")
            else:
                print("- Trade balance data points: 0")
                
            if "monthly_trade" in result["data"]:
                print(f"- Monthly trade data points: {len(result['data']['monthly_trade'])}")
            else:
                print("- Monthly trade data points: 0")
                
            if "state_data" in result["data"]:
                print(f"- State export data points: {len(result['data']['state_data'])}")
            else:
                print("- State export data points: 0")
                
            print(f"- Execution time: {result['metadata']['execution_time_seconds']:.2f} seconds")
            
            if not args.no_save:
                print("\nOutput files:")
                for file_type, file_path in result["files"].items():
                    print(f"- {file_type}: {file_path}")
        else:
            print("\nExtraction failed:")
            print(f"- Error: {result['metadata'].get('error', 'Unknown error')}")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"\nError: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()