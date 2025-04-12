#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BEA API Scraper

This module provides a production-ready implementation for fetching economic data
from the Bureau of Economic Analysis (BEA) API for the economic dashboard project.
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
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bea_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("bea_scraper")

class BEAAPIClient:
    """
    Client for the BEA API, providing methods for fetching
    economic data with proper error handling and rate limiting.
    """
    
    BASE_URL = "https://apps.bea.gov/api/data"
    
    def __init__(self, api_key: str = None, retry_attempts: int = 3, 
                 timeout: int = 60, output_dir: str = "data"):
        """
        Initialize the BEA API client.
        
        Args:
            api_key: BEA API key (required)
            retry_attempts: Number of retry attempts for failed requests
            timeout: Request timeout in seconds
            output_dir: Directory to save output files
        """
        self.api_key = api_key or os.environ.get('BEA_API_KEY')
        if not self.api_key:
            logger.error("No BEA API key provided. Set BEA_API_KEY environment variable or pass as parameter.")
            raise ValueError("BEA API key is required")
        
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
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a request to the BEA API with proper error handling.
        
        Args:
            params: Query parameters
            
        Returns:
            JSON response from API
            
        Raises:
            Exception: If API request fails
        """
        # Ensure API key is included in parameters
        params['UserID'] = self.api_key
        
        # Create cache key based on params
        cache_key = json.dumps(params, sort_keys=True)
        
        # Return cached result if available
        if cache_key in self._cache:
            logger.debug(f"Using cached result for params: {params}")
            return self._cache[cache_key]
        
        try:
            logger.debug(f"Making request to {self.BASE_URL} with params: {params}")
            response = self.session.get(
                self.BASE_URL, 
                params=params,
                timeout=self.timeout
            )
            
            # Log response metadata
            logger.debug(f"Response status: {response.status_code}, content-type: {response.headers.get('content-type')}")
            
            # Handle response based on status code
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Check for API errors in response
                    if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Error' in data['BEAAPI']['Results']:
                        error = data['BEAAPI']['Results']['Error']
                        error_code = error.get('APIErrorCode')
                        error_desc = error.get('APIErrorDescription')
                        logger.error(f"BEA API error: {error_code} - {error_desc}")
                        raise Exception(f"BEA API error: {error_code} - {error_desc}")
                    
                    # Cache successful responses
                    self._cache[cache_key] = data
                    return data
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON response. Raw content (first 500 chars): {response.text[:500]}")
                    raise Exception("Invalid JSON response from API")
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 3600))
                logger.warning(f"Rate limit exceeded. Retry after {retry_after} seconds.")
                raise Exception(f"Rate limit exceeded. Retry after {retry_after} seconds.")
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text[:500]}")
                raise Exception(f"API request failed: {response.status_code} - {response.text[:500]}")
                
        except requests.exceptions.Timeout:
            logger.error(f"Request to {self.BASE_URL} timed out after {self.timeout} seconds")
            raise Exception(f"Request timed out after {self.timeout} seconds")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error while connecting to {self.BASE_URL}")
            raise Exception("Connection error - please check your network connection")
        except Exception as e:
            logger.error(f"Unexpected error in API request: {str(e)}")
            raise
    
    def get_dataset_list(self) -> List[Dict[str, Any]]:
        """
        Get a list of available datasets from the BEA API.
        
        Returns:
            List of dataset objects
        """
        params = {
            'method': 'GETDATASETLIST',
            'ResultFormat': 'JSON'
        }
        
        response = self._make_request(params)
        
        # Extract dataset list from response
        if 'BEAAPI' in response and 'Results' in response['BEAAPI'] and 'Dataset' in response['BEAAPI']['Results']:
            datasets = response['BEAAPI']['Results']['Dataset']
            logger.info(f"Found {len(datasets)} available datasets")
            return datasets
        else:
            logger.warning("No datasets found in response")
            return []
    
    def get_parameter_list(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Get the list of parameters for a specific dataset.
        
        Args:
            dataset_name: The name of the dataset
            
        Returns:
            List of parameter objects
        """
        params = {
            'method': 'GETPARAMETERLIST',
            'ResultFormat': 'JSON',
            'DatasetName': dataset_name
        }
        
        response = self._make_request(params)
        
        # Extract parameter list from response
        if 'BEAAPI' in response and 'Results' in response['BEAAPI'] and 'Parameter' in response['BEAAPI']['Results']:
            parameters = response['BEAAPI']['Results']['Parameter']
            logger.info(f"Found {len(parameters)} parameters for dataset {dataset_name}")
            return parameters
        else:
            logger.warning(f"No parameters found for dataset {dataset_name}")
            return []
    
    def get_parameter_values(self, dataset_name: str, parameter_name: str) -> List[Dict[str, Any]]:
        """
        Get the list of valid values for a specific parameter.
        
        Args:
            dataset_name: The name of the dataset
            parameter_name: The name of the parameter
            
        Returns:
            List of parameter value objects
        """
        params = {
            'method': 'GETPARAMETERVALUES',
            'ResultFormat': 'JSON',
            'DatasetName': dataset_name,
            'ParameterName': parameter_name
        }
        
        response = self._make_request(params)
        
        # Extract parameter values from response
        if 'BEAAPI' in response and 'Results' in response['BEAAPI'] and 'ParamValue' in response['BEAAPI']['Results']:
            param_values = response['BEAAPI']['Results']['ParamValue']
            logger.info(f"Found {len(param_values)} values for parameter {parameter_name} in dataset {dataset_name}")
            return param_values
        else:
            logger.warning(f"No values found for parameter {parameter_name} in dataset {dataset_name}")
            return []
    
    def get_parameter_values_filtered(self, dataset_name: str, target_parameter: str, **filter_params) -> List[Dict[str, Any]]:
        """
        Get the list of valid values for a specific parameter, filtered by other parameters.
        
        Args:
            dataset_name: The name of the dataset
            target_parameter: The name of the parameter to get values for
            **filter_params: Additional parameters to filter by
            
        Returns:
            List of parameter value objects
        """
        params = {
            'method': 'GETPARAMETERVALUESFILTERED',
            'ResultFormat': 'JSON',
            'DatasetName': dataset_name,
            'TargetParameter': target_parameter,
            **filter_params
        }
        
        response = self._make_request(params)
        
        # Extract parameter values from response
        if 'BEAAPI' in response and 'Results' in response['BEAAPI'] and 'ParamValue' in response['BEAAPI']['Results']:
            param_values = response['BEAAPI']['Results']['ParamValue']
            logger.info(f"Found {len(param_values)} filtered values for parameter {target_parameter} in dataset {dataset_name}")
            return param_values
        else:
            logger.warning(f"No filtered values found for parameter {target_parameter} in dataset {dataset_name}")
            return []
    
    def get_data(self, dataset_name: str, **params) -> Dict[str, Any]:
        """
        Get data from a specific dataset with the given parameters.
        
        Args:
            dataset_name: The name of the dataset
            **params: Additional parameters specific to the dataset
            
        Returns:
            Data response from the API
        """
        request_params = {
            'method': 'GETDATA',
            'ResultFormat': 'JSON',
            'DatasetName': dataset_name,
            **params
        }
        
        response = self._make_request(request_params)
        
        # Check if response contains data
        if 'BEAAPI' in response and 'Results' in response['BEAAPI'] and 'Data' in response['BEAAPI']['Results']:
            data = response['BEAAPI']['Results']['Data']
            logger.info(f"Retrieved {len(data)} data points from dataset {dataset_name}")
            return response['BEAAPI']['Results']
        else:
            logger.warning(f"No data found in response for dataset {dataset_name}")
            return {}
    
    def get_regional_data(self, table_name: str, line_code: Union[int, str], geo_fips: str, 
                         years: Union[str, List[str]] = "LAST5") -> Dict[str, Any]:
        """
        Get regional economic data from the BEA API.
        
        Args:
            table_name: The name of the table
            line_code: The line code for the statistic
            geo_fips: Geographic area code
            years: Year(s) to retrieve data for
            
        Returns:
            Regional data response
        """
        # Convert years to string format if it's a list
        if isinstance(years, list):
            years = ','.join(map(str, years))
            
        params = {
            'TableName': table_name,
            'LineCode': line_code,
            'GeoFips': geo_fips,
            'Year': years
        }
        
        return self.get_data('Regional', **params)
    
    def get_nipa_data(self, table_name: str, frequency: str, years: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get National Income and Product Accounts (NIPA) data from the BEA API.
        
        Args:
            table_name: The name of the table
            frequency: Frequency of the data (A for annual, Q for quarterly, M for monthly)
            years: Year(s) to retrieve data for
            
        Returns:
            NIPA data response
        """
        # Convert years to string format if it's a list
        if isinstance(years, list):
            years = ','.join(map(str, years))
            
        params = {
            'TableName': table_name,
            'Frequency': frequency,
            'Year': years
        }
        
        return self.get_data('NIPA', **params)
    
    def get_gdp_by_industry_data(self, table_id: Union[int, str], frequency: str, year: Union[str, List[str]], 
                               industry: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get GDP by industry data from the BEA API.
        
        Args:
            table_id: The ID of the table
            frequency: Frequency of the data (A for annual, Q for quarterly)
            year: Year(s) to retrieve data for
            industry: Industry code(s)
            
        Returns:
            GDP by industry data response
        """
        # Convert parameters to string format if they're lists
        if isinstance(year, list):
            year = ','.join(map(str, year))
        if isinstance(industry, list):
            industry = ','.join(map(str, industry))
            
        params = {
            'TableID': table_id,
            'Frequency': frequency,
            'Year': year,
            'Industry': industry
        }
        
        return self.get_data('GDPbyIndustry', **params)
    
    def transform_to_dataframe(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform API response data to a pandas DataFrame.
        
        Args:
            data: API response data
            
        Returns:
            Pandas DataFrame with structured data
        """
        # Check if we have data to transform
        if not data or 'Data' not in data:
            logger.warning("No data to transform to DataFrame")
            return pd.DataFrame()
        
        data_points = data['Data']
        
        # Create a list to hold the transformed data
        transformed_data = []
        
        # Extract dimensions for column names
        dimensions = {}
        if 'Dimensions' in data:
            for dim in data['Dimensions']:
                dimensions[dim['Name']] = {
                    'Ordinal': dim.get('Ordinal'),
                    'DataType': dim.get('DataType'),
                    'IsValue': dim.get('IsValue')
                }
        
        # Process each data point
        for point in data_points:
            transformed_point = {}
            for key, value in point.items():
                # Skip NoteRef attributes
                if key == 'NoteRef':
                    continue
                transformed_point[key] = value
            
            transformed_data.append(transformed_point)
        
        # Create DataFrame
        df = pd.DataFrame(transformed_data)
        
        # Add metadata
        if df.empty:
            return df
        
        # Add any notes as metadata
        notes = {}
        if 'Notes' in data:
            for note in data['Notes']:
                ref = note.get('NoteRef')
                text = note.get('NoteText')
                if ref and text:
                    notes[ref] = text
        
        # Add metadata as DataFrame attributes
        df.attrs['notes'] = notes
        df.attrs['dimensions'] = dimensions
        for key, value in data.items():
            if key not in ['Data', 'Notes', 'Dimensions']:
                df.attrs[key] = value
        
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
                # Extract DataFrame attributes to include in JSON
                df_dict = {
                    'data': json.loads(data.to_json(orient='records', date_format='iso')),
                    'metadata': {
                        'notes': data.attrs.get('notes', {}),
                        'dimensions': data.attrs.get('dimensions', {}),
                        **{k: v for k, v in data.attrs.items() if k not in ['notes', 'dimensions']}
                    }
                }
                json.dump(df_dict, f, indent=2)
            else:
                json.dump(data, f, indent=2)
        
        logger.info(f"Data saved to {filepath}")
        
        return filepath
    
    def get_gdp_data(self) -> pd.DataFrame:
        """
        Get GDP data from the BEA API.
        
        Returns:
            DataFrame with GDP data
        """
        try:
            # Try different parameters for GDP data
            # Using 'CAINC1' from Regional dataset for GDP, which we know works
            gdp_data = self.get_regional_data('SAGDP1', '1', 'STATE', 'LAST5')
            
            # Transform to DataFrame
            df = self.transform_to_dataframe(gdp_data)
            
            # Save data
            if not df.empty:
                self.save_to_csv(df, "gdp_data")
                self.save_to_json(df, "gdp_data")
                
            return df
        except Exception as e:
            logger.error(f"Error fetching GDP data: {str(e)}")
            return pd.DataFrame()
    
    def get_personal_income_data(self) -> pd.DataFrame:
        """
        Get personal income data from the BEA API.
        
        Returns:
            DataFrame with personal income data
        """
        # Get state-level personal income data
        income_data = self.get_regional_data('SAINC1', '3', 'STATE', 'LAST5')
        
        # Transform to DataFrame
        df = self.transform_to_dataframe(income_data)
        
        # Save data
        if not df.empty:
            self.save_to_csv(df, "personal_income_data")
            self.save_to_json(df, "personal_income_data")
            
        return df
    
    def get_state_gdp_data(self) -> pd.DataFrame:
        """
        Get state GDP data from the BEA API.
        
        Returns:
            DataFrame with state GDP data
        """
        try:
            # Use SAGDP2 table with line code 1 (All industry total)
            state_gdp_data = self.get_regional_data('SAGDP2', '1', 'STATE', 'LAST5')
            
            # Transform to DataFrame
            df = self.transform_to_dataframe(state_gdp_data)
            
            # Save data
            if not df.empty:
                self.save_to_csv(df, "state_gdp_data")
                self.save_to_json(df, "state_gdp_data")
                
            return df
        except Exception as e:
            logger.error(f"Error fetching state GDP data: {str(e)}")
            return pd.DataFrame()
    
    def run_full_extraction(self) -> Dict[str, Any]:
        """
        Run a full data extraction from the BEA API.
        
        Returns:
            Dictionary with file paths and summary information
        """
        start_time = datetime.now()
        logger.info(f"Starting full BEA data extraction at {start_time}")
        
        result = {
            "timestamp": start_time.isoformat(),
            "files": {},
            "summary": {}
        }
        
        try:
            # 1. Get list of available datasets
            datasets = self.get_dataset_list()
            datasets_file = self.save_to_json(datasets, "bea_datasets")
            result["files"]["datasets"] = datasets_file
            result["summary"]["dataset_count"] = len(datasets)
            
            # 2. Get GDP data
            logger.info("Fetching GDP data...")
            gdp_df = self.get_gdp_data()
            if not gdp_df.empty:
                result["files"]["gdp_data_csv"] = os.path.join(self.output_dir, f"gdp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                result["files"]["gdp_data_json"] = os.path.join(self.output_dir, f"gdp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                result["summary"]["gdp_data_points"] = len(gdp_df)
            
            # 3. Get personal income data
            logger.info("Fetching personal income data...")
            income_df = self.get_personal_income_data()
            if not income_df.empty:
                result["files"]["income_data_csv"] = os.path.join(self.output_dir, f"personal_income_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                result["files"]["income_data_json"] = os.path.join(self.output_dir, f"personal_income_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                result["summary"]["income_data_points"] = len(income_df)
            
            # 4. Get state GDP data
            logger.info("Fetching state GDP data...")
            state_gdp_df = self.get_state_gdp_data()
            if not state_gdp_df.empty:
                result["files"]["state_gdp_data_csv"] = os.path.join(self.output_dir, f"state_gdp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                result["files"]["state_gdp_data_json"] = os.path.join(self.output_dir, f"state_gdp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                result["summary"]["state_gdp_data_points"] = len(state_gdp_df)
            
            # 5. Save a "latest" version for dashboard use
            latest_file = os.path.join(self.output_dir, "bea_data_latest.json")
            
            with open(latest_file, 'w') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "gdp_data": json.loads(gdp_df.to_json(orient='records', date_format='iso')) if not gdp_df.empty else [],
                    "personal_income_data": json.loads(income_df.to_json(orient='records', date_format='iso')) if not income_df.empty else [],
                    "state_gdp_data": json.loads(state_gdp_df.to_json(orient='records', date_format='iso')) if not state_gdp_df.empty else [],
                    "metadata": {
                        "source": "Bureau of Economic Analysis API",
                        "extraction_date": datetime.now().isoformat()
                    }
                }, f, indent=2)
            
            result["files"]["latest"] = latest_file
            
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
        report_file = os.path.join(self.output_dir, f"bea_extraction_report_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        return result

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="BEA API Scraper")
    
    parser.add_argument(
        "--api-key", 
        help="BEA API key (can also use BEA_API_KEY env variable)"
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
    api_key = args.api_key or os.environ.get('BEA_API_KEY')
    
    if not api_key:
        logger.error("No BEA API key provided. Use --api-key or set BEA_API_KEY environment variable.")
        sys.exit(1)
    
    try:
        # Initialize the API client
        client = BEAAPIClient(
            api_key=api_key,
            retry_attempts=args.retries,
            timeout=args.timeout,
            output_dir=args.output_dir
        )
        # Check available tables for the Regional dataset
        tables = client.get_parameter_values('Regional', 'TableName')
        print(tables)

        # Check available line codes for a specific table
        line_codes = client.get_parameter_values_filtered('Regional', 'LineCode', TableName='SAGDP2N')
        print(line_codes)
        # Run the extraction
        result = client.run_full_extraction()
        
        # Print summary
        if result["summary"]["status"] == "success":
            print("\nExtraction completed successfully:")
            print(f"- GDP data points: {result['summary'].get('gdp_data_points', 0)}")
            print(f"- Personal income data points: {result['summary'].get('income_data_points', 0)}")
            print(f"- State GDP data points: {result['summary'].get('state_gdp_data_points', 0)}")
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