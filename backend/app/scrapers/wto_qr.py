#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WTO Quantitative Restrictions (QR) API Scraper

This module provides a production-ready implementation for fetching data on
import and export prohibitions and restrictions from the WTO QR API.
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
        logging.FileHandler("wto_qr_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("wto_qr_scraper")

class WTOQuantitativeRestrictionsAPI:
    """
    Client for the WTO Quantitative Restrictions (QR) API, providing methods 
    for fetching data on import and export prohibitions and restrictions.
    """
    
    BASE_URL = "https://api.wto.org/qrs"
    
    def __init__(self, api_key: str = None, retry_attempts: int = 3, 
                 timeout: int = 30, output_dir: str = "data"):
        """
        Initialize the WTO QR API client.
        
        Args:
            api_key: WTO API subscription key
            retry_attempts: Number of retry attempts for failed requests
            timeout: Request timeout in seconds
            output_dir: Directory to save output files
        """
        self.api_key = api_key or os.environ.get('WTO_API_KEY')
        if not self.api_key:
            logger.warning("No WTO API key provided. Set WTO_API_KEY environment variable.")
        
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
            allowed_methods=["GET", "POST"]  # In newer versions, method_whitelist was renamed to allowed_methods
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make a request to the WTO QR API with proper error handling.
        
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
            logger.debug(f"Making request to {url} with params: {params}")
            response = self.session.get(
                url, 
                params=params, 
                headers=headers,
                timeout=self.timeout
            )
            
            # Handle response based on status code
            if response.status_code == 200:
                data = response.json()
                # Cache successful responses
                self._cache[cache_key] = data
                return data
            elif response.status_code == 401:
                logger.error("Authentication failed - invalid or missing API key")
                raise Exception(f"API authentication failed: {response.text}. Please check your WTO API subscription key.")
            elif response.status_code == 422:
                logger.error(f"Validation error: {response.text}")
                raise Exception(f"API validation error: {response.text}. Please check your request parameters.")
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded. Waiting before retry.")
                time.sleep(10)  # Wait before retrying
                return self._make_request(endpoint, params)  # Retry recursively
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
    
    def get_hs_versions(self) -> List[Dict[str, Any]]:
        """
        Get list of HS versions.
        
        Returns:
            List of HS version objects
        """
        response = self._make_request('hs-versions')
        
        # Check for the expected data structure
        if isinstance(response, dict) and "data" in response:
            return response["data"]
        
        logger.warning(f"Unexpected response format from hs-versions endpoint: {response}")
        return []
    
    def get_members(self, member_code: str = None, name: str = None, page: int = 1) -> Dict[str, Any]:
        """
        Get list of WTO members.
        
        Args:
            member_code: Optional member code filter
            name: Optional name filter
            page: Page number for pagination
            
        Returns:
            Member data with pagination information
        """
        params = {"page": page}
        
        if member_code:
            params["member_code"] = member_code
            
        if name:
            params["name"] = name
        
        return self._make_request('members', params)
    
    def get_notifications(self, reporter_member_code: str = None, notification_year: int = None, page: int = 1) -> Dict[str, Any]:
        """
        Get list of notifications.
        
        Args:
            reporter_member_code: Optional member code filter
            notification_year: Optional year filter
            page: Page number for pagination
            
        Returns:
            Notification data with pagination information
        """
        params = {"page": page}
        
        if reporter_member_code:
            params["reporter_member_code"] = reporter_member_code
            
        if notification_year:
            params["notification_year"] = notification_year
        
        return self._make_request('notifications', params)
    
    def get_products(self, hs_version: str, code: str = None, description: str = None, page: int = 1) -> Dict[str, Any]:
        """
        Get list of products.
        
        Args:
            hs_version: HS version (e.g., "h6" for HS-2017)
            code: Optional product code filter
            description: Optional description filter
            page: Page number for pagination
            
        Returns:
            Product data with pagination information
        """
        params = {
            "hs_version": hs_version,
            "page": page
        }
        
        if code:
            params["code"] = code
            
        if description:
            params["description"] = description
        
        return self._make_request('products', params)
    
    def get_qr_list(self, reporter_member_code: str = None, in_force_only: bool = None, 
                  year_of_entry_into_force: int = None, product_codes: str = None, 
                  product_ids: str = None, page: int = 1) -> Dict[str, Any]:
        """
        Get list of quantitative restrictions.
        
        Args:
            reporter_member_code: Optional member code filter
            in_force_only: If true, only QRs currently in force will be returned
            year_of_entry_into_force: Optional year filter
            product_codes: Comma separated list of product codes
            product_ids: Comma separated list of product ids
            page: Page number for pagination
            
        Returns:
            QR data with pagination information
        """
        params = {"page": page}
        
        if reporter_member_code:
            params["reporter_member_code"] = reporter_member_code
            
        if in_force_only is not None:
            params["in_force_only"] = "true" if in_force_only else "false"
            
        if year_of_entry_into_force:
            params["year_of_entry_into_force"] = year_of_entry_into_force
            
        if product_codes:
            params["product_codes"] = product_codes
            
        if product_ids:
            params["product_ids"] = product_ids
        
        return self._make_request('qrs', params)
    
    def get_qr_details(self, qr_id: int) -> Dict[str, Any]:
        """
        Get details of a specific quantitative restriction.
        
        Args:
            qr_id: ID of the QR
            
        Returns:
            Detailed QR data
        """
        return self._make_request(f'qrs/{qr_id}')
    
    def transform_qr_to_dataframe(self, qr_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform QR data to a pandas DataFrame.
        
        Args:
            qr_data: QR data from the API
            
        Returns:
            Pandas DataFrame with structured QR data
        """
        if not qr_data or not isinstance(qr_data, dict) or "data" not in qr_data:
            logger.warning("Invalid QR data format")
            return pd.DataFrame()
        
        qrs = qr_data["data"]
        
        if not qrs or not isinstance(qrs, list):
            logger.warning("No QR data to transform")
            return pd.DataFrame()
        
        # Create a list to hold the transformed data
        transformed_data = []
        
        for qr in qrs:
            try:
                # Skip non-dictionary items
                if not isinstance(qr, dict):
                    logger.warning(f"Skipping non-dictionary QR data: {qr}")
                    continue
                    
                # Extract basic QR information
                qr_info = {
                    "id": qr.get("id"),
                    "in_force_from": qr.get("in_force_from"),
                    "termination_dt": qr.get("termination_dt"),
                    "general_description": qr.get("general_description"),
                    "national_legal_bases": qr.get("national_legal_bases"),
                    "administrative_mechanisms": qr.get("administrative_mechanisms"),
                }
                
                # Extract reporter member info
                reporter_member = qr.get("reporter_member", {})
                if isinstance(reporter_member, dict):
                    qr_info["reporter_code"] = reporter_member.get("code")
                    name_obj = reporter_member.get("name", {})
                    if isinstance(name_obj, dict):
                        qr_info["reporter_name_en"] = name_obj.get("en")
                
                # Extract restriction types
                restrictions = qr.get("restrictions", [])
                if isinstance(restrictions, list):
                    qr_info["restrictions"] = ", ".join(restrictions)
                
                # Extract measures information
                measures = qr.get("measures", [])
                if isinstance(measures, list):
                    for i, measure in enumerate(measures):
                        if isinstance(measure, dict):
                            qr_info[f"measure_{i+1}_flow"] = measure.get("flow")
                            qr_info[f"measure_{i+1}_symbol"] = measure.get("symbol")
                            qr_info[f"measure_{i+1}_group"] = measure.get("group_name")
                            
                            description = measure.get("description", {})
                            if isinstance(description, dict):
                                qr_info[f"measure_{i+1}_description_en"] = description.get("en")
                
                # Extract notification information
                notifications = qr.get("notified_in", [])
                if isinstance(notifications, list):
                    for i, notification in enumerate(notifications):
                        if isinstance(notification, dict):
                            qr_info[f"notification_{i+1}_date"] = notification.get("notification_dt")
                            qr_info[f"notification_{i+1}_document"] = notification.get("document_symbol")
                            qr_info[f"notification_{i+1}_url"] = notification.get("document_url")
                
                transformed_data.append(qr_info)
            except Exception as e:
                logger.warning(f"Error processing QR {qr.get('id', 'unknown')}: {str(e)}")
                continue
        
        # Convert to DataFrame
        if transformed_data:
            df = pd.DataFrame(transformed_data)
            logger.info(f"Transformed {len(transformed_data)} QR records to DataFrame")
            return df
        else:
            logger.warning("No QR data was successfully transformed")
            return pd.DataFrame()
    
    def transform_qr_detail_to_dataframe(self, qr_detail: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Transform detailed QR data to multiple pandas DataFrames.
        
        Args:
            qr_detail: Detailed QR data from the API
            
        Returns:
            Dictionary of DataFrames (main, measures, products, notifications)
        """
        if not qr_detail or not isinstance(qr_detail, dict) or "data" not in qr_detail:
            logger.warning("Invalid QR detail format")
            return {
                "main": pd.DataFrame(),
                "measures": pd.DataFrame(),
                "products": pd.DataFrame(),
                "notifications": pd.DataFrame()
            }
        
        qr = qr_detail["data"]
        
        # Main QR information
        main_data = {
            "id": qr.get("id"),
            "in_force_from": qr.get("in_force_from"),
            "termination_dt": qr.get("termination_dt"),
            "reporter_code": qr.get("reporter_member", {}).get("code"),
            "reporter_name_en": qr.get("reporter_member", {}).get("name", {}).get("en"),
            "general_description": qr.get("general_description"),
            "national_legal_bases": qr.get("national_legal_bases"),
            "administrative_mechanisms": qr.get("administrative_mechanisms"),
            "restrictions": ", ".join(qr.get("restrictions", []))
        }
        main_df = pd.DataFrame([main_data])
        
        # Measures data
        measures_data = []
        for measure in qr.get("measures", []):
            measure_data = {
                "qr_id": qr.get("id"),
                "flow": measure.get("flow"),
                "symbol": measure.get("symbol"),
                "group_name": measure.get("group_name"),
                "description_en": measure.get("description", {}).get("en"),
                "description_fr": measure.get("description", {}).get("fr"),
                "description_es": measure.get("description", {}).get("es"),
                "interpreted": measure.get("interpreted"),
                "mast_codes": ", ".join(measure.get("mast_codes", []))
            }
            measures_data.append(measure_data)
        measures_df = pd.DataFrame(measures_data) if measures_data else pd.DataFrame()
        
        # Products data
        products_data = []
        for product in qr.get("affected_products", []):
            product_data = {
                "qr_id": qr.get("id"),
                "product_id": product.get("id"),
                "product_code": product.get("code"),
                "description_en": product.get("description", {}).get("en"),
                "description_fr": product.get("description", {}).get("fr"),
                "description_es": product.get("description", {}).get("es"),
                "hs_version": product.get("hs_version")
            }
            products_data.append(product_data)
        products_df = pd.DataFrame(products_data) if products_data else pd.DataFrame()
        
        # Notifications data
        notifications_data = []
        for notification in qr.get("notified_in", []):
            notification_data = {
                "qr_id": qr.get("id"),
                "notification_date": notification.get("notification_dt"),
                "document_symbol": notification.get("document_symbol"),
                "document_url": notification.get("document_url"),
                "original_language": notification.get("original_language"),
                "type": notification.get("type"),
                "covered_periods": ", ".join(notification.get("covered_periods", []))
            }
            notifications_data.append(notification_data)
        notifications_df = pd.DataFrame(notifications_data) if notifications_data else pd.DataFrame()
        
        return {
            "main": main_df,
            "measures": measures_df,
            "products": products_df,
            "notifications": notifications_df
        }
    
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
    
    def fetch_in_force_restrictions(self, countries: List[str] = None) -> pd.DataFrame:
        """
        Fetch currently in-force quantitative restrictions, optionally filtered by countries.
        
        Args:
            countries: Optional list of country codes
            
        Returns:
            DataFrame with QR data
        """
        all_qrs = []
        
        if countries:
            # Fetch QRs for each country
            for country_code in countries:
                logger.info(f"Fetching in-force restrictions for country {country_code}")
                
                try:
                    response = self.get_qr_list(
                        reporter_member_code=country_code,
                        in_force_only=True,
                        page=1
                    )
                    
                    # Process first page
                    if isinstance(response, dict) and "data" in response and isinstance(response["data"], list):
                        logger.info(f"Found {len(response['data'])} QRs for country {country_code} (page 1)")
                        all_qrs.extend(response["data"])
                    
                        # Check if there are more pages
                        if "meta" in response and isinstance(response["meta"], dict):
                            meta = response["meta"]
                            current_page = meta.get("current_page", 1)
                            last_page = meta.get("last_page", 1)
                            
                            # Fetch remaining pages
                            for page in range(current_page + 1, last_page + 1):
                                logger.info(f"Fetching page {page} for country {country_code}")
                                
                                try:
                                    page_response = self.get_qr_list(
                                        reporter_member_code=country_code,
                                        in_force_only=True,
                                        page=page
                                    )
                                    
                                    if isinstance(page_response, dict) and "data" in page_response and isinstance(page_response["data"], list):
                                        logger.info(f"Found {len(page_response['data'])} QRs for country {country_code} (page {page})")
                                        all_qrs.extend(page_response["data"])
                                    else:
                                        logger.warning(f"Unexpected format in page {page} response for country {country_code}")
                                except Exception as e:
                                    logger.error(f"Error fetching page {page} for country {country_code}: {str(e)}")
                    else:
                        logger.warning(f"Unexpected format in initial response for country {country_code}")
                except Exception as e:
                    logger.error(f"Error fetching QRs for country {country_code}: {str(e)}")
        else:
            # Fetch all in-force QRs
            logger.info("Fetching all in-force restrictions")
            
            try:
                response = self.get_qr_list(in_force_only=True, page=1)
                
                # Process first page
                if isinstance(response, dict) and "data" in response and isinstance(response["data"], list):
                    logger.info(f"Found {len(response['data'])} QRs (page 1)")
                    all_qrs.extend(response["data"])
                
                    # Check if there are more pages
                    if "meta" in response and isinstance(response["meta"], dict):
                        meta = response["meta"]
                        current_page = meta.get("current_page", 1)
                        last_page = meta.get("last_page", 1)
                        
                        # Fetch remaining pages
                        for page in range(current_page + 1, last_page + 1):
                            logger.info(f"Fetching page {page}")
                            
                            try:
                                page_response = self.get_qr_list(in_force_only=True, page=page)
                                
                                if isinstance(page_response, dict) and "data" in page_response and isinstance(page_response["data"], list):
                                    logger.info(f"Found {len(page_response['data'])} QRs (page {page})")
                                    all_qrs.extend(page_response["data"])
                                else:
                                    logger.warning(f"Unexpected format in page {page} response")
                            except Exception as e:
                                logger.error(f"Error fetching page {page}: {str(e)}")
                else:
                    logger.warning("Unexpected format in initial response")
            except Exception as e:
                logger.error(f"Error fetching all QRs: {str(e)}")
        
        # Create a dataframe from all QRs
        logger.info(f"Processing {len(all_qrs)} total QR records")
        if all_qrs:
            return self.transform_qr_to_dataframe({"data": all_qrs})
        
        return pd.DataFrame()
    
    def fetch_qr_details_for_products(self, product_codes: List[str], hs_version: str) -> pd.DataFrame:
        """
        Fetch QRs affecting specific products.
        
        Args:
            product_codes: List of product codes
            hs_version: HS version code
            
        Returns:
            DataFrame with QR data
        """
        logger.info(f"Fetching restrictions for products {product_codes} (HS version: {hs_version})")
        
        # Prepare product IDs
        product_ids = [f"{hs_version}-{code}" for code in product_codes]
        product_ids_str = ",".join(product_ids)
        
        # Fetch QRs
        response = self.get_qr_list(product_ids=product_ids_str, page=1)
        
        all_qrs = []
        
        # Process first page
        if isinstance(response, dict) and "data" in response:
            all_qrs.extend(response["data"])
        
        # Check if there are more pages
        if isinstance(response, dict) and "meta" in response:
            meta = response["meta"]
            current_page = meta.get("current_page", 1)
            last_page = meta.get("last_page", 1)
            
            # Fetch remaining pages
            for page in range(current_page + 1, last_page + 1):
                logger.info(f"Fetching page {page}")
                page_response = self.get_qr_list(product_ids=product_ids_str, page=page)
                
                if isinstance(page_response, dict) and "data" in page_response:
                    all_qrs.extend(page_response["data"])
        
        # Create a dataframe from all QRs
        if all_qrs:
            return self.transform_qr_to_dataframe({"data": all_qrs})
        
        return pd.DataFrame()
    
    def run_qr_extraction(self, countries: List[str] = None, products: List[str] = None, 
                         hs_version: str = "h6", in_force_only: bool = True) -> Dict[str, Any]:
        """
        Run a comprehensive extraction of QR data based on specified filters.
        
        Args:
            countries: Optional list of country codes
            products: Optional list of product codes
            hs_version: HS version code
            in_force_only: Whether to include only in-force QRs
            
        Returns:
            Dictionary with file paths and summary information
        """
        start_time = datetime.now()
        logger.info(f"Starting QR data extraction at {start_time}")
        
        result = {
            "timestamp": start_time.isoformat(),
            "files": {},
            "summary": {
                "countries": len(countries) if countries else "all",
                "products": len(products) if products else "all",
                "hs_version": hs_version,
                "in_force_only": in_force_only
            }
        }
        
        try:
            # 1. Fetch HS versions first
            logger.info("Fetching HS versions")
            hs_versions = self.get_hs_versions()
            hs_versions_file = self.save_to_json(hs_versions, "hs_versions")
            result["files"]["hs_versions"] = hs_versions_file
            result["summary"]["hs_versions_count"] = len(hs_versions)
            
            # 2. Fetch countries (members)
            logger.info("Fetching WTO members")
            members_response = self.get_members()
            members = members_response.get("data", [])
            members_file = self.save_to_json(members, "wto_members")
            result["files"]["members"] = members_file
            result["summary"]["members_count"] = len(members)
            
            # Make sure we have valid country codes
            valid_countries = []
            if countries:
                valid_member_codes = [m.get("code") for m in members if isinstance(m, dict)]
                valid_countries = [c for c in countries if c in valid_member_codes]
                
                if len(valid_countries) < len(countries):
                    logger.warning(f"Some country codes are invalid: {set(countries) - set(valid_countries)}")
            
            # 3. Fetch QRs based on filters
            qr_df = None
            
            if products and len(products) > 0:
                # Fetch QRs by product codes
                logger.info(f"Fetching QRs for products: {products}")
                qr_df = self.fetch_qr_details_for_products(products, hs_version)
                
                # If also filtering by country, apply additional filter
                if valid_countries and len(valid_countries) > 0:
                    qr_df = qr_df[qr_df["reporter_code"].isin(valid_countries)]
            else:
                # Fetch QRs by country
                logger.info(f"Fetching QRs for countries: {valid_countries or 'all'}")
                qr_df = self.fetch_in_force_restrictions(valid_countries)
            
            # Save QR data
            if qr_df is not None and not qr_df.empty:
                csv_file = self.save_to_csv(qr_df, "quantitative_restrictions")
                json_file = self.save_to_json(qr_df, "quantitative_restrictions")
                
                result["files"]["qr_csv"] = csv_file
                result["files"]["qr_json"] = json_file
                result["summary"]["qr_count"] = len(qr_df)
                
                # 4. Save a "latest" version for dashboard use
                latest_file = os.path.join(self.output_dir, "qr_data_latest.json")
                qr_dict = json.loads(qr_df.to_json(orient='records', date_format='iso'))
                
                with open(latest_file, 'w') as f:
                    json.dump({
                        "timestamp": datetime.now().isoformat(),
                        "data": qr_dict,
                        "metadata": {
                            "countries": valid_countries if valid_countries else "all",
                            "products": products if products else "all",
                            "hs_version": hs_version,
                            "in_force_only": in_force_only,
                            "source": "WTO Quantitative Restrictions API"
                        }
                    }, f, indent=2)
                
                result["files"]["latest"] = latest_file
            else:
                logger.warning("No QR data found matching the specified filters")
                result["summary"]["qr_count"] = 0
            
            # Calculate execution time
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            result["summary"]["execution_time_seconds"] = duration
            result["summary"]["status"] = "success"
            
            logger.info(f"QR extraction completed in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in QR extraction: {str(e)}")
            result["summary"]["status"] = "error"
            result["summary"]["error"] = str(e)
            
            # Include exception traceback for debugging
            import traceback
            result["summary"]["traceback"] = traceback.format_exc()
        
        # Save the execution report
        report_file = os.path.join(self.output_dir, f"qr_extraction_report_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        return result

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="WTO Quantitative Restrictions API Scraper")
    
    parser.add_argument(
        "--api-key", 
        help="WTO API subscription key (can also use WTO_API_KEY env variable)"
    )
    
    parser.add_argument(
        "--countries", 
        help="Comma-separated list of WTO member codes (e.g., C840,C156,C484)"
    )
    
    parser.add_argument(
        "--products", 
        help="Comma-separated list of product codes (e.g., 010110,010121)"
    )
    
    parser.add_argument(
        "--hs-version", 
        default="h6",
        help="HS version code (default: h6 for HS-2017)"
    )
    
    parser.add_argument(
        "--in-force-only", 
        action="store_true",
        help="Only include QRs currently in force"
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
        logger.error("No WTO API key provided. Use --api-key or set WTO_API_KEY environment variable.")
        sys.exit(1)
    
    # Parse countries if provided
    countries = args.countries.split(',') if args.countries else None
    
    # Parse products if provided
    products = args.products.split(',') if args.products else None
    
    try:
        # Initialize the API client
        client = WTOQuantitativeRestrictionsAPI(
            api_key=api_key,
            retry_attempts=args.retries,
            timeout=args.timeout,
            output_dir=args.output_dir
        )
        
        # Run the extraction
        result = client.run_qr_extraction(
            countries=countries,
            products=products,
            hs_version=args.hs_version,
            in_force_only=args.in_force_only
        )
        
        # Print summary
        if result["summary"]["status"] == "success":
            print("\nExtraction completed successfully:")
            print(f"- Countries: {result['summary']['countries']}")
            print(f"- Products: {result['summary']['products']}")
            print(f"- HS Version: {result['summary']['hs_version']}")
            print(f"- QRs found: {result['summary'].get('qr_count', 0)}")
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