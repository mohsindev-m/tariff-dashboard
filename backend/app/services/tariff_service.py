import logging
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import json
import os

from ..scrapers.census import get_tariff_dashboard_data, get_latest_trade_year
from ..scrapers.bea_scrapper import get_gdp_by_industry, get_international_transactions
from ..scrapers.wto_scraper import fetch_tariff_data, fetch_indicators

logger = logging.getLogger(__name__)

# Define cache directory
CACHE_DIR = "data/processed"
os.makedirs(CACHE_DIR, exist_ok=True)

# Cache expiration time (in seconds)
CACHE_EXPIRATION = 86400  # 24 hours

class TariffDataService:
    """
    Service to integrate data from various sources (Census, BEA, WTO)
    and provide consolidated data for the tariff dashboard.
    """
    
    def __init__(self):
        """Initialize the tariff data service."""
        self.latest_data = None
        self.last_updated = None
    
    async def get_dashboard_data(self, force_refresh=False):
        """
        Get the complete dashboard data from all sources.
        
        Parameters:
            force_refresh (bool): Force a data refresh even if cache is valid
            
        Returns:
            dict: The complete dashboard dataset
        """
        cache_file = os.path.join(CACHE_DIR, "tariff_dashboard_complete.json")
        
        # Check if we have valid cached data
        if not force_refresh and os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                
                # Check if cache is still valid
                if 'timestamp' in cache_data:
                    cache_time = datetime.fromisoformat(cache_data['timestamp'])
                    if datetime.now() - cache_time < timedelta(seconds=CACHE_EXPIRATION):
                        logger.info("Using cached dashboard data")
                        return cache_data['data']
            except Exception as e:
                logger.error(f"Error reading cache file: {e}")
        
        # Fetch fresh data
        logger.info("Fetching fresh dashboard data from all sources")
        
        # Get latest trade year from Census
        trade_year = get_latest_trade_year()
        trade_month = "06"  # June is often used for mid-year data
        
        # Fetch data from different sources concurrently
        tasks = [
            self._get_census_data(trade_year, trade_month),
            self._get_bea_data(trade_year),
            self._get_wto_data(trade_year)
        ]
        
        census_data, bea_data, wto_data = await asyncio.gather(*tasks)
        
        # Combine all data
        dashboard_data = self._integrate_data(census_data, bea_data, wto_data)
        
        # Save to cache
        try:
            with open(cache_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'data': dashboard_data
                }, f, indent=2)
            logger.info(f"Saved dashboard data to cache: {cache_file}")
        except Exception as e:
            logger.error(f"Failed to save dashboard data to cache: {e}")
        
        # Update instance variables
        self.latest_data = dashboard_data
        self.last_updated = datetime.now()
        
        return dashboard_data
    
    async def _get_census_data(self, year, month):
        """Fetch data from Census API."""
        try:
            # Run in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            census_data = await loop.run_in_executor(
                None, 
                get_tariff_dashboard_data,
                year, month, True
            )
            logger.info(f"Successfully retrieved Census data for {year}-{month}")
            return census_data
        except Exception as e:
            logger.error(f"Error fetching Census data: {e}")
            return {}
    
    async def _get_bea_data(self, year):
        """Fetch data from BEA API."""
        try:
            # Run in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            
            # Get GDP by industry data 
            gdp_industry_task = loop.run_in_executor(
                None,
                get_gdp_by_industry,
                "1",      # Value Added by Industry
                "A",      # Annual
                f"LAST5", # Last 5 years
                "ALL"     # All industries
            )
            
            # Get international transactions data
            transactions_task = loop.run_in_executor(
                None,
                get_international_transactions,
                "BalGds",      # Balance on goods
                "AllCountries", # All countries
                "A",           # Annual
                "LAST5"        # Last 5 years
            )
            
            gdp_industry_data, transactions_data = await asyncio.gather(
                gdp_industry_task, transactions_task
            )
            
            if not gdp_industry_data or not transactions_data:
                logger.warning("Some BEA data could not be retrieved")
            
            return {
                "gdp_by_industry": gdp_industry_data,
                "international_transactions": transactions_data
            }
        except Exception as e:
            logger.error(f"Error fetching BEA data: {e}")
            return {}
    
    async def _get_wto_data(self, year):
        """Fetch data from WTO API."""
        try:
            # Run in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            
            # First get tariff indicators
            indicators_task = loop.run_in_executor(
                None,
                fetch_indicators,
                "all",     # All indicators
                "tariff"   # With "tariff" in name
            )
            
            indicators = await indicators_task
            
            if not indicators or len(indicators) == 0:
                logger.error("No tariff indicators found")
                return {}
            
            # Use the first applicable tariff indicator
            indicator_code = indicators[0]["code"]
            
            # Get tariff data
            tariff_data_task = loop.run_in_executor(
                None,
                fetch_tariff_data,
                indicator_code,
                "all",       # All economies
                "default",   # Default partner
                str(year),   # Year
                "default",   # Default product sector
                False,       # Don't include sub-sectors
                "json",      # JSON format
                "full",      # Full output mode
                "default",   # Default decimals
                0,           # No offset
                500,         # Maximum records
                "H",         # Human-readable headings
                1,           # Language: English
                False        # Don't include metadata
            )
            
            try:
                tariff_data = await tariff_data_task
                
                # If tariff_data is None or empty (e.g., 204 status), use a fallback
                if not tariff_data:
                    logger.warning("No tariff data returned, using a previous year as fallback")
                    # Try with previous year
                    fallback_year = str(int(year) - 1)
                    tariff_data_task = loop.run_in_executor(
                        None,
                        fetch_tariff_data,
                        indicator_code,
                        "all",
                        "default",
                        fallback_year,
                        "default",
                        False,
                        "json",
                        "full",
                        "default",
                        0,
                        500,
                        "H",
                        1,
                        False
                    )
                    tariff_data = await tariff_data_task
            except Exception as e:
                logger.error(f"Error parsing tariff data: {e}")
                tariff_data = None
            
            if not tariff_data:
                logger.error("Failed to fetch tariff data")
                return {"tariff_indicators": indicators}
            
            return {
                "tariff_indicators": indicators,
                "tariff_data": tariff_data
            }
        except Exception as e:
            logger.error(f"Error fetching WTO data: {e}")
            return {}
    
    def _integrate_data(self, census_data, bea_data, wto_data):
        """
        Integrate data from all sources into a unified dashboard dataset.
        """
        dashboard = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "data_sources": ["U.S. Census Bureau", "Bureau of Economic Analysis", "World Trade Organization"]
            },
            "global_map_data": [],
            "sector_impact_data": [],
            "historical_trends": [],
            "detailed_metrics": []
        }
        
        # 1. Process global map data (from census trade balance and WTO tariff rates)
        if census_data and 'trade_balance' in census_data:
            trade_balance = census_data['trade_balance']
            
            # Get tariff rates from WTO if available
            tariff_rates = {}
            if (wto_data and 'tariff_data' in wto_data and wto_data['tariff_data'] and 
                'Dataset' in wto_data['tariff_data']):
                for item in wto_data['tariff_data']['Dataset']:
                    country = item.get('ReportingEconomy', '')
                    value = item.get('Value', 0)
                    if country and value:
                        try:
                            tariff_rates[country] = float(value)
                        except (ValueError, TypeError):
                            pass
            
            # If no WTO data, use default values
            if not tariff_rates:
                logger.warning("No tariff rates available from WTO, using default values")
                # Sample default values for major economies
                tariff_rates = {
                    "United States": 3.4,
                    "China": 7.5,
                    "European Union": 5.1,
                    "Japan": 4.2,
                    "CANADA": 2.8,
                    "MEXICO": 5.9
                }
            
            # Combine trade balance with tariff rates
            for district in trade_balance:
                district_name = district.get('DIST_NAME', '')
                
                # Try to match district with a country for tariff rate
                tariff_rate = 0
                for country, rate in tariff_rates.items():
                    if country.lower() in district_name.lower():
                        tariff_rate = rate
                        break
                
                # Default value if no match
                if tariff_rate == 0:
                    tariff_rate = 3.5  # Global average tariff rate
                
                dashboard["global_map_data"].append({
                    "region": district_name,
                    "trade_deficit": district.get('trade_balance', 0),
                    "exports": district.get('exports_value', 0),
                    "imports": district.get('imports_value', 0),
                    "effective_tariff": tariff_rate,
                    "code": district.get('DISTRICT', '')
                })
        
        # 2. Process sector impact data (from Census sectors and BEA industry data)
        if census_data and 'sector_data' in census_data:
            sectors = census_data['sector_data']
            
            # Get GDP contribution by industry from BEA if available
            industry_gdp = {}
            if (bea_data and 'gdp_by_industry' in bea_data and 
                bea_data['gdp_by_industry'] and 'BEAAPI' in bea_data['gdp_by_industry']):
                
                bea_results = bea_data['gdp_by_industry']['BEAAPI'].get('Results', {})
                if 'Data' in bea_results:
                    for item in bea_results['Data']:
                        industry = item.get('Industry', '')
                        value = item.get('DataValue', 0)
                        if industry and value:
                            try:
                                industry_gdp[industry] = float(value)
                            except (ValueError, TypeError):
                                pass
            
            # Map tariff data to sectors if available
            sector_tariffs = {}
            if wto_data and 'tariff_data' in wto_data:
                # Implement mapping logic based on WTO tariff data
                pass
            
            # Calculate sector-specific impacts
            for sector in sectors:
                sector_name = sector.get('SECTOR', '')
                export_value = sector.get('ALL_VAL_MO', 0)
                percentage = sector.get('PERCENTAGE', 0)
                
                # Add to dashboard
                dashboard["sector_impact_data"].append({
                    "sector": sector_name,
                    "export_value": export_value,
                    "percentage": percentage,
                    "tariff_impact": sector_tariffs.get(sector_name, 0),
                    "gdp_contribution": industry_gdp.get(sector_name, 0),
                    "jobs_impact": 0  # Would require additional data source
                })
        
        # 3. Process historical trends (from Census time series and BEA data)
        if census_data and 'time_series' in census_data:
            time_series = census_data['time_series']
            
            # Get BEA international transaction history if available
            bea_transactions = {}
            if (bea_data and 'international_transactions' in bea_data and 
                bea_data['international_transactions'] and 'BEAAPI' in bea_data['international_transactions']):
                
                bea_results = bea_data['international_transactions']['BEAAPI'].get('Results', {})
                if 'Data' in bea_results:
                    for item in bea_results['Data']:
                        year = item.get('Year', '')
                        value = item.get('DataValue', 0)
                        if year and value:
                            try:
                                bea_transactions[year] = float(value)
                            except (ValueError, TypeError):
                                pass
            
            # Combine census time series with BEA data
            for year_data in time_series:
                year = year_data.get('YEAR', '')
                
                dashboard["historical_trends"].append({
                    "year": year,
                    "exports": year_data.get('EXPORTS', 0),
                    "imports": year_data.get('IMPORTS', 0),
                    "trade_deficit": year_data.get('DEFICIT', 0),
                    "bea_balance": bea_transactions.get(year, 0)
                })
        
        # 4. Process detailed metrics (combination of all data sources)
        if census_data and 'hs_data' in census_data:
            hs_data = census_data['hs_data']
            
            for item in hs_data:
                hs_code = item.get('HS_CHAPTER', '')
                description = item.get('DESCRIPTION', '')
                
                dashboard["detailed_metrics"].append({
                    "hs_code": hs_code,
                    "description": description,
                    "export_value": item.get('ALL_VAL_MO', 0),
                    "export_year": item.get('ALL_VAL_YR', 0),
                    "tariff_rate": 0,  # Would need to be matched from WTO data
                    "supply_chain_risk": 0  # Would require additional calculation
                })
        
        return dashboard
    
    async def get_global_map_data(self):
        """Get only the global map data for the dashboard."""
        full_data = await self.get_dashboard_data()
        return full_data.get("global_map_data", [])
    
    async def get_sector_impact_data(self):
        """Get only the sector impact data for the dashboard."""
        full_data = await self.get_dashboard_data()
        return full_data.get("sector_impact_data", [])
    
    async def get_historical_trends(self):
        """Get only the historical trends data for the dashboard."""
        full_data = await self.get_dashboard_data()
        return full_data.get("historical_trends", [])
    
    async def get_detailed_metrics(self):
        """Get only the detailed metrics data for the dashboard."""
        full_data = await self.get_dashboard_data()
        return full_data.get("detailed_metrics", [])