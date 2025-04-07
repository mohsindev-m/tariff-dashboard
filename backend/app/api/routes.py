from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import logging

from ..services.tariff_service import TariffDataService

# Initialize router
router = APIRouter(prefix="/api", tags=["tariff"])

# Initialize logger
logger = logging.getLogger(__name__)

# Create a service instance (for simplicity, using a global instance)
tariff_service = TariffDataService()

@router.get("/dashboard", response_model=Dict[str, Any])
async def get_complete_dashboard(
    force_refresh: bool = Query(False, description="Force data refresh from sources")
):
    """
    Get complete dashboard data from all sources (Census, BEA, WTO).
    This endpoint combines trade balances, sector data, historical trends,
    and detailed metrics into a single comprehensive dataset.
    """
    try:
        data = await tariff_service.get_dashboard_data(force_refresh=force_refresh)
        return data
    except Exception as e:
        logger.error(f"Error retrieving dashboard data: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving dashboard data: {str(e)}")

@router.get("/map", response_model=List[Dict[str, Any]])
async def get_map_data():
    """
    Get global map data for tariff visualization.
    This data includes trade balance, effective tariff rates,
    and other metrics by region/country.
    """
    try:
        data = await tariff_service.get_global_map_data()
        return data
    except Exception as e:
        logger.error(f"Error retrieving map data: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving map data: {str(e)}")

@router.get("/sectors", response_model=List[Dict[str, Any]])
async def get_sector_data():
    """
    Get sector-specific tariff impact data.
    This data includes export values, tariff impacts, and 
    GDP contributions by economic sector.
    """
    try:
        data = await tariff_service.get_sector_impact_data()
        return data
    except Exception as e:
        logger.error(f"Error retrieving sector data: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving sector data: {str(e)}")

@router.get("/trends", response_model=List[Dict[str, Any]])
async def get_historical_trends():
    """
    Get historical trade trends.
    This data includes exports, imports, trade deficit,
    and balance over time.
    """
    try:
        data = await tariff_service.get_historical_trends()
        return data
    except Exception as e:
        logger.error(f"Error retrieving historical trends: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving historical trends: {str(e)}")

@router.get("/metrics", response_model=List[Dict[str, Any]])
async def get_detailed_metrics():
    """
    Get detailed metrics for tariff analysis.
    This data includes HS code-level exports, tariff rates,
    and supply chain risk metrics.
    """
    try:
        data = await tariff_service.get_detailed_metrics()
        return data
    except Exception as e:
        logger.error(f"Error retrieving detailed metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving detailed metrics: {str(e)}")

@router.post("/refresh", response_model=Dict[str, Any])
async def refresh_data(background_tasks: BackgroundTasks):
    """
    Trigger a background refresh of all tariff data.
    This endpoint initiates a complete refresh of data from all sources
    without blocking the response.
    """
    try:
        # Queue the refresh to happen in the background
        background_tasks.add_task(tariff_service.get_dashboard_data, force_refresh=True)
        return {"status": "success", "message": "Data refresh initiated"}
    except Exception as e:
        logger.error(f"Error initiating data refresh: {e}")
        raise HTTPException(status_code=500, detail=f"Error initiating data refresh: {str(e)}")