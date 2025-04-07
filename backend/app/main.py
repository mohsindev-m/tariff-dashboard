from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager
import asyncio

from .api.routes import router as api_router
from .services.tariff_service import TariffDataService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Global service instance
tariff_service = TariffDataService()

# Background scheduler for periodic data refresh
scheduler = AsyncIOScheduler()

# Lifespan event handler for startup/shutdown tasks
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize data and scheduler
    logger.info("Starting up tariff dashboard API")
    
    # Initial data load
    await tariff_service.get_dashboard_data(force_refresh=True)
    
    # Schedule periodic refresh (every 6 hours)
    scheduler.add_job(
        tariff_service.get_dashboard_data,
        'interval', 
        hours=6,
        kwargs={"force_refresh": True},
        id='refresh_tariff_data'
    )
    scheduler.start()
    
    # Yield control back to FastAPI
    yield
    
    # Shutdown: Clean up resources
    logger.info("Shutting down tariff dashboard API")
    scheduler.shutdown()

# Create FastAPI app
app = FastAPI(
    title="Tariff Dashboard API",
    description="API for analyzing tariff impacts across sectors and regions",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router)

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Tariff Dashboard API is running",
        "docs": "/docs",
        "version": "1.0.0"
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "last_data_refresh": tariff_service.last_updated.isoformat() if tariff_service.last_updated else None
    }