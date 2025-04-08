import logging
import threading
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.services.scheduler import TariffScheduler
from app.services.tariff_pipeline import get_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("tariff_dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("tariff_dashboard")

pipeline = None
scheduler = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline, scheduler
    
    # Startup section
    logger.info("Initializing tariff data pipeline")
    pipeline = get_pipeline()
    
    dashboard_data = pipeline.get_dashboard_api_data()
    if not dashboard_data:
        logger.info("No dashboard data found, running initial data collection")
        # Run the initial pipeline in a background thread
        init_thread = threading.Thread(target=pipeline.run_full_pipeline)
        init_thread.daemon = True
        init_thread.start()
    
    logger.info("Starting tariff data scheduler")
    scheduler = TariffScheduler(pipeline)
    scheduler.start()
    
    yield
    
    if scheduler:
        logger.info("Stopping tariff data scheduler")
        scheduler.stop()
    
    logger.info("Shutdown complete")

app = FastAPI(
    title="Tariff Dashboard API",
    description="API for the tariff dashboard with real-time data",
    version="1.0.0",
    lifespan=lifespan  
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "tariff-dashboard-api"}
