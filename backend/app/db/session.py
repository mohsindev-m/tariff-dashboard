import motor.motor_asyncio
from ..core.config import settings
import os
import logging

logger = logging.getLogger(__name__)

# MongoDB connection (if you want to store results)
client = None
db = None

def connect_to_mongo():
    global client, db
    try:
        mongo_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
        client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
        db = client.tariff_dashboard
        logger.info("Connected to MongoDB")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

async def get_tariff_collection():
    if db is None:
        connect_to_mongo()
    return db.tariff_data if db else None