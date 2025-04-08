import os
from typing import List
from functools import lru_cache
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_V1_STR: str = "/api"
    PROJECT_NAME: str = "Tariff Dashboard API"
    
    # Database settings – note the DB_PATH is defined here.
    SQLALCHEMY_DATABASE_URI: str = os.getenv(
        "DATABASE_URL", 
        "sqlite:///./tariff_dashboard.db"
    )
    DB_PATH: str = os.getenv("DB_PATH", "./db/tariff_dashboard.sqlite")
    
    # External API keys
    CENSUS_API_KEY: str = os.getenv("CENSUS_API_KEY")
    BEA_API_KEY: str = os.getenv("BEA_API_KEY")
    WTO_API_KEY: str = os.getenv("WTO_API_KEY")
    NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY")
    
    # Pipeline settings
    DATA_DIR: str = os.getenv("DATA_DIR", "./data")
    CACHE_DIR: str = os.getenv("CACHE_DIR", "./cache")
    
    # CORS settings
    BACKEND_CORS_ORIGINS: List[str] = [
        "http://localhost:3000", 
        "http://localhost:3001",  
        "http://localhost"
    ]
    
    class Config:
        case_sensitive = True
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()
