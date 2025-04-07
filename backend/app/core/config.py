import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    API_V1_STR: str = "/api"
    PROJECT_NAME: str = "Tariff Dashboard API"
    
    # External API keys
    CENSUS_API_KEY: str = os.getenv("CENSUS_API_KEY", "903043b59ad55c323132bd1ba3964e5b04d796cd")
    BEA_API_KEY: str = os.getenv("BEA_API_KEY", "EB3C36A8-1BE3-49B5-8F90-347C5281ED01")
    WTO_API_KEY: str = os.getenv("WTO_API_KEY", "36faf295023942b99db1af50883c2398")
    
    # Database settings
    MONGODB_URL: str = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "tariff_dashboard")
    
    # Cache settings
    CACHE_DIR: str = os.getenv("CACHE_DIR", "data/processed")
    CACHE_EXPIRATION: int = int(os.getenv("CACHE_EXPIRATION", "86400"))  # 24 hours
    
    # CORS settings
    BACKEND_CORS_ORIGINS: list = ["*"]  # For development

    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()