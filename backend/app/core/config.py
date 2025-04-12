from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    # API and project settings
    API_V1_STR: str = "/api"
    PROJECT_NAME: str = "Tariff Dashboard API"

    # Database settings
    SQLALCHEMY_DATABASE_URI: str = "sqlite:///./tariff_dashboard.db"
    DB_PATH: str = "db/tariff_dashboard.sqlite"

    CENSUS_API_KEY: str = "903043b59ad55c323132bd1ba3964e5b04d796cd"
    BEA_API_KEY: str = "EB3C36A8-1BE3-49B5-8F90-347C5281ED01"
    WTO_API_KEY: str = "36faf295023942b99db1af50883c2398"
    NEWSAPI_KEY: str = "433e3ed264004649a80266b05d21bb82"

    # Pipeline and caching settings
    DATA_DIR: str = "./data"
    CACHE_DIR: str = "./cache"

    # CORS settings for the backend
    BACKEND_CORS_ORIGINS: List[str] = [
        "http://localhost:3000", 
        "http://localhost:3001",  
        "http://localhost"
    ]

    model_config = SettingsConfigDict(
        env_file=".env", 
        case_sensitive=True
    )

settings = Settings()
