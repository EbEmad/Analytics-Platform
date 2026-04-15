"""
Configuration for web scrapers
"""
import os
from dataclasses import dataclass
@dataclass
class ScraperConfig:
    """Configuration for web scrapers"""

    FBREF_REQUEST_DELAY: float=3.0
    UNDERSTAT_REQUEST_DELAY: float=2.0

    MAX_RETRIES:int=3
    RETRY_BACKOFF_FACTOR:float=2.0

    REQUEST_TIMEOUT:int=30

    USER_AGENT:str=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    FBREF_BASE_URL:str="https://fbref.com"
    ARSENAL_FBREF_ID:str="18bb7c10" 

    UNDERSTAT_BASE_URL:str="https://understat.com"
    ARSENAL_UNDERSTAT_NAME:str="Arsenal"


    DB_HOST:str=os.getenv("POSTGRES_HOST", "postgres")
    DB_PORT:int=int(os.getenv("POSTGRES_PORT", "5432"))
    DB_NAME:str=os.getenv("ANALYTICS_DB_NAME", "arsenalfc_analytics")
    DB_USER:str=os.getenv("ANALYTICS_DB_USER","analytics_user")
    DB_PASSWORD:str=os.getenv("ANALYTICS_DB_PASSWORD","analytics_pass")

    @property
    def db_connection_string(self)->str:
        """Get PostgreSQL connection string"""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
config = ScraperConfig()