"""
Utility functions for web scraping
"""
import time
import logging
from typing import Optional, Callable, Any
from functools import wraps
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import config

logger = logging.getLogger(__name__)


def get_session_with_retries()->requests.Session:
    """
    Create a requests session with automatic retry logic

    Returns:
        Configured requests.Session object
    """
    session=requests.Session()
    retry_strategy=Retry(
        total=config.MAX_RETRIES,
        backoff_factor=config.RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )

    adapter=HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)


    session.headers.update({
        "User-Agent": config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    })

    return session


def rate_limit(delay:float):
    """
    Decorator to enforce rate limiting between function calls

    Args:
        delay: Minimum seconds to wait between calls
    """

    def decorator(func:Callable)->Callable:
        last_called=[0.0]

        @wraps(func)
        def wrapper(*args,**kwargs)->Any:
            elapsed=time.time()-last_called[0]

            if elapsed<delay:
                sleep_time=delay-elapsed
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            result=func(*args,**kwargs)

            last_called[0]=time.time()

            return result
        return wrapper
    return decorator

def safe_extract_text(element,selector:str,default:str="")->str:
    """
    Safely extract text from BeautifulSoup element

    Args:
        element: BeautifulSoup element
        selector: CSS selector
        default: Default value if not found

    Returns:
        Extracted text or default value
    """

    try:
        found=element.select_one(selector)
        return found.get_text(strip=True) if found else default
    
    except Exception as e:
        logger.warning(f"Error extracting text with selector '{selector}': {e}")
        return default


def safe_extract_int(value:Optional[str],default:int=0)->int:
    """
    Safely convert string to integer

    Args:
        value: String value
        default: Default value if conversion fails

    Returns:
        Integer value or default
    """

    if not value:
        return default
    
    try:
        cleaned=value.replace(",","").replace(" ","").strip()
        return int(cleaned) if cleaned else default
    except (ValueError,AttributeError):
        return default
    
def safe_extract_float(value:Optional[str],default:float=0.0)->float:
    """
    Safely convert string to float

    Args:
        value: String value
        default: Default value if conversion fails

    Returns:
        Float value or default
    """

    if not value:
        return default
    
    try:
        cleaned=value.replace(",","").replace(" ","").strip()
        return float(cleaned) if cleaned else default
    except (ValueError,AttributeError):
        return default
    

def clean_player_name(name:str)->str:
    """
    Clean player name (remove special characters, standardize)

    Args:
        name: Raw player name

    Returns:
        Cleaned player name
    """

    if not name:
        return ""
    
    cleaned=" ".join(name.split())

    for indicator in [" (GK)", " (DF)", " (MF)", " (FW)"]:
        cleaned=cleaned.replace(indicator,"")
    
    return cleaned.strip()


def generate_match_id(home_team: str, away_team: str, match_date: str) -> str:
    """
    Generate consistent match ID from teams and date

    Args:
        home_team: Home team name
        away_team: Away team name
        match_date: Match date (YYYY-MM-DD format)

    Returns:
        Match ID string
    """

    home = home_team.lower().replace(" ", "_")
    away = away_team.lower().replace(" ", "_")
    date = match_date.replace("-", "")

    return f"{date}_{home}_vs_{away}"


class ScraperException(Exception):
    """Base exception for scraper errors"""
    pass

class RateLimitException(ScraperException):
    """Exception raised when rate limit is exceeded"""
    pass


class DataValidationException(ScraperException):
    """Exception raised when scraped data fails validation"""
    pass
