import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "App Stores"
SOURCE_TYPE = "structured"


def fetch_app_stores_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch app developers from Google Play Store.
    
    Uses Apify Google Play scraper.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_app_stores_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"App Stores fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_app_stores_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from Google Play Store via Apify."""
    try:
        actor_id = "apify/google-play-scraper"
        input_data = {
            "searchTerm": query,
            "language": "en",
            "country": "US",
            "maxResults": 10
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            # Extract developer info
            developer_name = item.get('developer') or item.get('developerName')
            developer_url = item.get('developerUrl') or item.get('developer_url')
            
            if developer_name:
                normalized.append({
                    'name': developer_name,
                    'domain': _extract_domain_from_url(developer_url) if developer_url else None,
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'raw_url': developer_url,
                    'app_name': item.get('title') or item.get('name'),
                    'raw_fields': item
                })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"App Stores Apify failed: {e}")
    
    return []


def _extract_domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc or None
    except:
        return None
