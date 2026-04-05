import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "Clutch"
SOURCE_TYPE = "semi_structured"


def fetch_clutch_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch B2B companies from Clutch agency directory.
    
    Uses Apify Clutch scraper.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_clutch_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"Clutch fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_clutch_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from Clutch via Apify."""
    try:
        actor_id = "apify/clutch-scraper"
        input_data = {
            "search": query,
            "limit": 10,
            "maxRequests": 50
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            normalized.append({
                'name': item.get('name') or item.get('company_name'),
                'domain': item.get('website') or item.get('domain'),
                'source': SOURCE_NAME,
                'source_type': SOURCE_TYPE,
                'raw_url': item.get('url'),
                'rating': item.get('rating'),
                'raw_fields': item
            })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"Clutch Apify failed: {e}")
    
    return []
