import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "G2"
SOURCE_TYPE = "semi_structured"


def fetch_g2_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch software companies from G2 directory.
    
    Uses Apify G2 scraper.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_g2_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"G2 fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_g2_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from G2 via Apify."""
    try:
        actor_id = "apify/g2-scraper"
        input_data = {
            "searchTerm": query,
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
                'category': item.get('category'),
                'raw_fields': item
            })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"G2 Apify failed: {e}")
    
    return []
