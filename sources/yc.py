import requests
import json
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "Y Combinator"
SOURCE_TYPE = "structured"


def fetch_y_combinator_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch startup companies from Y Combinator.
    
    Uses Apify actor (yc-companies scraper) or falls back to direct API.
    """
    all_rows = []
    
    for query in queries:
        try:
            # Try YC API first
            rows = _fetch_yc_api(query)
            if rows:
                all_rows.extend(rows)
                continue
            
            # Fallback: Apify actor
            rows = _fetch_yc_apify(query)
            if rows:
                all_rows.extend(rows)
        
        except Exception as e:
            logger.error(f"YC fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_yc_api(query: str) -> List[Dict[str, Any]]:
    """Fetch from YC's public API or companies list."""
    try:
        # YC publishes company list at https://www.ycombinator.com/companies
        # We'll use a scrape approach via requests
        url = "https://www.ycombinator.com/api/company_search"
        params = {"query": query, "limit": 10}
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rows = []
            for company in data.get("companies", []):
                rows.append({
                    'name': company.get('name'),
                    'domain': company.get('website') or company.get('url'),
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'raw_url': company.get('url'),
                    'raw_fields': company
                })
            return rows
    except Exception as e:
        logger.warning(f"YC API failed: {e}")
    
    return []


def _fetch_yc_apify(query: str) -> List[Dict[str, Any]]:
    """Fallback to Apify YC scraper."""
    try:
        actor_id = "apify/ycombinator-scraper"
        input_data = {
            "search": query,
            "limit": 10,
            "maxRequests": 100
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
                'raw_fields': item
            })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"YC Apify failed: {e}")
    
    return []
