import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "Marketplaces"
SOURCE_TYPE = "semi_structured"


def fetch_marketplaces_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch sellers/vendors from Amazon and IndiaMART.
    
    Uses Apify marketplace scrapers.
    """
    all_rows = []
    
    for query in queries:
        try:
            # Amazon sellers
            rows = _fetch_amazon_sellers(query)
            if rows:
                all_rows.extend(rows)
            
            # IndiaMART sellers
            rows = _fetch_indiamart_sellers(query)
            if rows:
                all_rows.extend(rows)
        
        except Exception as e:
            logger.error(f"Marketplaces fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_amazon_sellers(query: str) -> List[Dict[str, Any]]:
    """Fetch sellers from Amazon."""
    try:
        actor_id = "apify/amazon-seller-scraper"
        input_data = {
            "searchTerm": query,
            "limit": 10,
            "maxRequests": 50
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            seller_name = item.get('seller_name') or item.get('name')
            if seller_name:
                normalized.append({
                    'name': seller_name,
                    'domain': item.get('seller_url') or None,
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'marketplace': 'Amazon',
                    'raw_url': item.get('seller_url'),
                    'raw_fields': item
                })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"Amazon sellers fetch failed: {e}")
    
    return []


def _fetch_indiamart_sellers(query: str) -> List[Dict[str, Any]]:
    """Fetch sellers from IndiaMART."""
    try:
        actor_id = "apify/indiamart-seller-scraper"
        input_data = {
            "searchTerm": query,
            "limit": 10,
            "maxRequests": 50
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            seller_name = item.get('seller_name') or item.get('name')
            if seller_name:
                normalized.append({
                    'name': seller_name,
                    'domain': item.get('seller_url') or None,
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'marketplace': 'IndiaMART',
                    'raw_url': item.get('seller_url'),
                    'raw_fields': item
                })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"IndiaMART sellers fetch failed: {e}")
    
    return []
