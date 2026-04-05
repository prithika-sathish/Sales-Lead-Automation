import requests
import json
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "Crunchbase"
SOURCE_TYPE = "structured"


def fetch_crunchbase_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch startup companies from Crunchbase.
    
    Uses Crunchbase API (free tier) or Apify actor.
    """
    all_rows = []
    
    for query in queries:
        try:
            # Try Crunchbase API first
            rows = _fetch_crunchbase_api(query)
            if rows:
                all_rows.extend(rows)
                continue
            
            # Fallback: Apify actor
            rows = _fetch_crunchbase_apify(query)
            if rows:
                all_rows.extend(rows)
        
        except Exception as e:
            logger.error(f"Crunchbase fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_crunchbase_api(query: str) -> List[Dict[str, Any]]:
    """Fetch from Crunchbase's free public data."""
    try:
        # Crunchbase has limited free API; using search endpoint
        url = "https://api.crunchbase.com/v3.1/entities/companies/search"
        
        # Note: Requires API key; skip if not available
        api_key = os.getenv("CRUNCHBASE_API_KEY")
        if not api_key:
            return []
        
        params = {
            "name": query,
            "user_key": api_key
        }
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rows = []
            for company in data.get("entities", []):
                rows.append({
                    'name': company.get('name'),
                    'domain': company.get('domain') or company.get('website'),
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'raw_url': company.get('web_url'),
                    'raw_fields': company
                })
            return rows
    except Exception as e:
        logger.warning(f"Crunchbase API failed: {e}")
    
    return []


def _fetch_crunchbase_apify(query: str) -> List[Dict[str, Any]]:
    """Fallback to Apify Crunchbase scraper."""
    try:
        actor_id = "apify/crunchbase-scraper"
        input_data = {
            "companyName": query,
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
        logger.warning(f"Crunchbase Apify failed: {e}")
    
    return []


import os
