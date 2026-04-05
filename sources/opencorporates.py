import requests
import logging
from typing import List, Dict, Any
import json

logger = logging.getLogger(__name__)

SOURCE_NAME = "OpenCorporates"
SOURCE_TYPE = "structured"


def fetch_opencorporates_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch company data from OpenCorporates company registry.
    
    Uses OpenCorporates free API (rate-limited but public).
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_opencorporates_api(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"OpenCorporates fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_opencorporates_api(query: str) -> List[Dict[str, Any]]:
    """Fetch from OpenCorporates free API."""
    try:
        url = "https://api.opencorporates.com/v0.4/companies/search"
        
        params = {
            "q": query,
            "per_page": 10,
            "order": "relevance"
        }
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rows = []
            
            for company in data.get("results", {}).get("companies", []):
                company_data = company.get("company", {})
                rows.append({
                    'name': company_data.get('name'),
                    'domain': None,  # OpenCorporates doesn't provide website in free tier
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'jurisdiction': company_data.get('jurisdiction_code'),
                    'raw_url': company_data.get('registered_address'),
                    'raw_fields': company_data
                })
            
            return rows
    except Exception as e:
        logger.warning(f"OpenCorporates API failed: {e}")
    
    return []
