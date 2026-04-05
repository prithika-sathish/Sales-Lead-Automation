import requests
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

SOURCE_NAME = "GitHub"
SOURCE_TYPE = "semi_structured"


def fetch_github_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch organizations from GitHub.
    
    Uses GitHub API to find organizations matching queries.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_github_api(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"GitHub fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_github_api(query: str) -> List[Dict[str, Any]]:
    """Fetch organizations from GitHub API."""
    try:
        url = "https://api.github.com/search/users"
        
        headers = {
            'Accept': 'application/vnd.github.v3+json'
        }
        
        params = {
            'q': f'{query} type:org',
            'per_page': 10,
            'sort': 'repositories',
            'order': 'desc'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rows = []
            
            for org in data.get("items", []):
                org_name = org.get('login')
                rows.append({
                    'name': org_name,
                    'domain': org.get('blog') or None,
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'raw_url': org.get('html_url'),
                    'location': org.get('location'),
                    'bio': org.get('bio'),
                    'raw_fields': org
                })
            
            return rows
    except Exception as e:
        logger.warning(f"GitHub API failed: {e}")
    
    return []
