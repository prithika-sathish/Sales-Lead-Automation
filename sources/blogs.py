import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor
import re

logger = logging.getLogger(__name__)

SOURCE_NAME = "Blogs"
SOURCE_TYPE = "unstructured"


def fetch_blogs_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch company mentions from blog posts.
    
    Uses Apify web scraper on search results + entity extraction.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_blogs_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"Blogs fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_blogs_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from blogs via Google search + Apify scraper."""
    try:
        # Use Google search to find blog posts
        actor_id = "apify/google-search-scraper"
        input_data = {
            "query": f'{query} site:medium.com OR site:hashnode.com OR site:dev.to',
            "limit": 20,
            "maxRequests": 100
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            # Extract entities from blog content
            title = item.get('title', '')
            url = item.get('url', '')
            
            # Extract capitalized phrases as potential companies
            entities = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', title)
            
            for entity in entities[:3]:
                if len(entity) > 2 and entity not in ['The', 'How', 'Why', 'What']:
                    normalized.append({
                        'name': entity,
                        'domain': None,
                        'source': SOURCE_NAME,
                        'source_type': SOURCE_TYPE,
                        'raw_url': url,
                        'article_title': title,
                        'platform': _extract_platform(url),
                        'raw_fields': item
                    })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"Blogs Apify failed: {e}")
    
    return []


def _extract_platform(url: str) -> str:
    """Extract blog platform from URL."""
    if 'medium.com' in url:
        return 'Medium'
    elif 'hashnode.com' in url:
        return 'Hashnode'
    elif 'dev.to' in url:
        return 'Dev.to'
    return 'Unknown'
