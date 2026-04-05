import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor
import re

logger = logging.getLogger(__name__)

SOURCE_NAME = "Reddit"
SOURCE_TYPE = "unstructured"


def fetch_reddit_source_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch company mentions from Reddit.
    
    Uses Apify Reddit scraper + entity extraction.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_reddit_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"Reddit fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_reddit_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from Reddit via Apify."""
    try:
        actor_id = "apify/reddit-scraper"
        input_data = {
            "searchTerm": query,
            "limit": 50,
            "maxRequests": 100
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            # Extract entities from post content
            text = item.get('text') or item.get('content') or item.get('title', '')
            
            # Simple entity extraction: capitalized phrases
            entities = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
            
            for entity in entities[:3]:
                if len(entity) > 2:  # Filter very short names
                    normalized.append({
                        'name': entity,
                        'domain': None,
                        'source': SOURCE_NAME,
                        'source_type': SOURCE_TYPE,
                        'raw_url': item.get('url'),
                        'post_title': item.get('title'),
                        'subreddit': item.get('subreddit'),
                        'author': item.get('author'),
                        'raw_fields': item
                    })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"Reddit Apify failed: {e}")
    
    return []
