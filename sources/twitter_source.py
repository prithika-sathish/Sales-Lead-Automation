import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor
import re

logger = logging.getLogger(__name__)

SOURCE_NAME = "Twitter"
SOURCE_TYPE = "unstructured"


def fetch_twitter_source_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch company mentions from Twitter.
    
    Uses Apify Twitter scraper + entity extraction.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_twitter_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"Twitter fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_twitter_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from Twitter via Apify."""
    try:
        actor_id = "apify/twitter-scraper"
        input_data = {
            "searchTerm": query,
            "limit": 50,
            "maxRequests": 100
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            # Extract mentions and hashtags
            text = item.get('text', '')
            mentions = re.findall(r'@(\w+)', text)
            hashtags = re.findall(r'#(\w+)', text)
            
            # Add mentions as potential companies
            for mention in mentions[:3]:
                normalized.append({
                    'name': mention,
                    'domain': None,
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'raw_url': item.get('url'),
                    'tweet_text': text,
                    'author': item.get('author'),
                    'raw_fields': item
                })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"Twitter Apify failed: {e}")
    
    return []
