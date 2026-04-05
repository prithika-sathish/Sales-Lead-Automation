import requests
import logging
from typing import List, Dict, Any
from sources.apify_common import run_apify_actor

logger = logging.getLogger(__name__)

SOURCE_NAME = "News"
SOURCE_TYPE = "unstructured"


def fetch_news_leads(queries: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch company mentions from news articles.
    
    Uses Apify news scraper + NER extraction.
    """
    all_rows = []
    
    for query in queries:
        try:
            rows = _fetch_news_apify(query)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            logger.error(f"News fetch error for '{query}': {e}")
    
    return all_rows


def _fetch_news_apify(query: str) -> List[Dict[str, Any]]:
    """Fetch from news via Apify."""
    try:
        actor_id = "apify/google-news-scraper"
        input_data = {
            "searchTerm": query,
            "limit": 20,
            "maxRequests": 100
        }
        
        rows = run_apify_actor(actor_id, input_data)
        normalized = []
        
        for item in rows:
            # Extract entities from news content using NER
            entities = _extract_entities(item.get('title', '') + ' ' + item.get('content', ''))
            
            for entity in entities:
                normalized.append({
                    'name': entity,
                    'domain': None,
                    'source': SOURCE_NAME,
                    'source_type': SOURCE_TYPE,
                    'raw_url': item.get('url'),
                    'article_title': item.get('title'),
                    'source_site': item.get('source'),
                    'raw_fields': item
                })
        
        return normalized
    
    except Exception as e:
        logger.warning(f"News Apify failed: {e}")
    
    return []


def _extract_entities(text: str) -> List[str]:
    """Extract organization entities from text using simple NER."""
    # Fallback: simple keyword extraction if spaCy not available
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(text)
        entities = [ent.text for ent in doc.ents if ent.label_ in ["ORG", "PERSON"]]
        return entities[:5]  # Return top 5
    except:
        # Fallback: extract capitalized phrases
        import re
        phrases = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
        return list(set(phrases))[:5]
