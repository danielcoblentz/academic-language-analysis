"""
get_papers.py - Fetch ecology papers from OpenAlex + enrich with Crossref/Unpaywall

Pulls metadata, calculates impact scores, and upserts to MongoDB.
"""

import json
import requests
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from requests.exceptions import RequestException
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from lib.util import (
    fetch_crossref,
    fetch_unpaywall,
    normalize_doi,
    build_paper_document
)
from Database.db import get_db


# --- Config ---

load_dotenv()
TOPIC_ID = os.getenv("TOPIC_ID")
EMAIL = os.getenv("EMAIL")
BASE_URL = os.getenv("BASE_URL")

if not TOPIC_ID:
    print("Error: TOPIC_ID not set in .env")
    sys.exit(1)

if not BASE_URL:
    BASE_URL = "https://api.openalex.org/works"


# --- API params ---

params = {
    'filter': f'concepts.id:{TOPIC_ID},publication_year:2020-2025,is_oa:true,has_abstract:true',
    'sort': 'cited_by_count:desc',
    'per-page': 50,
}
if EMAIL:
    params['mailto'] = EMAIL


# --- Functions ---

def fetch_papers():
    """Query OpenAlex API."""
    print("Searching for ecology papers with abstracts...")
    print(f"Query: {params['filter']}")
    try:
        response = requests.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        print(f"Found {data.get('meta', {}).get('count', 0)} total matches")
        return data
    except RequestException as e:
        print(f"API request failed: {e}")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Couldn't parse API response")
        sys.exit(1)


def process_and_enrich(data):
    """Process results and enrich with additional APIs."""
    papers = []
    results = data.get('results', [])
    
    print(f"Processing {len(results)} papers...")
    
    for work in tqdm(results, desc="Enriching", unit="paper"):
        doi = normalize_doi(work.get('doi'))
        crossref = fetch_crossref(doi) if doi else {}
        unpaywall = fetch_unpaywall(doi, EMAIL) if doi else {}
        doc = build_paper_document(work, crossref, unpaywall)
        papers.append(doc)
    
    return papers


def upsert_to_db(papers):
    """Insert or update papers in MongoDB."""
    db = get_db()
    coll = db['papers']
    inserted = 0
    updated = 0

    for doc in papers:
        _id = doc.get('_id')
        if not _id:
            continue
        try:
            result = coll.replace_one({"_id": _id}, doc, upsert=True)
            if result.upserted_id:
                inserted += 1
            elif result.modified_count:
                updated += 1
        except Exception as e:
            print(f"DB error for {_id}: {e}")

    return inserted, updated


def print_summary(papers, inserted, updated):
    """Print results summary."""
    with_abstract = sum(1 for p in papers if p['content']['abstract'])
    
    print(f"\n{'='*50}")
    print(f"Stage 1 complete")
    print(f"{'='*50}")
    print(f"Total processed: {len(papers)}")
    print(f"With abstracts: {with_abstract}")
    print(f"New in DB: {inserted}")
    print(f"Updated: {updated}")

    print(f"\nSample papers:")
    for paper in papers[:3]:
        print(f"\n  [{paper['impact']['classification']}] {paper['title'][:60]}...")
        abstract = paper['content']['abstract'] or "NO ABSTRACT"
        print(f"Abstract: {abstract[:100]}...")
        print(f"Citations: {paper['impact']['citation_count']}")


def main():
    data = fetch_papers()
    papers = process_and_enrich(data)
    inserted, updated = upsert_to_db(papers)
    print_summary(papers, inserted, updated)
    print("\nNext: python data_cleaning/extract.py -n 5")


if __name__ == "__main__":
    main()