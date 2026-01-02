"""
util.py - Helper functions for the data pipeline

API calls, author extraction, impact scoring, doc building.
"""

from datetime import datetime
import requests
from urllib.parse import quote


# --- Impact scoring ---

def impact_score(citations, publication_year):
    """
    Citations per year since publication
    Minimum age of 1 year to avoid divide-by-zero
    """
    try:
        year = int(publication_year)
    except Exception:
        year = None

    current_year = datetime.now().year
    if year and year <= current_year:
        age = max(1, current_year - year + 1)
    else:
        age = 1

    try:
        citations_val = float(citations)
    except Exception:
        citations_val = 0.0

    return citations_val / age


def classify_impact(score):
    """numeric score into HIGH/MODERATE/LOW"""
    if score > 5:
        return "HIGH"
    elif score > 1:
        return "MODERATE"
    return "LOW"


def determine_processing_status(pdf_url):
    """Figure out initial status based on whether we have a PDF link"""
    return "pending_download" if pdf_url else "no_pdf_available"


# --- External API calls ---

def fetch_crossref(doi, timeout=10):
    """
    Get metadata from Crossref for a DOI
    Returns empty dict if anything goes wrong
    """
    if not doi:
        return {}
    
    doi_clean = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    url = f"https://api.crossref.org/works/{quote(doi_clean, safe='')}"
    
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json().get('message', {})
    except Exception:
        return {}


def fetch_unpaywall(doi, email=None, timeout=10):
    """
    Check Unpaywall for OA info
    Email is required by their API but we'll try anyway if missing
    """
    if not doi:
        return {}
    
    doi_clean = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    url = f"https://api.unpaywall.org/v2/{quote(doi_clean, safe='')}"
    params = {'email': email} if email else {}
    
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


# --- Data helpers ---

def normalize_doi(doi):
    """Lowercase and strip URL prefixes from DOI"""
    if not doi:
        return None
    doi = doi.lower().strip()
    return doi.replace("https://doi.org/", "").replace("http://doi.org/", "")


def _safe_get(data, *keys, default=None):
    """Dig into nested dicts without KeyErrors"""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return default
    return data if data is not None else default


def _extract_authors(work, crossref=None):
    """Pull author names + affiliations from OpenAlex, fallback to Crossref"""
    authors = []
    
    for authorship in (work.get('authorships') or []):
        author_info = authorship.get('author') or {}
        name = author_info.get('display_name') or ""
        institutions = authorship.get('institutions') or []
        affiliation = institutions[0].get('display_name') if institutions else ""
        if name:
            authors.append({"name": name, "affiliation": affiliation or ""})
    
    if not authors and crossref:
        for a in (crossref.get('author') or []):
            given = a.get('given', '')
            family = a.get('family', '')
            name = f"{given} {family}".strip()
            affs = a.get('affiliation') or []
            affiliation = affs[0].get('name') if affs else ""
            if name:
                authors.append({"name": name, "affiliation": affiliation or ""})
    
    return authors


def _reconstruct_abstract(inverted_index):
    """OpenAlex stores abstracts as inverted index - reconstruct it."""
    if not inverted_index:
        return ""
    words = []
    for word, positions in inverted_index.items():
        for pos in positions:
            words.append((pos, word))
    words.sort(key=lambda x: x[0])
    return ' '.join(w[1] for w in words)


# --- Document builder ---

def build_paper_document(work, crossref=None, unpaywall=None):
    """
    Take raw API data and build a clean doc matching our MongoDB schema
    """
    title = work.get('title') or "<no title>"
    citations = work.get('cited_by_count', 0) or 0
    year = work.get('publication_year')
    doi = normalize_doi(work.get('doi'))
    openalex_id = work.get('id')

    crossref = crossref or {}
    unpaywall = unpaywall or {}

    authors = _extract_authors(work, crossref)

    journal_name = (
        _safe_get(work, 'host_venue', 'display_name') or
        _safe_get(work, 'primary_location', 'source', 'display_name') or
        (crossref.get('container-title') or [None])[0]
    )
    issn = (
        _safe_get(work, 'host_venue', 'issn_l') or
        _safe_get(work, 'primary_location', 'source', 'issn_l') or
        (crossref.get('ISSN') or [None])[0]
    )

    is_oa = bool(work.get('is_oa', False)) or bool(unpaywall.get('is_oa', False))
    pdf_url = (
        _safe_get(work, 'best_oa_location', 'url_for_pdf') or
        _safe_get(work, 'primary_location', 'pdf_url') or
        _safe_get(unpaywall, 'best_oa_location', 'url_for_pdf') or
        unpaywall.get('url')
    )
    oa_status = (
        _safe_get(work, 'best_oa_location', 'license') or
        _safe_get(unpaywall, 'best_oa_location', 'license') or
        ""
    )

    # try multiple sources for abstract
    abstract = (
        work.get('abstract') or 
        _reconstruct_abstract(work.get('abstract_inverted_index')) or
        crossref.get('abstract') or 
        ""
    )

    score = impact_score(citations, year)
    classification = classify_impact(score)

    counts_by_year = work.get('counts_by_year') or []
    influential = 0
    if isinstance(counts_by_year, list) and counts_by_year:
        influential = int(counts_by_year[-1].get('cited_by_count', 0))

    processing_status = determine_processing_status(pdf_url)

    tags = [c.get('display_name') for c in (work.get('concepts') or []) if c.get('display_name')]

    work_id = openalex_id or doi or f"auto:{title[:80]}"

    return {
        "_id": work_id,
        "title": title,
        "year": int(year) if year else None,
        "authors": authors,
        "journal": {"name": journal_name or "", "issn": issn or ""},
        "impact": {
            "citation_count": int(citations),
            "citations_per_year": float(score),
            "classification": classification,
            "influential_citations": influential
        },
        "open_access": {"is_oa": bool(is_oa), "pdf_url": pdf_url or "", "status": oa_status},
        "content": {"abstract": abstract, "full_text_extracted": False, "local_path": None},
        "processing_status": processing_status,
        "tags": tags
    }
