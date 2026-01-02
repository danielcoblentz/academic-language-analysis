"""
download_pdfs.py - Download PDFs for papers in the database
"""

import requests
import sys
from pathlib import Path
from tqdm import tqdm
import time

sys.path.insert(0, str(Path(__file__).parent.parent))
from Database.db import get_db


# --- Config ---

PDF_DIR = Path(__file__).parent.parent / "data" / "pdfs"
TIMEOUT = 30
DELAY = 1


# --- Functions ---

def get_papers_to_download(db, limit=None):
    """Get papers with PDF URLs that haven't been downloaded."""
    coll = db['papers']
    query = {
        "open_access.pdf_url": {"$ne": "", "$ne": None},
        "processing_status": "pending_download"
    }
    cursor = coll.find(query)
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)


def download_pdf(url, save_path):
    """Download PDF from URL."""
    try:
        headers = {'User-Agent': 'AcademicLanguageAnalysis/1.0 (research project)'}
        r = requests.get(url, timeout=TIMEOUT, headers=headers, allow_redirects=True)
        r.raise_for_status()
        
        content_type = r.headers.get('content-type', '')
        if 'pdf' not in content_type.lower() and not r.content[:4] == b'%PDF':
            return False, "Not a PDF"
        
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(r.content)
        return True, None
    except Exception as e:
        return False, str(e)


def update_paper_status(db, paper_id, status, local_path=None):
    """Update paper processing status."""
    update = {"$set": {"processing_status": status}}
    if local_path:
        update["$set"]["content.local_path"] = str(local_path)
    db['papers'].update_one({"_id": paper_id}, update)


def show_status(db):
    """Show download status."""
    coll = db['papers']
    total = coll.count_documents({})
    pending = coll.count_documents({"processing_status": "pending_download"})
    downloaded = coll.count_documents({"processing_status": "downloaded"})
    failed = coll.count_documents({"processing_status": "failed"})
    no_pdf = coll.count_documents({"processing_status": "no_pdf_available"})
    
    print(f"\nDownload status:")
    print(f"  Total papers: {total}")
    print(f"  Pending: {pending}")
    print(f"  Downloaded: {downloaded}")
    print(f"  Failed: {failed}")
    print(f"  No PDF available: {no_pdf}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Download PDFs for papers")
    parser.add_argument("-n", "--limit", type=int, default=10, help="Max PDFs to download")
    parser.add_argument("--status", action="store_true", help="Show status only")
    args = parser.parse_args()
    
    print("Connecting to database...")
    db = get_db()
    
    if args.status:
        show_status(db)
        return
    
    papers = get_papers_to_download(db, limit=args.limit)
    
    if not papers:
        print("No papers to download.")
        show_status(db)
        return
    
    print(f"Downloading {len(papers)} PDFs...")
    
    success = 0
    failed = 0
    
    with tqdm(papers, desc="Downloading", unit="pdf") as pbar:
        for paper in pbar:
            paper_id = paper['_id']
            url = paper['open_access']['pdf_url']
            year = paper.get('year') or 'unknown'
            
            safe_id = paper_id.replace('/', '_').replace(':', '_')[-50:]
            save_path = PDF_DIR / str(year) / f"{safe_id}.pdf"
            
            pbar.set_postfix_str(paper['title'][:30] + "...")
            
            ok, err = download_pdf(url, save_path)
            
            if ok:
                update_paper_status(db, paper_id, "downloaded", save_path)
                success += 1
            else:
                update_paper_status(db, paper_id, "failed")
                failed += 1
            
            time.sleep(DELAY)
    
    print(f"\nSuccess: {success}, Failed: {failed}")
    show_status(db)


if __name__ == "__main__":
    main()
