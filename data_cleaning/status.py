"""
status.py - Quick overview of the entire pipeline

Shows what's in the database and what needs to be done.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from Database.db import get_db


def main():
    print("Connecting to database...")
    try:
        db = get_db()
    except Exception as e:
        print(f"Connection failed: {e}")
        return
    
    papers = db['papers']
    total = papers.count_documents({})
    
    print("\n" + "="*50)
    print("PIPELINE STATUS")
    print("="*50)
    
    print(f"\nPAPERS: {total}")
    if total > 0:
        with_abstract = papers.count_documents({"content.abstract": {"$ne": None, "$ne": ""}})
        print(f"   With abstracts: {with_abstract}")
        
        # processing status breakdown
        statuses = ["pending_download", "downloaded", "pending_parse", "parsed", "failed", "no_pdf_available"]
        for s in statuses:
            count = papers.count_documents({"processing_status": s})
            if count > 0:
                print(f"   {s}: {count}")
        
        sample = papers.find_one()
        if sample:
            print(f"\n   Sample paper:")
            print(f"   Title: {sample.get('title', 'N/A')[:60]}...")
            print(f"   Abstract: {str(sample.get('content', {}).get('abstract', 'N/A'))[:80]}...")
            print(f"   Citations: {sample.get('impact', {}).get('citation_count', 'N/A')}")
    
    extracted = db['extracted_features'].count_documents({})
    print(f"\nEXTRACTED FEATURES: {extracted}")
    if extracted > 0:
        sample = db['extracted_features'].find_one()
        if sample:
            count = sample.get('data_points', {}).get('extraction_count', 0)
            print(f"   Sample extraction count: {count}")
    
    snapshots = db['snapshots'].count_documents({})
    print(f"\nSNAPSHOTS: {snapshots}")
    
    with_jargon = papers.count_documents({"jargon": {"$exists": True}})
    print(f"\nJARGON SCORED: {with_jargon}")
    
    print("\n" + "="*50)
    print("NEXT STEPS")
    print("="*50)
    
    if total == 0:
        print("\n1. Run: python data_cleaning/get_papers.py")
    elif extracted == 0:
        print("\n1. Run: python data_cleaning/extract.py -n 5")
    elif with_jargon == 0:
        print("\n1. Run: python data_cleaning/analyze_jargon.py")
    else:
        print("\nPipeline has data. Try:")
        print("   python data_cleaning/analyze_jargon.py --stats")
        print("   python data_cleaning/extract.py --status")


if __name__ == "__main__":
    main()
