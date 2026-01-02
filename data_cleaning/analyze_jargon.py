"""
analyze_jargon - calculate jargon scores for papers in the database
"""

import re
import sys
import argparse
from pathlib import Path
from collections import Counter
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
from Database.db import get_db

DICT_PATH = Path(__file__).parent / "dictionaries" / "google-10000-english-usa-no-swears-medium.txt"


def load_common_words():
    try:
        with DICT_PATH.open('r', encoding='utf-8') as f:
            return set(w.strip().lower() for w in f if w.strip())
    except FileNotFoundError:
        print(f"warning: dictionary not found at {DICT_PATH}")
        return set()


def calculate_jargon_score(text, common_words):
    if not text:
        return None
    
    words = re.findall(r'\b[a-z]{3,}\b', text.lower())
    if not words:
        return None
    
    jargon_words = [w for w in words if w not in common_words]
    
    return {
        "score": round(len(jargon_words) / len(words), 4),
        "total_words": len(words),
        "jargon_count": len(jargon_words),
        "top_jargon": Counter(jargon_words).most_common(10)
    }


def save_jargon_to_db(db, paper_id, jargon_result):
    db['papers'].update_one({"_id": paper_id}, {"$set": {"jargon": jargon_result}})


def analyze_correlation(db):
    papers = list(db['papers'].find({"jargon.score": {"$exists": True}}))
    
    if len(papers) < 5:
        print("not enough data for analysis yet")
        return
    
    by_class = {"HIGH": [], "MODERATE": [], "LOW": []}
    for p in papers:
        cls = p['impact']['classification']
        if cls in by_class:
            by_class[cls].append(p['jargon']['score'])
    
    print("\njargon by impact level:")
    for cls in ["HIGH", "MODERATE", "LOW"]:
        scores = by_class[cls]
        if scores:
            avg = sum(scores) / len(scores)
            print(f"  {cls}: {avg:.1%} avg jargon ({len(scores)} papers)")
    
    print("\nnext steps:")
    print("  python data_cleaning/visualize.py")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--limit", type=int)
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()
    
    print("loading common words")
    common_words = load_common_words()
    print(f"loaded {len(common_words)} words")
    
    print("connecting to database")
    db = get_db()
    
    if args.stats:
        analyze_correlation(db)
        return
    
    query = {"content.abstract": {"$ne": "", "$ne": None}, "jargon": {"$exists": False}}
    cursor = db['papers'].find(query)
    if args.limit:
        cursor = cursor.limit(args.limit)
    papers = list(cursor)
    
    if not papers:
        print("all papers already scored")
        analyze_correlation(db)
        return
    
    print(f"scoring {len(papers)} papers")
    
    with tqdm(papers, desc="scoring", unit="paper") as pbar:
        for paper in pbar:
            result = calculate_jargon_score(paper['content']['abstract'], common_words)
            if result:
                save_jargon_to_db(db, paper['_id'], result)
    
    print("\ndone")
    analyze_correlation(db)


if __name__ == "__main__":
    main()
