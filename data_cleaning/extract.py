"""
extract.py - Pull structured info from papers using LangExtract

Runs extraction on abstracts from our MongoDB papers collection.
Results go into extracted_features collection.
"""

import langextract as lx
import textwrap
import argparse
import sys
from pathlib import Path
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
from Database.db import get_db


# --- Config ---

MODEL_ID = "gemini-2.5-flash"
OUTPUT_DIR = Path(__file__).parent / "output"
SCRIPT_VERSION = "v1.0"


# --- Prompts & Examples ---

EXTRACTION_PROMPT = textwrap.dedent("""\
    Extract key scientific entities from this research abstract:
    - Methods: experimental techniques, statistical methods
    - Subjects: species, organisms, ecosystems studied  
    - Metrics: measurements, variables, outcomes
    - Findings: key results or conclusions
    
    Use exact text when possible. Provide attributes for context.
""")


def get_extraction_examples():
    """Examples for ecology/science papers."""
    return [
        lx.data.ExampleData(
            text="We measured soil nitrogen levels across 12 alpine meadow sites "
                 "using spectrophotometry. Results showed a 23% decrease in N concentration.",
            extractions=[
                lx.data.Extraction(
                    extraction_class="method",
                    extraction_text="spectrophotometry",
                    attributes={"type": "measurement technique"}
                ),
                lx.data.Extraction(
                    extraction_class="subject",
                    extraction_text="alpine meadow sites",
                    attributes={"count": "12", "type": "ecosystem"}
                ),
                lx.data.Extraction(
                    extraction_class="metric",
                    extraction_text="soil nitrogen levels",
                    attributes={"variable": "N concentration"}
                ),
                lx.data.Extraction(
                    extraction_class="finding",
                    extraction_text="23% decrease in N concentration",
                    attributes={"direction": "decrease", "magnitude": "23%"}
                ),
            ]
        )
    ]


# --- Core functions ---

def extract_from_text(text, prompt=None, examples=None, model_id=None):
    """Run extraction on text."""
    prompt = prompt or EXTRACTION_PROMPT
    examples = examples or get_extraction_examples()
    model_id = model_id or MODEL_ID

    return lx.extract(
        text_or_documents=text,
        prompt_description=prompt,
        examples=examples,
        model_id=model_id,
    )


def get_papers_to_process(db, limit=None, only_unprocessed=True):
    """Get papers with abstracts, optionally skip already processed."""
    coll = db['papers']
    query = {"content.abstract": {"$ne": "", "$ne": None}}
    
    if only_unprocessed:
        processed_ids = set(
            doc['paper_id'] for doc in db['extracted_features'].find({}, {"paper_id": 1})
        )
        papers = []
        for p in coll.find(query):
            if p['_id'] not in processed_ids:
                papers.append(p)
                if limit and len(papers) >= limit:
                    break
        return papers
    else:
        cursor = coll.find(query)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)


def save_extraction_to_db(db, paper_id, extraction_result):
    """Save extraction results to extracted_features collection."""
    coll = db['extracted_features']
    
    extractions = []
    if hasattr(extraction_result, 'extractions'):
        for ext in extraction_result.extractions:
            extractions.append({
                "class": ext.extraction_class,
                "text": ext.extraction_text,
                "attributes": ext.attributes or {}
            })
    
    doc = {
        "paper_id": paper_id,
        "script_version": SCRIPT_VERSION,
        "data_points": {
            "extractions": extractions,
            "extraction_count": len(extractions)
        }
    }
    
    coll.replace_one({"paper_id": paper_id}, doc, upsert=True)
    return doc


def save_results_to_file(results, output_dir=None):
    """Save results to JSONL for visualization."""
    output_dir = Path(output_dir or OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "extraction_results.jsonl"
    lx.io.save_annotated_documents(results, output_name=output_file.name, output_dir=str(output_dir))
    return output_file


def generate_visualization(input_file, output_dir=None):
    """Create HTML viz from results."""
    output_dir = Path(output_dir or OUTPUT_DIR)
    output_file = output_dir / "visualization.html"
    
    html_content = lx.visualize(str(input_file))
    content = html_content.data if hasattr(html_content, 'data') else html_content
    output_file.write_text(content)
    
    return output_file


def show_db_status(db):
    """Print database status"""
    papers_count = db['papers'].count_documents({})
    with_abstract = db['papers'].count_documents({"content.abstract": {"$ne": "", "$ne": None}})
    extracted = db['extracted_features'].count_documents({})
    
    print(f"\nDatabase status:")
    print(f"Total papers: {papers_count}")
    print(f"With abstracts: {with_abstract}")
    print(f"Already extracted: {extracted}")
    print(f"Remaining: {with_abstract - extracted}")
    
    return papers_count, with_abstract, extracted


# --- CLI ---

def process_papers(limit=5, save_viz=True, reprocess=False):
    """Main processing loop."""
    print("Connecting to database...")
    try:
        db = get_db()
    except Exception as e:
        print(f"Database connection failed: {e}")
        print("\nCheck your .env MongoDB credentials.")
        sys.exit(1)
    
    papers_count, with_abstract, extracted = show_db_status(db)
    
    if papers_count == 0:
        print("\nNo papers in database.")
        print("Run 'python data_cleaning/get_papers.py' first.")
        return
    
    print("\nFetching papers...")
    papers = get_papers_to_process(db, limit=limit, only_unprocessed=not reprocess)
    
    if not papers:
        if with_abstract == 0:
            print("No papers have abstracts.")
        else:
            print("All papers processed. Use --reprocess to re-run.")
        return
    
    results = []
    errors = []
    
    with tqdm(papers, desc="Extracting", unit="paper") as pbar:
        for paper in pbar:
            paper_id = paper['_id']
            abstract = paper['content']['abstract']
            title = paper['title'][:40] + "..." if len(paper['title']) > 40 else paper['title']
            
            pbar.set_postfix_str(title)
            
            try:
                result = extract_from_text(abstract)
                save_extraction_to_db(db, paper_id, result)
                results.append(result)
            except Exception as e:
                errors.append((paper_id, str(e)))
    
    print(f"\nProcessed {len(results)} papers")
    if errors:
        print(f"Errors: {len(errors)}")
        for pid, err in errors[:3]:
            print(f"  {pid}: {err}")
    
    if results and save_viz:
        print("\nGenerating visualization...")
        with tqdm(total=2, desc="Saving", unit="step") as pbar:
            output_file = save_results_to_file(results)
            pbar.update(1)
            viz_file = generate_visualization(output_file)
            pbar.update(1)
        print(f"Open in browser: {viz_file.absolute()}")


def main():
    parser = argparse.ArgumentParser(description="Extract entities from paper abstracts")
    parser.add_argument("-n", "--limit", type=int, default=5, help="Max papers to process")
    parser.add_argument("--no-viz", action="store_true", help="Skip visualization")
    parser.add_argument("--reprocess", action="store_true", help="Re-process already extracted papers")
    parser.add_argument("--status", action="store_true", help="Show DB status only")
    args = parser.parse_args()
    
    if args.status:
        db = get_db()
        show_db_status(db)
        return
    
    process_papers(limit=args.limit, save_viz=not args.no_viz, reprocess=args.reprocess)


if __name__ == "__main__":
    main()