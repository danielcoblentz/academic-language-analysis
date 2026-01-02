"""
visualize - create charts for jargon analysis and entity extraction
"""

import sys
import argparse
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from Database.db import get_db

OUTPUT_DIR = Path(__file__).parent / "output"


def get_jargon_data(db):
    papers = list(db['papers'].find({"jargon.score": {"$exists": True}}))
    return [{
        "title": p['title'][:50],
        "jargon": p['jargon']['score'],
        "citations": p['impact']['citation_count'],
        "classification": p['impact']['classification'],
        "year": p.get('year')
    } for p in papers]


def create_jargon_chart(data):
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>jargon vs citations</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>body { font-family: sans-serif; margin: 20px; }</style>
</head>
<body>
    <h1>jargon density vs citation count</h1>
    <div id="scatter"></div>
    <div id="by-class"></div>
    <script>
        var data = DATAHERE;
        
        var trace = {
            x: data.map(d => d.jargon * 100),
            y: data.map(d => d.citations),
            mode: 'markers',
            type: 'scatter',
            text: data.map(d => d.title),
            marker: {
                color: data.map(d => d.classification == 'HIGH' ? 'green' : d.classification == 'MODERATE' ? 'orange' : 'red'),
                size: 10
            }
        };
        
        Plotly.newPlot('scatter', [trace], {
            xaxis: {title: 'jargon density (%)'},
            yaxis: {title: 'citations', type: 'log'},
            hovermode: 'closest'
        });
        
        var high = data.filter(d => d.classification == 'HIGH').map(d => d.jargon * 100);
        var mod = data.filter(d => d.classification == 'MODERATE').map(d => d.jargon * 100);
        var low = data.filter(d => d.classification == 'LOW').map(d => d.jargon * 100);
        
        Plotly.newPlot('by-class', [
            {y: high, type: 'box', name: 'HIGH'},
            {y: mod, type: 'box', name: 'MODERATE'},
            {y: low, type: 'box', name: 'LOW'}
        ], {yaxis: {title: 'jargon density (%)'}});
    </script>
</body>
</html>"""
    
    html = html.replace("DATAHERE", json.dumps(data))
    out_path = OUTPUT_DIR / "jargon_analysis.html"
    out_path.write_text(html)
    return out_path


def create_extraction_viz():
    try:
        import langextract as lx
    except ImportError:
        print("langextract not installed")
        return None
    
    results_file = OUTPUT_DIR / "extraction_results.jsonl"
    if not results_file.exists():
        print("no extraction results found - run extract.py first")
        return None
    
    out_path = OUTPUT_DIR / "extraction_viz.html"
    html_content = lx.visualize(str(results_file))
    content = html_content.data if hasattr(html_content, 'data') else html_content
    out_path.write_text(content)
    return out_path


def print_summary(data):
    print(f"\nanalyzed {len(data)} papers")
    
    if not data:
        return
    
    avg_jargon = sum(d['jargon'] for d in data) / len(data)
    print(f"average jargon: {avg_jargon:.1%}")
    
    high = [d for d in data if d['classification'] == 'HIGH']
    low = [d for d in data if d['classification'] == 'LOW']
    
    if high and low:
        high_avg = sum(d['jargon'] for d in high) / len(high)
        low_avg = sum(d['jargon'] for d in low) / len(low)
        
        if high_avg < low_avg:
            print("trend: high-cited papers have less jargon")
        elif high_avg > low_avg:
            print("trend: high-cited papers have more jargon")


def prompt_user():
    print("\nvisualization options:")
    print("  1 - jargon analysis (citations vs jargon density)")
    print("  2 - entity extraction (langextract results)")
    print("  3 - both")
    print("  q - quit")
    
    choice = input("\nchoose [1/2/3/q]: ").strip().lower()
    
    if choice == '1':
        return True, False
    elif choice == '2':
        return False, True
    elif choice == '3':
        return True, True
    else:
        return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jargon", action="store_true", help="jargon analysis only")
    parser.add_argument("--extraction", action="store_true", help="extraction viz only")
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()
    
    # if no flags, prompt user
    if not args.jargon and not args.extraction:
        do_jargon, do_extraction = prompt_user()
        if do_jargon is None:
            print("exiting")
            return
    else:
        do_jargon = args.jargon
        do_extraction = args.extraction
    
    import webbrowser
    paths = []
    
    if do_jargon:
        print("connecting to database")
        db = get_db()
        
        print("fetching jargon data")
        data = get_jargon_data(db)
        
        if data:
            print("creating jargon chart")
            path = create_jargon_chart(data)
            paths.append(path)
            print_summary(data)
        else:
            print("no jargon data - run analyze_jargon.py first")
    
    if do_extraction:
        print("creating extraction visualization")
        path = create_extraction_viz()
        if path:
            paths.append(path)
    
    for p in paths:
        print(f"saved: {p}")
        if not args.no_open:
            webbrowser.open(str(p))


if __name__ == "__main__":
    main()
