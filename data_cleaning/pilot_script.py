"""
pilot_script.py - Quick jargon scoring test

Calculates jargon density by comparing text against a common words list.
Jargon Score = (Total Words - Common Words) / Total Words
"""

import re
from collections import Counter
from pathlib import Path


# --- Jargon calculation ---

def calculate_jargon_score(text, common_words_set):
    """
    Count how many words aren't in our common words list.
    Only looks at words 3+ chars to skip tiny stuff.
    """
    text = text.lower()
    words = re.findall(r'\b[a-z]{3,}\b', text)
    
    if not words:
        return {"score": 0, "total_words": 0, "jargon_count": 0, "top_jargon": []}
    
    # anything not in common_words is "jargon"
    jargon_words = [w for w in words if w not in common_words_set]
    jargon_score = len(jargon_words) / len(words)
    
    return {
        "score": round(jargon_score, 4),
        "total_words": len(words),
        "jargon_count": len(jargon_words),
        "top_jargon": Counter(jargon_words).most_common(10)
    }


# --- Load word list ---

def load_common_words():
    """
    Try to load the common words dictionary.
    Falls back to a tiny set if file not found.
    """
    dict_path = Path(__file__).parent / 'dictionaries' / 'google-10000-english-usa-no-swears-medium.txt'
    
    try:
        with dict_path.open('r', encoding='utf-8') as f:
            content_lines = f.read().splitlines()
        return set(w.strip().lower() for w in content_lines if w.strip())
    except FileNotFoundError:
        print(f"Warning: dictionary not found at {dict_path}")
        print("Using tiny fallback list")
        return {'the', 'and', 'for', 'with', 'that', 'this', 'from', 'are', 'was', 'were'}


# --- Main ---

def main():
    common_words = load_common_words()
    
    sample = """We examined the spatial distribution of soil nutrients in desert ecosystems..."""
    
    results = calculate_jargon_score(sample, common_words)
    
    print(f"Jargon Density: {results['score']*100:.1f}%")
    print(f"Total words: {results['total_words']}")
    print(f"Jargon words: {results['jargon_count']}")
    print(f"Top jargon: {results['top_jargon']}")


if __name__ == "__main__":
    main()