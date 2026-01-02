"""
db.py - MongoDB connection and schema setup

Handles connecting to Mongo and making sure our collections
have the right validators set up.
"""

from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure

load_dotenv()


# --- Connection config from .env ---

MONGO_USER = os.getenv('mongo_DB_user')
MONGO_PASS = os.getenv('mongo_DB_pass')
MONGO_HOST = os.getenv('mongo_DB_host', 'research-eco-cluster.pnzjjwe.mongodb.net')
MONGO_URI = os.getenv('MONGO_URI')  # optional full URI


# --- Client setup ---

def get_client():
    """Build a MongoClient from env vars."""
    if MONGO_URI:
        # full URI provided, use directly
        uri = MONGO_URI
    elif MONGO_USER and MONGO_PASS:
        # build Atlas URI from parts
        user_enc = quote_plus(MONGO_USER)
        pass_enc = quote_plus(MONGO_PASS)
        uri = f"mongodb+srv://{user_enc}:{pass_enc}@{MONGO_HOST}/?appName=research-eco-cluster"
    else:
        # local fallback (probably won't work without local mongo running)
        uri = "mongodb://localhost:27017"
        print("Warning: No Mongo credentials found, trying localhost")
    
    return MongoClient(uri)


# --- Schema definitions ---

def get_papers_schema():
    """Schema for the main papers collection."""
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id", "title", "year", "journal", "impact", "open_access", "content", "tags"],
            "properties": {
                "_id": {"bsonType": "string"},
                "title": {"bsonType": "string"},
                "year": {"bsonType": ["int", "null"]},
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "properties": {
                            "name": {"bsonType": "string"},
                            "affiliation": {"bsonType": ["string", "null"]}
                        }
                    }
                },
                "journal": {
                    "bsonType": "object",
                    "required": ["name", "issn"],
                    "properties": {
                        "name": {"bsonType": ["string", "null"]},
                        "issn": {"bsonType": ["string", "null"]}
                    }
                },
                "impact": {
                    "bsonType": "object",
                    "required": ["citation_count", "citations_per_year", "classification", "influential_citations"],
                    "properties": {
                        "citation_count": {"bsonType": "int"},
                        "citations_per_year": {"bsonType": ["double", "int"]},
                        "classification": {"bsonType": "string"},
                        "influential_citations": {"bsonType": "int"}
                    }
                },
                "open_access": {
                    "bsonType": "object",
                    "required": ["is_oa", "pdf_url", "status"],
                    "properties": {
                        "is_oa": {"bsonType": "bool"},
                        "pdf_url": {"bsonType": ["string", "null"]},
                        "status": {"bsonType": ["string", "null"]}
                    }
                },
                "content": {
                    "bsonType": "object",
                    "required": ["abstract", "full_text_extracted", "local_path"],
                    "properties": {
                        "abstract": {"bsonType": ["string", "null"]},
                        "full_text_extracted": {"bsonType": "bool"},
                        "local_path": {"bsonType": ["string", "null"]}
                    }
                },
                "processing_status": {
                    "bsonType": "string",
                    "enum": ["pending_download", "downloaded", "pending_parse", "parsed", "failed", "no_pdf_available"]
                },
                "tags": {
                    "bsonType": "array",
                    "items": {"bsonType": "string"}
                }
            }
        }
    }


def get_snapshots_schema():
    """Schema for citation time-series tracking."""
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["paper_id", "snapshots"],
            "properties": {
                "paper_id": {"bsonType": "string"},
                "snapshots": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "required": ["date", "count"],
                        "properties": {
                            "date": {"bsonType": "string"},  # ISO format
                            "count": {"bsonType": "int"}
                        }
                    }
                }
            }
        }
    }


def get_features_schema():
    """Schema for extracted features from paper text."""
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["paper_id", "script_version", "data_points"],
            "properties": {
                "paper_id": {"bsonType": "string"},
                "script_version": {"bsonType": "string"},
                "data_points": {"bsonType": "object"}  # flexible
            }
        }
    }


# --- Schema setup ---

def setup_schema_validation(db_name='academic_language'):
    """
    Create collections with validators if they don't exist,
    or update existing ones.
    """
    client = get_client()
    db = client[db_name]

    validators = {
        "papers": get_papers_schema(),
        "snapshots": get_snapshots_schema(),
        "extracted_features": get_features_schema()
    }

    for coll_name, validator in validators.items():
        coll_list = db.list_collection_names()
        if coll_name not in coll_list:
            try:
                db.create_collection(coll_name, validator=validator)
                print(f"Created '{coll_name}' with schema")
            except CollectionInvalid:
                pass
        else:
            try:
                db.command("collMod", coll_name, validator=validator)
                print(f"Updated '{coll_name}' schema")
            except OperationFailure:
                # probably don't have permission, that's ok
                pass


def get_db(db_name='academic_language'):
    """Get a db handle, setting up schemas first."""
    client = get_client()
    db = client[db_name]
    setup_schema_validation(db_name)
    return db


# --- CLI ---

if __name__ == "__main__":
    setup_schema_validation()
