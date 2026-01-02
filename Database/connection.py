from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import sys

# boilerplate for adding a mongoBD connection
load_dotenv()

mongo_user = os.getenv('mongo_DB_user')
mongo_pass = os.getenv('mongo_DB_pass')
mongo_host = os.getenv('mongo_DB_host', 'research-eco-cluster.pnzjjwe.mongodb.net')

if not mongo_user or not mongo_pass:
    print("Error: mongo_DB_user or mongo_DB_pass not set in .env. Add them locally and re-run.")
    sys.exit(1)

user_enc = quote_plus(mongo_user)
pass_enc = quote_plus(mongo_pass)

uri = f"mongodb+srv://{user_enc}:{pass_enc}@{mongo_host}/?appName=research-eco-cluster"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection (do not log credentials)
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"MongoDB connection error: {e}")
    sys.exit(1)
