import yaml
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env
load_dotenv()

# Get database URL from environment variable
db_url = os.getenv('DB_URL')
if not db_url:
    raise ValueError("DB_URL not found in .env file")

# Connect to database
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Query to get all tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'raw_quotes'
""")

tables = cur.fetchall()

# Create the source definition
source_def = {
    "version": 2,
    "sources": [{
        "name": "anime_quotes",
        "database": "anime_quotes",
        "schema": "raw_quotes",
        "tables": [{"name": table[0]} for table in tables]
    }]
}

# Write to schema.yml
schema_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'staging', 'schema.yml')
with open(schema_path, 'w') as f:
    yaml.dump(source_def, f, sort_keys=False)