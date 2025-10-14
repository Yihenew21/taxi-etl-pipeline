import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "taxi_db"),
    "user": os.getenv("DB_USER", "taxi_user"),
    "password": os.getenv("DB_PASSWORD", "taxi_password"),
}

# File paths
RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

# Batch size for processing
BATCH_SIZE = 10000
