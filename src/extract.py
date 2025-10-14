import pandas as pd
import os
from .config import RAW_DATA_PATH


def extract_trip_data(filename):
    """
    Extract trip data from CSV file.

    Args:
        filename: Name of the CSV file in data/raw/

    Returns:
        pd.DataFrame: Raw trip data
    """
    filepath = os.path.join(RAW_DATA_PATH, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    print(f"Extracting data from {filename}...")
    df = pd.read_csv(filepath)
    print(f"Extracted {len(df)} records")

    return df


def extract_zone_data(filename="taxi_zone_lookup.csv"):
    """Extract zone reference data."""
    filepath = os.path.join(RAW_DATA_PATH, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    print(f"Extracting zone data...")
    df = pd.read_csv(filepath)
    print(f"Extracted {len(df)} zones")

    return df
