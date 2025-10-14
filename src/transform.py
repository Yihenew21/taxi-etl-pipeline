import pandas as pd
from datetime import datetime


def transform_trip_data(df):
    """
    Transform trip data:
    - Convert datetime columns
    - Remove invalid records
    - Add computed fields
    """
    print("Transforming trip data...")

    # Create a copy to avoid modifying original
    df = df.copy()

    # Convert datetime columns
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # Remove rows where pickup is after dropoff (invalid)
    initial_count = len(df)
    df = df[df["tpep_pickup_datetime"] <= df["tpep_dropoff_datetime"]]
    print(f"Removed {initial_count - len(df)} invalid records")

    # Remove rows with negative fare or trip distance
    df = df[(df["fare_amount"] > 0) & (df["trip_distance"] > 0)]

    # Calculate trip duration in minutes
    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
        / 60
    ).astype(int)

    # Calculate cost per mile
    df["cost_per_mile"] = (df["fare_amount"] / df["trip_distance"]).round(2)

    # Extract hour of pickup for analysis
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    # Handle missing values
    df = df.fillna(0)

    print(f"Transformed data: {len(df)} valid records")

    return df


def transform_zone_data(df):
    """Transform zone reference data."""
    print("Transforming zone data...")

    df = df.copy()

    # Rename columns for clarity
    df = df.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone_name",
            "service_zone": "service_zone",
        }
    )

    print(f"Transformed {len(df)} zone records")

    return df


def validate_data(df):
    """Perform data quality checks."""
    print("Validating data...")

    issues = []

    if df.empty:
        issues.append("Dataset is empty")

    if df.isnull().any().any():
        null_counts = df.isnull().sum()
        print(f"Null values found:\n{null_counts[null_counts > 0]}")

    return len(issues) == 0
