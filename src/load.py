from sqlalchemy import create_engine, text
import pandas as pd
from .config import DB_CONFIG


def get_db_connection_string():
    """Create SQLAlchemy connection string."""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def create_schema(engine):
    """Create database schema."""
    print("Creating schema...")

    with engine.connect() as conn:
        # Drop existing tables (for development)
        conn.execute(text("DROP TABLE IF EXISTS trips CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS zones CASCADE"))
        conn.commit()

        # Create zones table
        conn.execute(
            text(
                """
            CREATE TABLE zones (
                location_id INTEGER PRIMARY KEY,
                borough VARCHAR(50),
                zone_name VARCHAR(100),
                service_zone VARCHAR(50)
            )
        """
            )
        )

        # Create trips table
        conn.execute(
            text(
                """
            CREATE TABLE trips (
                trip_id SERIAL PRIMARY KEY,
                "VendorID" INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DECIMAL(10, 2),
                "RatecodeID" INTEGER,
                store_and_fwd_flag VARCHAR(1),
                "PULocationID" INTEGER REFERENCES zones(location_id),
                "DOLocationID" INTEGER REFERENCES zones(location_id),
                payment_type INTEGER,
                fare_amount DECIMAL(10, 2),
                extra DECIMAL(10, 2),
                mta_tax DECIMAL(10, 2),
                tip_amount DECIMAL(10, 2),
                tolls_amount DECIMAL(10, 2),
                improvement_surcharge DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                congestion_surcharge DECIMAL(10, 2),
                trip_duration_minutes INTEGER,
                cost_per_mile DECIMAL(10, 2),
                pickup_hour INTEGER,
                pickup_date DATE
            )
        """
            )
        )

        conn.commit()

        conn.commit()

    print("Schema created successfully")


def load_zones(df_zones, engine):
    """Load zone reference data."""
    print("Loading zone data...")

    df_zones.to_sql("zones", engine, if_exists="append", index=False)

    print(f"Loaded {len(df_zones)} zones")


def load_trips(df_trips, engine, batch_size=10000):
    """Load trip data in batches."""
    print(f"Loading trip data ({len(df_trips)} records)...")

    # Don't include trip_id since it's auto-generated
    columns_to_load = [col for col in df_trips.columns if col != "trip_id"]

    # Load in batches to handle large datasets
    for i in range(0, len(df_trips), batch_size):
        batch = df_trips.iloc[i : i + batch_size][columns_to_load]
        batch.to_sql("trips", engine, if_exists="append", index=False)
        print(f"Loaded {min(i+batch_size, len(df_trips))}/{len(df_trips)} records")

    print("Trip data loaded successfully")


def verify_load(engine):
    """Verify data was loaded correctly."""
    print("\nVerifying data load...")

    with engine.connect() as conn:
        zones_count = conn.execute(text("SELECT COUNT(*) FROM zones")).scalar()
        trips_count = conn.execute(text("SELECT COUNT(*) FROM trips")).scalar()

        print(f"Zones in database: {zones_count}")
        print(f"Trips in database: {trips_count}")

        # Sample statistics
        stats = conn.execute(
            text(
                """
            SELECT 
                COUNT(*) as total_trips,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_minutes) as avg_duration
            FROM trips
        """
            )
        ).fetchone()
        print(f"\nTrip Statistics:")
        print(f"  Total trips: {stats[0]}")
        print(f"  Average fare: ${stats[1]:.2f}")
        print(f"  Average distance: {stats[2]:.2f} miles")
        print(f"  Average duration: {stats[3]:.1f} minutes")