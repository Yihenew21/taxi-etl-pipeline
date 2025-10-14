from sqlalchemy import create_engine
from src.config import DB_CONFIG
from src.extract import extract_trip_data, extract_zone_data
from src.transform import transform_trip_data, transform_zone_data, validate_data
from src.load import (
    get_db_connection_string,
    create_schema,
    load_zones,
    load_trips,
    verify_load,
)


def main():
    print("=" * 60)
    print("NYC TAXI ETL PIPELINE")
    print("=" * 60)

    try:
        # Step 1: Extract
        print("\n[STEP 1: EXTRACT]")
        df_trips = extract_trip_data("yellow_tripdata_2019-01.csv")
        df_zones = extract_zone_data("taxi_zone_lookup.csv")

        # Step 2: Transform
        print("\n[STEP 2: TRANSFORM]")
        df_trips = transform_trip_data(df_trips)
        df_zones = transform_zone_data(df_zones)

        # Step 3: Validate
        print("\n[STEP 3: VALIDATE]")
        if not validate_data(df_trips):
            raise ValueError("Data validation failed")

        # Step 4: Load
        print("\n[STEP 4: LOAD]")
        connection_string = get_db_connection_string()
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as conn:
            print("✓ Connected to PostgreSQL database")

        # Create schema and load data
        create_schema(engine)
        load_zones(df_zones, engine)
        load_trips(df_trips, engine)

        # Verify
        print("\n[STEP 5: VERIFY]")
        verify_load(engine)

        print("\n" + "=" * 60)
        print("✓ ETL Pipeline completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        raise


if __name__ == "__main__":
    main()
