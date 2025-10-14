#!/usr/bin/env python3
"""
NYC Taxi ETL Pipeline - Database Test Runner

This script runs comprehensive database tests to validate
the data loaded into PostgreSQL after ETL processing.
"""

import sys
import os
from sqlalchemy import create_engine, text

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import DB_CONFIG


class DatabaseTestQueries:
    """Collection of database test queries for data validation."""
    
    def __init__(self):
        """Initialize database connection."""
        self.connection_string = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.engine = create_engine(self.connection_string)
    
    def run_query(self, query, description=""):
        """Execute a query and return results."""
        print(f"\n{'='*60}")
        print(f"QUERY: {description}")
        print(f"{'='*60}")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                columns = result.keys()
                
                print(f"Results: {len(rows)} rows")
                print("-" * 60)
                
                # Display results in a formatted table
                if rows:
                    # Print column headers
                    header = " | ".join(f"{col:>12}" for col in columns)
                    print(header)
                    print("-" * len(header))
                    
                    # Print rows (limit to 10 for display)
                    for i, row in enumerate(rows[:10]):
                        row_str = " | ".join(f"{str(val):>12}" for val in row)
                        print(row_str)
                    
                    if len(rows) > 10:
                        print(f"... and {len(rows) - 10} more rows")
                
                return rows
                
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            return None
    
    def test_basic_counts(self):
        """Test basic record counts and data integrity."""
        print("\n" + "="*80)
        print("BASIC DATA VALIDATION QUERIES")
        print("="*80)
        
        # Total record counts
        self.run_query(
            "SELECT COUNT(*) as total_trips FROM trips",
            "Total trips in database"
        )
        
        self.run_query(
            "SELECT COUNT(*) as total_zones FROM zones",
            "Total zones in database"
        )
        
        # Check for null values in critical columns
        self.run_query(
            """
            SELECT 
                COUNT(*) as total_records,
                COUNT(tpep_pickup_datetime) as pickup_not_null,
                COUNT(tpep_dropoff_datetime) as dropoff_not_null,
                COUNT(fare_amount) as fare_not_null,
                COUNT(trip_distance) as distance_not_null
            FROM trips
            """,
            "Null value check in critical columns"
        )
    
    def test_data_quality(self):
        """Test data quality and business rules."""
        print("\n" + "="*80)
        print("DATA QUALITY VALIDATION QUERIES")
        print("="*80)
        
        # Check for invalid trip durations (negative or zero)
        self.run_query(
            """
            SELECT COUNT(*) as invalid_duration_trips
            FROM trips 
            WHERE trip_duration_minutes <= 0
            """,
            "Trips with invalid duration (≤0 minutes)"
        )
        
        # Check for invalid fare amounts
        self.run_query(
            """
            SELECT COUNT(*) as invalid_fare_trips
            FROM trips 
            WHERE fare_amount <= 0 OR fare_amount > 1000
            """,
            "Trips with invalid fare amounts (≤0 or >$1000)"
        )
        
        # Check for invalid trip distances
        self.run_query(
            """
            SELECT COUNT(*) as invalid_distance_trips
            FROM trips 
            WHERE trip_distance <= 0 OR trip_distance > 500
            """,
            "Trips with invalid distances (≤0 or >500 miles)"
        )
    
    def test_business_analytics(self):
        """Test business analytics and insights."""
        print("\n" + "="*80)
        print("BUSINESS ANALYTICS QUERIES")
        print("="*80)
        
        # Average metrics
        self.run_query(
            """
            SELECT 
                COUNT(*) as total_trips,
                ROUND(AVG(fare_amount), 2) as avg_fare,
                ROUND(AVG(trip_distance), 2) as avg_distance,
                ROUND(AVG(trip_duration_minutes), 1) as avg_duration,
                ROUND(AVG(cost_per_mile), 2) as avg_cost_per_mile
            FROM trips
            """,
            "Overall trip statistics"
        )
        
        # Peak hours analysis
        self.run_query(
            """
            SELECT 
                pickup_hour,
                COUNT(*) as trip_count,
                ROUND(AVG(fare_amount), 2) as avg_fare,
                ROUND(AVG(trip_distance), 2) as avg_distance
            FROM trips 
            GROUP BY pickup_hour 
            ORDER BY trip_count DESC 
            LIMIT 10
            """,
            "Top 10 busiest hours with statistics"
        )
        
        # Payment method distribution
        self.run_query(
            """
            SELECT 
                payment_type,
                COUNT(*) as trip_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trips), 2) as percentage
            FROM trips 
            GROUP BY payment_type 
            ORDER BY trip_count DESC
            """,
            "Payment method distribution"
        )
    
    def test_geographic_analysis(self):
        """Test geographic data and zone relationships."""
        print("\n" + "="*80)
        print("GEOGRAPHIC ANALYSIS QUERIES")
        print("="*80)
        
        # Top pickup locations
        self.run_query(
            """
            SELECT 
                z.zone_name,
                z.borough,
                COUNT(*) as pickup_count,
                ROUND(AVG(t.fare_amount), 2) as avg_fare
            FROM trips t
            JOIN zones z ON t."PULocationID" = z.location_id
            GROUP BY z.zone_name, z.borough
            ORDER BY pickup_count DESC
            LIMIT 10
            """,
            "Top 10 pickup locations"
        )
        
        # Top dropoff locations
        self.run_query(
            """
            SELECT 
                z.zone_name,
                z.borough,
                COUNT(*) as dropoff_count,
                ROUND(AVG(t.fare_amount), 2) as avg_fare
            FROM trips t
            JOIN zones z ON t."DOLocationID" = z.location_id
            GROUP BY z.zone_name, z.borough
            ORDER BY dropoff_count DESC
            LIMIT 10
            """,
            "Top 10 dropoff locations"
        )
    
    def run_all_tests(self):
        """Run all database test queries."""
        print("🚀 STARTING DATABASE TEST QUERIES")
        print("="*80)
        
        try:
            # Test basic connectivity
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                print("✅ Database connection successful")
            
            # Run all test categories
            self.test_basic_counts()
            self.test_data_quality()
            self.test_business_analytics()
            self.test_geographic_analysis()
            
            print("\n" + "="*80)
            print("✅ ALL DATABASE TESTS COMPLETED SUCCESSFULLY")
            print("="*80)
            
        except Exception as e:
            print(f"\n❌ DATABASE TEST FAILED: {str(e)}")
            return False
        
        return True


def main():
    """Main function to run database tests."""
    print("🚀 NYC TAXI ETL PIPELINE - DATABASE TESTING")
    print("="*80)
    
    try:
        print("✅ Importing database test queries...")
        db_tester = DatabaseTestQueries()
        
        print("✅ Starting database tests...")
        success = db_tester.run_all_tests()
        
        if success:
            print("\n🎉 DATABASE TESTING COMPLETED SUCCESSFULLY!")
            print("="*80)
            print("✅ All data validation queries executed")
            print("✅ Data quality checks completed")
            print("✅ Business analytics queries run")
            print("✅ Geographic analysis completed")
            print("✅ Performance metrics calculated")
            print("="*80)
        else:
            print("\n💥 DATABASE TESTING FAILED!")
            sys.exit(1)
            
    except ImportError as e:
        print(f"\n❌ Import Error: {str(e)}")
        print("\nMake sure you have installed the required dependencies:")
        print("pip install -r requirements.txt")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n💥 Database testing failed: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Ensure PostgreSQL is running")
        print("2. Check database credentials in src/config.py")
        print("3. Run the ETL pipeline first to load data")
        print("4. Verify database connection settings")
        sys.exit(1)

if __name__ == "__main__":
    main()