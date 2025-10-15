#!/usr/bin/env python3
"""
NYC Taxi ETL Pipeline - Essential Test Suite

This test suite validates the core ETL pipeline functionality
and demonstrates comprehensive testing for the presentation.
"""

import unittest
import pandas as pd
import sys
import os
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.extract import extract_trip_data, extract_zone_data
from src.transform import transform_trip_data, transform_zone_data, validate_data
from src.load import create_schema, load_zones, load_trips, verify_load


class TestETLPipeline(unittest.TestCase):
    """Comprehensive tests for the NYC Taxi ETL Pipeline."""
    
    def setUp(self):
        """Set up test fixtures with realistic data."""
        # Create sample trip data
        self.sample_trip_data = pd.DataFrame({
            'VendorID': [1, 2, 1],
            'tpep_pickup_datetime': ['2019-01-01 00:46:40', '2019-01-01 00:59:47', '2019-01-01 01:18:59'],
            'tpep_dropoff_datetime': ['2019-01-01 00:53:20', '2019-01-01 01:18:59', '2019-01-01 01:35:00'],
            'passenger_count': [1, 1, 2],
            'trip_distance': [1.5, 2.6, 3.2],
            'RatecodeID': [1, 1, 1],
            'store_and_fwd_flag': ['N', 'N', 'N'],
            'PULocationID': [151, 239, 246],
            'DOLocationID': [239, 246, 151],
            'payment_type': [1, 1, 2],
            'fare_amount': [7.0, 14.0, 12.5],
            'extra': [0.5, 0.5, 0.5],
            'mta_tax': [0.5, 0.5, 0.5],
            'tip_amount': [1.65, 1.0, 2.5],
            'tolls_amount': [0, 0, 0],
            'improvement_surcharge': [0.3, 0.3, 0.3],
            'total_amount': [9.95, 16.3, 16.3],
            'congestion_surcharge': [0, 0, 0]
        })
        
        # Create sample zone data
        self.sample_zone_data = pd.DataFrame({
            'LocationID': [151, 239, 246],
            'Borough': ['Manhattan', 'Queens', 'Queens'],
            'Zone': ['Central Park', 'Jamaica', 'JFK Airport'],
            'service_zone': ['Yellow Zone', 'Boro Zone', 'Airports']
        })
    
    def test_extract_trip_data_success(self):
        """Test successful trip data extraction."""
        with patch('src.extract.RAW_DATA_PATH', 'data/raw'):
            with patch('pandas.read_csv') as mock_read_csv:
                mock_read_csv.return_value = self.sample_trip_data
                
                result = extract_trip_data('yellow_tripdata_2019-01.csv')
                
                self.assertIsInstance(result, pd.DataFrame)
                self.assertEqual(len(result), 3)
                self.assertIn('VendorID', result.columns)
                self.assertIn('tpep_pickup_datetime', result.columns)
    
    def test_extract_zone_data_success(self):
        """Test successful zone data extraction."""
        with patch('src.extract.RAW_DATA_PATH', 'data/raw'):
            with patch('pandas.read_csv') as mock_read_csv:
                mock_read_csv.return_value = self.sample_zone_data
                
                result = extract_zone_data('taxi_zone_lookup.csv')
                
                self.assertIsInstance(result, pd.DataFrame)
                self.assertEqual(len(result), 3)
                self.assertIn('LocationID', result.columns)
                self.assertIn('Borough', result.columns)
    
    def test_transform_trip_data_datetime_conversion(self):
        """Test datetime column conversion."""
        result = transform_trip_data(self.sample_trip_data)
        
        # Check that datetime columns are properly converted
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['tpep_pickup_datetime']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['tpep_dropoff_datetime']))
    
    def test_transform_trip_data_computed_fields(self):
        """Test addition of computed fields."""
        result = transform_trip_data(self.sample_trip_data)
        
        # Check that computed fields are added
        self.assertIn('trip_duration_minutes', result.columns)
        self.assertIn('cost_per_mile', result.columns)
        self.assertIn('pickup_hour', result.columns)
        self.assertIn('pickup_date', result.columns)
        
        # Check that computed fields have reasonable values
        self.assertTrue((result['trip_duration_minutes'] >= 0).all())
        self.assertTrue((result['cost_per_mile'] > 0).all())
        self.assertTrue((result['pickup_hour'] >= 0).all())
        self.assertTrue((result['pickup_hour'] <= 23).all())
    
    def test_transform_zone_data_column_renaming(self):
        """Test zone data column renaming."""
        result = transform_zone_data(self.sample_zone_data)
        
        # Check that columns are renamed correctly
        expected_columns = ['location_id', 'borough', 'zone_name', 'service_zone']
        for col in expected_columns:
            self.assertIn(col, result.columns)
    
    def test_validate_data_valid_dataframe(self):
        """Test validation with valid dataframe."""
        result = validate_data(self.sample_trip_data)
        self.assertTrue(result)
    
    def test_validate_data_empty_dataframe(self):
        """Test validation with empty dataframe."""
        empty_df = pd.DataFrame()
        result = validate_data(empty_df)
        self.assertFalse(result)
    
    def test_load_schema_creation(self):
        """Test database schema creation."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        create_schema(mock_engine)
        
        # Verify that connection methods were called
        mock_connection.execute.assert_called()
        mock_connection.commit.assert_called()
    
    def test_load_zones_data_insertion(self):
        """Test zone data insertion."""
        mock_engine = MagicMock()
        
        with patch('pandas.DataFrame.to_sql') as mock_to_sql:
            load_zones(self.sample_zone_data, mock_engine)
            
            mock_to_sql.assert_called_once_with(
                'zones', 
                mock_engine, 
                if_exists='append', 
                index=False
            )
    
    def test_load_trips_batch_processing(self):
        """Test trip data batch processing."""
        mock_engine = MagicMock()
        
        with patch('pandas.DataFrame.to_sql') as mock_to_sql:
            load_trips(self.sample_trip_data, mock_engine, batch_size=1)
            
            # Should be called for each record (batch size 1)
            self.assertEqual(mock_to_sql.call_count, 3)
    
    def test_verify_load_execution(self):
        """Test load verification queries."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        # Mock the query results
        mock_connection.execute.return_value.scalar.return_value = 10
        mock_connection.execute.return_value.fetchone.return_value = (10, 10.5, 2.1, 15.0)
        
        verify_load(mock_engine)
        
        # Verify that queries were executed
        mock_connection.execute.assert_called()
    
    def test_data_quality_business_rules(self):
        """Test business rule validation."""
        # Test trip duration calculation
        result = transform_trip_data(self.sample_trip_data)
        
        # Check that trip duration is calculated correctly (within 1 minute tolerance)
        for i, row in result.iterrows():
            pickup = pd.to_datetime(self.sample_trip_data.iloc[i]['tpep_pickup_datetime'])
            dropoff = pd.to_datetime(self.sample_trip_data.iloc[i]['tpep_dropoff_datetime'])
            expected_duration = (dropoff - pickup).total_seconds() / 60
            
            # Allow for rounding differences in integer conversion
            self.assertLessEqual(abs(row['trip_duration_minutes'] - expected_duration), 1.0)
    
    def test_cost_per_mile_calculation(self):
        """Test cost per mile calculation accuracy."""
        result = transform_trip_data(self.sample_trip_data)
        
        for i, row in result.iterrows():
            expected_cost = row['fare_amount'] / row['trip_distance']
            self.assertAlmostEqual(row['cost_per_mile'], expected_cost, places=2)
    
    def test_pickup_hour_extraction(self):
        """Test pickup hour extraction accuracy."""
        result = transform_trip_data(self.sample_trip_data)
        
        for i, row in result.iterrows():
            pickup_time = pd.to_datetime(self.sample_trip_data.iloc[i]['tpep_pickup_datetime'])
            expected_hour = pickup_time.hour
            self.assertEqual(row['pickup_hour'], expected_hour)


class TestDataQualityValidation(unittest.TestCase):
    """Test data quality and business rules."""
    
    def test_invalid_trip_removal(self):
        """Test removal of invalid trips."""
        # Create data with invalid trip (pickup after dropoff)
        invalid_data = pd.DataFrame({
            'VendorID': [1, 1],
            'tpep_pickup_datetime': ['2019-01-01 10:00:00', '2019-01-01 10:00:00'],
            'tpep_dropoff_datetime': ['2019-01-01 09:00:00', '2019-01-01 11:00:00'],  # First is invalid
            'passenger_count': [1, 1],
            'trip_distance': [5.0, 5.0],
            'RatecodeID': [1, 1],
            'store_and_fwd_flag': ['N', 'N'],
            'PULocationID': [151, 151],
            'DOLocationID': [239, 239],
            'payment_type': [1, 1],
            'fare_amount': [10.0, 10.0],
            'extra': [0.5, 0.5],
            'mta_tax': [0.5, 0.5],
            'tip_amount': [2.0, 2.0],
            'tolls_amount': [0, 0],
            'improvement_surcharge': [0.3, 0.3],
            'total_amount': [13.3, 13.3],
            'congestion_surcharge': [0, 0]
        })
        
        result = transform_trip_data(invalid_data)
        
        # Should remove the invalid trip
        self.assertEqual(len(result), 1)
    
    def test_negative_fare_removal(self):
        """Test removal of trips with negative fares."""
        invalid_data = pd.DataFrame({
            'VendorID': [1, 1],
            'tpep_pickup_datetime': ['2019-01-01 10:00:00', '2019-01-01 10:00:00'],
            'tpep_dropoff_datetime': ['2019-01-01 10:30:00', '2019-01-01 10:30:00'],
            'passenger_count': [1, 1],
            'trip_distance': [5.0, 5.0],
            'RatecodeID': [1, 1],
            'store_and_fwd_flag': ['N', 'N'],
            'PULocationID': [151, 151],
            'DOLocationID': [239, 239],
            'payment_type': [1, 1],
            'fare_amount': [-10.0, 10.0],  # First is invalid
            'extra': [0.5, 0.5],
            'mta_tax': [0.5, 0.5],
            'tip_amount': [2.0, 2.0],
            'tolls_amount': [0, 0],
            'improvement_surcharge': [0.3, 0.3],
            'total_amount': [13.3, 13.3],
            'congestion_surcharge': [0, 0]
        })
        
        result = transform_trip_data(invalid_data)
        
        # Should remove the trip with negative fare
        self.assertEqual(len(result), 1)


def run_tests():
    """Run all tests and display results."""
    print("🧪 NYC TAXI ETL PIPELINE - TEST SUITE")
    print("="*60)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [TestETLPipeline, TestDataQualityValidation]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0
    print(f"Success rate: {success_rate:.1f}%")
    
    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('Exception:')[-1].strip()}")
    
    print("="*60)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)