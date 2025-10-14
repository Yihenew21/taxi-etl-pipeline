#!/usr/bin/env python3
"""
NYC Taxi ETL Pipeline - Test Runner

This script runs the essential test suite for the ETL pipeline.
Perfect for demonstrating testing capabilities in your presentation.
"""

import sys
import os

def main():
    """Run the ETL pipeline tests."""
    print("🚀 NYC TAXI ETL PIPELINE - TEST RUNNER")
    print("="*60)
    
    try:
        # Import and run tests
        from test_pipeline import run_tests
        
        print("✅ Starting ETL pipeline tests...")
        print("✅ Testing extract, transform, and load modules...")
        print("✅ Validating data quality and business rules...")
        print("✅ Checking computed fields and calculations...")
        print()
        
        success = run_tests()
        
        if success:
            print("\n🎉 ALL TESTS PASSED!")
            print("✅ ETL Pipeline is working correctly")
            print("✅ Data quality validation successful")
            print("✅ Business rules enforced")
            print("✅ Ready for production!")
        else:
            print("\n❌ SOME TESTS FAILED!")
            print("Please check the test output above for details")
            return False
            
    except ImportError as e:
        print(f"\n❌ Import Error: {str(e)}")
        print("\nMake sure you have installed the required dependencies:")
        print("pip install -r requirements.txt")
        return False
        
    except Exception as e:
        print(f"\n💥 Test execution failed: {str(e)}")
        return False
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
