"""
Quick verification that PySpark is working correctly.
Run this first to confirm your environment is ready.
"""

from pyspark.sql import SparkSession

def main():
    print("Verifying PySpark setup...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SetupVerification") \
        .master("local[*]") \
        .getOrCreate()
    
    # Quick test
    df = spark.createDataFrame([
        ("corn", 150.5),
        ("soybeans", 45.2),
        ("wheat", 62.8)
    ], ["crop", "yield"])
    
    print("\n✓ Spark session created successfully")
    print(f"✓ Spark version: {spark.version}")
    
    # Verify parquet reading
    try:
        yield_df = spark.read.parquet("data/crop_yield")
        count = yield_df.count()
        print(f"✓ Parquet data loaded: {count} crop yield records")
    except Exception as e:
        print(f"✗ Could not read parquet data: {e}")
        print("  Run 'python generate_data.py' first if data is missing")
    
    spark.stop()
    print("\n🚀 Environment is ready! You can begin the assessment.\n")

if __name__ == "__main__":
    main()
