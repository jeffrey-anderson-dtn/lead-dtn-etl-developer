"""
Data Generator for ETL Developer Assessment
Generates realistic agriculture data with intentional quality issues for testing.
"""

import os
import random
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# Configuration
YEARS = [2023, 2024, 2025]
CROPS = ["corn", "soybeans", "wheat"]
FIPS_CODES = [f"{i:05d}" for i in range(1001, 1011)]  # 10 counties
PARCELS_PER_COMBO = (10, 15)  # Range of parcels per county/crop/year

# Realistic yield ranges by crop (bushels per acre)
YIELD_RANGES = {
    "corn": (150, 220),
    "soybeans": (40, 65),
    "wheat": (45, 75),
}

# Realistic abandonment percentages by crop
ABANDONMENT_RANGES = {
    "corn": (1, 8),
    "soybeans": (2, 10),
    "wheat": (3, 12),
}

random.seed(42)  # For reproducibility


def generate_land_id():
    """Generate a unique land parcel ID."""
    return f"PARCEL-{uuid.uuid4().hex[:8].upper()}"


def generate_crop_yield_data():
    """Generate crop yield data with intentional quality issues."""
    records_by_year = {year: [] for year in YEARS}
    
    duplicate_candidates = []  # Track records for potential duplication
    
    for year in YEARS:
        for fips_cd in FIPS_CODES:
            for crop in CROPS:
                num_parcels = random.randint(*PARCELS_PER_COMBO)
                yield_range = YIELD_RANGES[crop]
                
                for _ in range(num_parcels):
                    land_id = generate_land_id()
                    land_area = round(random.uniform(80, 500), 2)
                    planted_area = round(land_area * random.uniform(0.7, 0.95), 2)
                    yield_val = round(random.uniform(*yield_range), 2)
                    
                    record = {
                        "crop_name": crop,
                        "land_id": land_id,
                        "fips_cd": fips_cd,
                        "yield": yield_val,
                        "yield_units": "bushels",
                        "land_area": land_area,
                        "planted_area": planted_area,
                        "area_units": "acres",
                    }
                    
                    records_by_year[year].append(record)
                    duplicate_candidates.append((year, record.copy()))
    
    # === INJECT DATA QUALITY ISSUES ===
    
    # Issue 1: Null values (5 records with null yield)
    for year in YEARS:
        if records_by_year[year]:
            for _ in range(2):
                idx = random.randint(0, len(records_by_year[year]) - 1)
                records_by_year[year][idx]["yield"] = None
    
    # Issue 2: Negative yields (4 records)
    negative_count = 0
    for year in YEARS:
        if records_by_year[year] and negative_count < 4:
            idx = random.randint(0, len(records_by_year[year]) - 1)
            if records_by_year[year][idx]["yield"] is not None:
                records_by_year[year][idx]["yield"] = round(random.uniform(-50, -10), 2)
                negative_count += 1
    
    # Issue 3: Duplicate primary keys (3 duplicates)
    for _ in range(3):
        year, record = random.choice(duplicate_candidates)
        # Modify some non-key fields to make it a "different" record with same PK
        dup_record = record.copy()
        dup_record["yield"] = round(record["yield"] * random.uniform(0.9, 1.1), 2) if record["yield"] else None
        dup_record["planted_area"] = round(record["planted_area"] * random.uniform(0.95, 1.05), 2)
        records_by_year[year].append(dup_record)
    
    return records_by_year


def generate_abandonment_data():
    """Generate county crop abandonment data with intentional quality issues."""
    records_by_year = {year: [] for year in YEARS}
    
    # Track which county we'll skip for referential integrity test
    missing_county_year = random.choice(YEARS)
    missing_county_fips = random.choice(FIPS_CODES)
    missing_crop = random.choice(CROPS)
    
    for year in YEARS:
        for fips_cd in FIPS_CODES:
            for crop in CROPS:
                # Issue 4: Missing county record (referential integrity)
                if (year == missing_county_year and 
                    fips_cd == missing_county_fips and 
                    crop == missing_crop):
                    print(f"  [Quality Issue] Skipping abandonment record for: "
                          f"year={year}, fips={fips_cd}, crop={crop}")
                    continue
                
                abandonment_range = ABANDONMENT_RANGES[crop]
                abandonment_pct = round(random.uniform(*abandonment_range), 2)
                
                # Calculate a plausible abandoned area based on typical county size
                avg_county_planted = random.uniform(5000, 20000)
                abandoned_area = round(avg_county_planted * (abandonment_pct / 100), 2)
                
                record = {
                    "crop_name": crop,
                    "fips_cd": fips_cd,
                    "abandoned_area": abandoned_area,
                    "abandonment_percent": abandonment_pct,
                }
                
                records_by_year[year].append(record)
    
    # Issue 5: Abandonment percent > 100 (2 records)
    for year in random.sample(YEARS, 2):
        if records_by_year[year]:
            idx = random.randint(0, len(records_by_year[year]) - 1)
            records_by_year[year][idx]["abandonment_percent"] = round(
                random.uniform(105, 150), 2
            )
            print(f"  [Quality Issue] Set abandonment > 100% for record in year {year}")
    
    # Issue 6: Duplicate primary key in abandonment (2 duplicates)
    for year in random.sample(YEARS, 2):
        if records_by_year[year]:
            original = random.choice(records_by_year[year])
            dup_record = original.copy()
            dup_record["abandonment_percent"] = round(
                original["abandonment_percent"] * random.uniform(0.8, 1.2), 2
            )
            records_by_year[year].append(dup_record)
            print(f"  [Quality Issue] Added duplicate abandonment PK in year {year}")
    
    return records_by_year


def save_partitioned_parquet(records_by_year, base_path, schema_fields):
    """Save data as year-partitioned Parquet files."""
    os.makedirs(base_path, exist_ok=True)
    
    for year, records in records_by_year.items():
        if not records:
            continue
            
        partition_path = os.path.join(base_path, f"harvest_year={year}")
        os.makedirs(partition_path, exist_ok=True)
        
        # Build PyArrow table
        columns = {field: [] for field in schema_fields}
        
        for record in records:
            for field in schema_fields:
                columns[field].append(record.get(field))
        
        table = pa.table(columns)
        
        file_path = os.path.join(partition_path, "data.parquet")
        pq.write_table(table, file_path)
        
        print(f"  Written {len(records)} records to {file_path}")


def main():
    print("=" * 60)
    print("ETL Assessment Data Generator")
    print("=" * 60)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    
    # Generate crop yield data
    print("\nGenerating crop yield data...")
    yield_data = generate_crop_yield_data()
    yield_fields = [
        "crop_name", "land_id", "fips_cd", "yield", 
        "yield_units", "land_area", "planted_area", "area_units"
    ]
    save_partitioned_parquet(
        yield_data, 
        os.path.join(data_dir, "crop_yield"),
        yield_fields
    )
    
    # Generate abandonment data
    print("\nGenerating county crop abandonment data...")
    abandonment_data = generate_abandonment_data()
    abandonment_fields = [
        "crop_name", "fips_cd", "abandoned_area", "abandonment_percent"
    ]
    save_partitioned_parquet(
        abandonment_data,
        os.path.join(data_dir, "county_crop_abandonment"),
        abandonment_fields
    )
    
    # Summary
    total_yield = sum(len(r) for r in yield_data.values())
    total_abandonment = sum(len(r) for r in abandonment_data.values())
    
    print("\n" + "=" * 60)
    print("Data Generation Complete!")
    print("=" * 60)
    print(f"\nCrop Yield Records: {total_yield}")
    print(f"Abandonment Records: {total_abandonment}")
    print(f"\nData written to: {data_dir}/")
    print("\nIntentional Quality Issues Included:")
    print("  - Null yield values")
    print("  - Negative yield values")
    print("  - Duplicate primary keys (both datasets)")
    print("  - Abandonment percent > 100%")
    print("  - Missing county in abandonment data (referential integrity)")


if __name__ == "__main__":
    main()
