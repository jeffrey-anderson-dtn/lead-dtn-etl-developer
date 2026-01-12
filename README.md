# ETL Developer Assessment: Agriculture Production Pipeline

## Overview

You are tasked with building a PySpark ETL pipeline that processes agriculture crop yield and abandonment data to compute production statistics at both the field and county levels.

## Time Limit

45 minutes

## Format

- This is a live, screen-shared assessment
- You may use any tools, including AI assistants (Copilot, ChatGPT, etc.)
- You must share your entire screen
- You may not use any other computers, phones, tablets, or other electronic devices while completing the assignment
- Please think aloud as you work — we want to understand your approach
- Be prepared to explain your decisions and debug any issues

## Input Data

You are provided with two datasets stored as partitioned Parquet files:

### 1. Crop Yield Data (`data/crop_yield/`)

Field-level crop yield information, partitioned by `harvest_year`.

| Column | Type | Description |
|--------|------|-------------|
| harvest_year | integer | Year of harvest (partition key) |
| crop_name | string | Type of crop (corn, soybeans, wheat) |
| land_id | string | Unique identifier for the land parcel |
| fips_cd | string | County FIPS code |
| yield | double | Yield amount |
| yield_units | string | Unit of yield (always "bushels") |
| land_area | double | Total land area |
| planted_area | double | Area planted with crop |
| area_units | string | Unit of area (always "acres") |

**Primary Key:** `harvest_year`, `crop_name`, `land_id`

### 2. County Crop Abandonment Data (`data/county_crop_abandonment/`)

County-level crop abandonment rates, partitioned by `harvest_year`.

| Column | Type | Description |
|--------|------|-------------|
| harvest_year | integer | Year of harvest (partition key) |
| crop_name | string | Type of crop |
| fips_cd | string | County FIPS code |
| abandoned_area | double | Total abandoned area in county |
| abandonment_percent | double | Percentage of planted area abandoned (0-100) |

**Primary Key:** `harvest_year`, `fips_cd`, `crop_name`

## Requirements

### Step 1: Data Ingestion

- Read all partitions from both Parquet datasets
- Handle the partitioned directory structure appropriately

### Step 2: Data Validation & Quality

Implement appropriate data quality checks and handle issues such as:
- Missing or null values
- Invalid data ranges
  - Percentages must be 0 .. 100
  - Area and yields cannot be negative
  - Planted area cannot exceed land_area
- Duplicate records
- Referential integrity between datasets

Document your approach to handling data quality issues.

### Step 3: Field-Level Calculations

Join the datasets and compute the following for each field (land parcel):

- **abandoned_area**: `planted_area * (abandonment_percent / 100)`
  - Uses the county-level abandonment percentage applied to the field's planted area
- **crop_production**: `(planted_area - abandoned_area) * yield`

Output schema should include all original crop_yield fields plus the calculated fields.

### Step 4: County-Level Rollup

Aggregate the field-level data to produce county-level statistics grouped by `harvest_year`, `fips_cd`, and `crop_name`:

| Metric | Description |
|--------|-------------|
| total_planted_area | Sum of planted_area |
| total_abandoned_area | Sum of abandoned_area |
| total_production | Sum of crop_production |
| county_yield | total_production / (total_planted_area - total_abandoned_area) |

### Step 5: Output

Save the results as Parquet files:
- Field-level results: `output/field_level_production/`
- County-level rollup: `output/county_rollup/`

## Deliverables

1. **PySpark script(s)** implementing the ETL pipeline
2. **Brief documentation** explaining:
   - Your approach to data quality handling
   - Any assumptions made
   - How to run your solution

## Evaluation Criteria

Your submission will be evaluated on:

1. **Correctness** - Accurate calculations and proper joins
2. **Data Quality Handling** - Appropriate validation and error handling
3. **Code Quality** - Clean, readable, well-organized code
4. **PySpark Best Practices** - Efficient use of Spark APIs
5. **Documentation** - Clear explanation of approach and assumptions

## Getting Started

### Option 1: GitHub Codespaces (Recommended)

Click the green "Code" button → "Codespaces" → "Create codespace on main"

The environment will automatically install Java, PySpark, and PyArrow. Test data is pre-generated in the `data/` directory.

Once ready (~2 minutes), verify your setup:
```bash
python verify_setup.py
```

### Option 2: Local Development

```bash
# Requires Java 8/11/17 installed
pip install pyspark pyarrow jupyter pandas great-expectations

# Verify setup (data is already in the repo)
python verify_setup.py

# Open the starter notebook
jupyter notebook etl_solution.ipynb

# Or run as a script
spark-submit your_solution.py
```

Good luck!
