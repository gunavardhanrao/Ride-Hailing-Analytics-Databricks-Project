# Ride-Hailing Analytics Using Databricks

## 1. Project Overview

This project implements an end-to-end Ride-Hailing Data Analytics solution using Databricks and the Medallion Architecture (Bronze, Silver and Gold).

The project processes raw ride-hailing trip data through an automated data pipeline, performs data cleaning, deduplication and validation, and transforms reliable trip records into business-ready analytical datasets.

Using Databricks Lakeflow Spark Declarative Pipelines, Delta tables and SQL, the solution organizes data into three layers:

- **Bronze Layer:** Ingests raw trip data from CSV files using Auto Loader and a streaming table.
- **Silver Layer:** Removes duplicate trips, standardizes data and separates valid and invalid records.
- **Gold Layer:** Creates business-level summaries for monthly trends, vehicle performance, payment methods, city-level analysis and data quality.

The processed datasets are visualized using a three-page Databricks AI/BI dashboard covering Executive Overview, Vehicles & Payments Analytics, and Data Quality & Reconciliation.

## 2. Business Objective

The objective of this project is to transform raw ride-hailing transaction data into accurate, reliable and actionable business insights.

The solution aims to:

1. Build an automated data pipeline for ingesting and processing ride-hailing trip records.
2. Improve data reliability through deduplication, cleaning and validation.
3. Identify trip volumes, revenue trends, travel distances and platform earnings.
4. Analyze vehicle-type performance, customer ratings and payment behavior.
5. Monitor missing values, invalid records and overall pipeline data quality.
6. Ensure complete record accountability through data reconciliation.
7. Present business KPIs and operational insights through interactive dashboards.

The project demonstrates how data engineering and analytics can work together to convert raw operational data into meaningful business intelligence.

## 3. Technical Architecture & Data Flow

### 3.1 Architecture Overview

The project follows the Medallion Architecture, which organizes data processing into three layers: Bronze, Silver and Gold.

The end-to-end workflow is:

```text
Raw Ride-Hailing CSV Dataset
             |
             v
Databricks Unity Catalog Volume
mobility_catalog.trips.raw_files
             |
             v
BRONZE LAYER
bronze_ride_trips
(Streaming Table + Auto Loader)
             |
             v
SILVER LAYER
silver_trips_prepared
(Deduplication and Standardization)
             |
             +------------------------+
             |                        |
             v                        v
silver_trips_clean       silver_trips_rejected
(Valid Records)          (Invalid Records)
             |                        |
             v                        |
GOLD BUSINESS LAYER                  |
             |                        |
             +-- gold_monthly_trips    |
             +-- gold_vehicle_summary  |
             +-- gold_payment_summary  |
             +-- gold_city_summary     |
             |                        |
             +------------------------+
                          |
                          v
                 gold_data_quality
                          |
                          v
               Databricks AI/BI Dashboard
```

The data-quality summary also uses Bronze and Silver records to reconcile the number of raw, duplicate, valid and rejected trips.

### 3.2 Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Unified platform for data engineering and analytics |
| Unity Catalog | Organizes and governs the catalog, schema, tables and volumes |
| Databricks Volumes | Stores the raw CSV dataset |
| SQL | Implements ingestion, cleaning, validation and aggregations |
| Auto Loader | Incrementally discovers and ingests new files |
| Streaming Tables | Supports incremental Bronze ingestion |
| Materialized Views | Stores processed Silver and aggregated Gold results |
| Lakeflow Spark Declarative Pipelines | Defines and executes the transformation pipeline |
| Databricks AI/BI Dashboards | Visualizes business metrics and data-quality results |

### 3.3 Data Processing Workflow

**Step 1 — Raw Data Storage**

The ride-hailing CSV dataset is uploaded to a Unity Catalog Volume at:

`/Volumes/mobility_catalog/trips/raw_files/`

This separates source-file storage from the transformation logic.

**Step 2 — Bronze Ingestion**

Auto Loader reads the CSV data into the `bronze_ride_trips` streaming table.

The Bronze layer retains the source records and captures ingestion metadata, including source filenames and loading timestamps.

**Step 3 — Silver Preparation**

The `silver_trips_prepared` materialized view processes the Bronze records, removes duplicate trip IDs and prepares the data for validation.

**Step 4 — Silver Validation**

Prepared trips are separated into:

- `silver_trips_clean` — records that pass the implemented validation rules.
- `silver_trips_rejected` — records that fail validation.

**Step 5 — Gold Aggregation**

Clean trip records are transformed into four business summary views:

- `gold_monthly_trips`
- `gold_vehicle_summary`
- `gold_payment_summary`
- `gold_city_summary`

A fifth Gold view, `gold_data_quality`, summarizes data-quality checks and reconciliation results.

**Step 6 — Business Intelligence**

The Gold datasets are used to build three Databricks AI/BI dashboard pages:

1. Executive Overview
2. Vehicles & Payments Analytics
3. Data Quality & Reconciliation

This architecture separates raw data ingestion, data-quality processing and business analytics, making the pipeline easier to maintain, validate and explain.

## 4. Bronze Layer — Raw Data Ingestion

### 4.1 Objective

The Bronze layer is the entry point of the Medallion Architecture. Its purpose is to ingest raw ride-hailing trip records while preserving the original source information.

The dataset was uploaded to a Unity Catalog Volume and ingested using Databricks Auto Loader through a streaming table.

### 4.2 Data Source

| Property | Details |
|---|---|
| Source format | CSV |
| Storage | Databricks Unity Catalog Volume |
| Catalog | `mobility_catalog` |
| Schema | `trips` |
| Volume | `raw_files` |
| Source path | `/Volumes/mobility_catalog/trips/raw_files/` |
| Bronze table | `bronze_ride_trips` |
| Ingestion method | Auto Loader with `STREAM read_files()` |

### 4.3 Ingestion Process

The Bronze ingestion was implemented using a SQL-based Lakeflow Spark Declarative Pipeline.

The ingestion process performs the following operations:

1. Reads the CSV files from the Unity Catalog Volume.
2. Uses Auto Loader to support incremental file ingestion.
3. Preserves the raw trip records for downstream processing.
4. Captures the source filename using `_metadata.file_name`.
5. Records the ingestion timestamp using `current_timestamp()`.
6. Writes the incoming records into the Bronze streaming table.

The Bronze layer acts as the source for Silver transformations.

### 4.4 Ingestion Metadata

Two additional metadata columns are captured during ingestion:

| Column | Purpose |
|---|---|
| `source_file` | Identifies the source file from which each record was ingested |
| `loaded_at` | Records when the data was processed by the ingestion query |

These fields improve traceability and help identify the origin of records during troubleshooting.

### 4.5 Bronze Layer Validation

After ingestion, SQL checks were performed to verify the number of raw records, unique trip IDs, duplicate records and source files.

| Validation Metric | Result |
|---|---:|
| Total raw records | 1,214 |
| Unique trip IDs | 1,190 |
| Duplicate records | 24 |
| Source files | 1 |

The difference between total records and unique trip IDs confirms the presence of 24 duplicate trip records.

These duplicates are handled in the Silver preparation layer rather than being removed during Bronze ingestion.

### 4.6 Why Use a Streaming Table?

A streaming table supports incremental ingestion of new data.

Instead of manually reloading the entire CSV dataset whenever new files arrive, Auto Loader can discover and process new files as the pipeline runs.

This approach provides a foundation for scalable ingestion while retaining source-file traceability.

In this project, the pipeline successfully processed the initial CSV dataset containing 1,214 records.

## 5. Silver Layer — Data Cleaning, Deduplication & Validation

### 5.1 Objective

The Silver layer transforms raw ride-hailing records into reliable, structured datasets suitable for business analysis.

It performs three major operations:

1. Deduplication of trip records.
2. Data preparation and standardization.
3. Validation and separation of clean and rejected records.

The Silver layer consists of three materialized views:

- `silver_trips_prepared`
- `silver_trips_clean`
- `silver_trips_rejected`

### 5.2 Data Deduplication

The Bronze dataset contained 1,214 records, including multiple records with the same `trip_id`.

Deduplication was implemented using the SQL `ROW_NUMBER()` window function.

The function assigns a sequence number to records within each trip ID group, based on the ordering defined in the transformation query.

Only one record per `trip_id` is retained.

**Deduplication results:**

| Metric | Count |
|---|---:|
| Bronze records | 1,214 |
| Duplicate records removed | 24 |
| Unique trips retained | 1,190 |

After deduplication, the `silver_trips_prepared` materialized view contained 1,190 records with no remaining duplicate trip IDs.

### 5.3 Data Preparation and Standardization

The Silver preparation stage organizes raw fields into a consistent structure for downstream validation and analysis.

This stage helps ensure that trip records can be evaluated using standardized data types and consistent field representations.

Data preparation is performed before splitting records into clean and rejected datasets.

### 5.4 Data Validation

The prepared dataset is evaluated against the validation rules implemented in the Silver SQL transformations.

Records passing the required checks are written to:

`silver_trips_clean`

Records failing the required checks are written to:

`silver_trips_rejected`

This separation prevents invalid trip records from entering business-level Gold aggregations while preserving them for investigation.

### 5.5 Silver Layer Results

| Metric | Records |
|---|---:|
| Prepared trips | 1,190 |
| Clean trips | 1,168 |
| Rejected trips | 22 |
| Reconciliation difference | 0 |

The reconciliation calculation is:

**Prepared Trips = Clean Trips + Rejected Trips**

1,190 = 1,168 + 22

This confirms that every prepared trip was classified into either the clean or rejected dataset.

### 5.6 Data Quality Monitoring

The pipeline also monitors data-quality issues, including:

- Invalid trip distances
- Negative rate values
- Missing city information
- Missing payment methods
- Missing fares
- Missing customer IDs
- Missing customer ratings

These issue counts are recorded in the Gold data-quality summary.

Some issue categories may overlap, so the sum of individual issue counts should not be interpreted as the number of rejected records.

### 5.7 Why Maintain a Rejected Dataset?

Instead of deleting invalid records, the pipeline preserves them separately.

This approach provides several advantages:

- Invalid records remain available for investigation.
- Business aggregations can use validated trip records.
- Data-quality issues can be monitored independently.
- Record reconciliation remains transparent and auditable.

### 5.8 Silver Layer Outcome

The Silver layer successfully reduced 1,214 raw records to 1,190 unique trips and classified them into 1,168 clean and 22 rejected records.

The clean dataset serves as the source for the Gold business summary views.

## 6. Gold Layer — Business Aggregations & Analytics

### 6.1 Objective

The Gold layer transforms validated ride-hailing trip records into aggregated datasets that support business reporting, operational analysis and data-quality monitoring.

The project uses Databricks SQL and Lakeflow Spark Declarative Pipelines to create five Gold materialized views.

Four views provide business-level insights, while the fifth monitors pipeline data quality and reconciliation.

### 6.2 Gold Materialized Views

| Materialized View | Purpose |
|---|---|
| `gold_monthly_trips` | Analyzes monthly trip volumes, travel distances, revenue, driver payouts and platform earnings |
| `gold_city_summary` | Summarizes trip activity and business performance by city |
| `gold_vehicle_summary` | Analyzes vehicle-type performance, revenue, distance and customer ratings |
| `gold_payment_summary` | Analyzes payment methods, revenue and pending payment amounts |
| `gold_data_quality` | Monitors data-quality issues, duplicates, rejected records and reconciliation |

### 6.3 Monthly Trip Analytics

**Source:** `silver_trips_clean`

**Gold View:** `gold_monthly_trips`

This view aggregates validated trip records by month.

The monthly summary supports the analysis of:

- Total trips
- Total travel distance
- Net revenue
- Driver payouts
- Platform earnings

These metrics help identify changes in demand, financial performance and ride-hailing activity over time.

The Executive Overview dashboard uses this view to display overall KPIs and monthly trends.

### 6.4 City-Level Analytics

**Source:** `silver_trips_clean`

**Gold View:** `gold_city_summary`

This view summarizes ride-hailing performance at the city level.

City-level analysis helps compare geographical markets and identify differences in trip demand and revenue generation.

The dataset can support decisions about operational planning and market performance.

### 6.5 Vehicle Performance Analytics

**Source:** `silver_trips_clean`

**Gold View:** `gold_vehicle_summary`

This view aggregates trip activity by vehicle type.

It supports analysis of:

- Number of trips by vehicle type
- Total distance travelled
- Net revenue generated
- Average customer ratings

The vehicle summary contains five vehicle categories.

The highest trip volume was recorded for the Mini category, with 304 trips.

Vehicle comparisons help identify popular service categories and evaluate customer experience across different ride options.

### 6.6 Payment Method Analytics

**Source:** `silver_trips_clean`

**Gold View:** `gold_payment_summary`

This view aggregates ride-hailing transactions by payment method.

It supports analysis of:

- Number of trips by payment method
- Net revenue by payment method
- Pending payment amounts
- Distribution of payment preferences

The payment summary contains five categories, including Unknown.

UPI was the most frequently used payment method, accounting for 534 trips.

This analysis helps understand payment preferences and identify outstanding payment amounts.

### 6.7 Data Quality & Reconciliation

**Gold View:** `gold_data_quality`

Unlike the business summary views, the data-quality view uses information from multiple pipeline stages.

It measures:

- Raw records ingested
- Duplicate records removed
- Prepared records
- Clean and rejected records
- Invalid trip distances
- Negative rate values
- Missing values in important fields
- Reconciliation difference and status

**Verified reconciliation:**

| Metric | Count |
|---|---:|
| Bronze records | 1,214 |
| Duplicate records | 24 |
| Prepared records | 1,190 |
| Clean records | 1,168 |
| Rejected records | 22 |
| Reconciliation difference | 0 |
| Reconciliation status | PASSED |

The reconciliation logic verifies:

**Bronze Records = Clean Records + Rejected Records + Duplicate Records**

1,214 = 1,168 + 22 + 24

A zero reconciliation difference confirms that the pipeline accounts for all ingested records.

### 6.8 Business Metrics and Aggregation Rules

The Gold layer uses SQL aggregation functions to summarize trip-level data.

| Metric | Aggregation approach |
|---|---|
| Total Trips | Count of validated trips |
| Total Distance | Sum of trip distances |
| Net Revenue | Sum of the calculated net revenue field |
| Driver Payout | Sum of driver payout amounts |
| Platform Earnings | Sum of platform earnings |
| Average Customer Rating | Average of available customer ratings |
| Pending Amount | Sum of pending payment amounts |

Revenue, payouts and earnings should be interpreted according to the definitions implemented in the Gold SQL transformations.

The dashboard uses SUM aggregation across monthly summaries when calculating overall trip, distance and financial KPIs.

### 6.9 Avoiding Double Counting

The monthly, city, vehicle and payment summaries represent different aggregation levels.

For example:

- Monthly summary: one record per month.
- Vehicle summary: one record per vehicle type.
- Payment summary: one record per payment method.
- City summary: one record per city.

These summaries should not be joined directly on unrelated dimensions because doing so can duplicate aggregated values and inflate business metrics.

Instead, each Gold view is used independently for its corresponding dashboard visualizations.

### 6.10 Gold Layer Outcome

The Gold layer successfully produced five analytical materialized views.

These views provide a structured foundation for monitoring ride-hailing performance, customer experience, payment behavior and pipeline data quality.

By separating business aggregations from raw and cleaned records, the architecture makes dashboard development and metric validation more manageable.

## 7. Databricks AI/BI Dashboard Implementation

### 7.1 Dashboard Overview

An interactive analytics dashboard was developed using Databricks AI/BI Dashboards to visualize business performance and data-quality metrics from the Gold layer.

The published dashboard contains three pages:

1. Executive Overview
2. Vehicles & Payments Analytics
3. Data Quality & Reconciliation

The dashboard uses a dark-themed layout with KPI cards, charts, tables and interactive filtering.

### 7.2 Page 1 — Executive Overview

**Purpose:** Provide a high-level summary of ride-hailing business performance.

**Primary data source:** `gold_monthly_trips`

The page contains four KPI cards:

| KPI | Result |
|---|---:|
| Total Trips | 1,168 |
| Net Revenue | ₹237,150.56 |
| Total Distance | 15,769.1 km |
| Platform Earnings | ₹52,639.54 |

**Visualizations:**

- Monthly Trips — bar chart showing ride volumes across months.
- Monthly Net Revenue vs Platform Earnings — line chart comparing financial trends.
- Month Range Filter — allows users to analyze selected periods.

The month filter was tested successfully.

For example, selecting April 2025 updates the dashboard to show 110 trips, with the financial KPIs and visualizations reflecting the selected period.

This page helps business stakeholders understand overall demand, revenue generation and operational performance.

### 7.3 Page 2 — Vehicles & Payments Analytics

**Purpose:** Analyze vehicle-category performance and customer payment behavior.

**Primary data sources:**

- `gold_vehicle_summary`
- `gold_payment_summary`

The page includes summary KPIs for vehicle categories, total trips, payment methods and pending payments.

**Vehicle analytics:**

- Trip distribution by vehicle category
- Revenue by vehicle type
- Average customer rating by vehicle type
- Detailed vehicle performance table

**Payment analytics:**

- Payment method distribution
- Revenue by payment method
- Pending amounts by payment method
- Detailed payment summary table

**Vehicle trip distribution:**

| Vehicle Type | Trips |
|---|---:|
| Mini | 304 |
| Auto | 259 |
| Sedan | 248 |
| Bike | 245 |
| Premium | 112 |

Mini recorded the highest trip volume.

**Payment method distribution:**

| Payment Method | Trips |
|---|---:|
| UPI | 534 |
| Cash | 283 |
| Card | 214 |
| Wallet | 123 |
| Unknown | 14 |

UPI was the most frequently used payment method.

Total pending payments were approximately ₹18.7K.

This page helps stakeholders evaluate service-category demand, customer payment preferences and outstanding payments.

### 7.4 Page 3 — Data Quality & Reconciliation

**Purpose:** Monitor pipeline reliability and ensure that all incoming trip records are accounted for.

**Primary data source:** `gold_data_quality`

The page includes six KPI cards:

- Raw Records
- Prepared Records
- Clean Records
- Rejected Records
- Duplicate Records
- Reconciliation Difference

**Data-quality visualizations:**

1. Data-quality issues bar chart — compares counts of missing and invalid values.
2. Reconciliation donut chart — shows the distribution of clean, rejected and duplicate records.
3. Reconciliation summary table — displays record counts and validation results.
4. Reconciliation status indicator — highlights whether the record-count check passed.

**Reconciliation results:**

| Metric | Result |
|---|---:|
| Raw Records | 1,214 |
| Prepared Records | 1,190 |
| Clean Records | 1,168 |
| Rejected Records | 22 |
| Duplicate Records | 24 |
| Reconciliation Difference | 0 |
| Status | PASSED |

The reconciliation calculation is:

1,214 - (1,168 + 22 + 24) = 0

The dashboard uses conditional formatting to make the reconciliation status easier to identify.

The issue chart also highlights missing values and invalid data conditions. Individual issue counts can overlap and therefore should not be summed to calculate rejected records.

### 7.5 Dashboard Design Decisions

Several design decisions were implemented to improve usability and readability.

**KPI Cards**

Important business metrics are displayed prominently so users can understand overall performance without examining detailed tables.

**Appropriate Chart Selection**

- Bar charts compare categories and monthly trip volumes.
- Line charts display financial trends over time.
- Donut charts summarize record classifications.
- Tables provide detailed numerical information.

**Interactive Filtering**

A month range filter allows users to focus on specific reporting periods and updates the relevant Executive Overview metrics.

**Visual Consistency**

A consistent dark theme, aligned KPI cards and clear chart titles were used across the dashboard pages.

**Data Quality Transparency**

A separate data-quality page allows users to verify whether pipeline records are accounted for before relying on business insights.

### 7.6 Dashboard Outcome

The Databricks AI/BI dashboard successfully connects the Gold analytical datasets to business-facing visualizations.

It enables users to:

- Monitor trip volumes and financial performance.
- Compare vehicle-category performance.
- Analyze payment behavior and pending amounts.
- Investigate missing and invalid data.
- Verify pipeline record reconciliation.

The dashboard was published after the visualizations and interactive filtering were tested.

## 8. Data Validation, Reconciliation & Testing

### 8.1 Objective

Data validation and reconciliation are important steps in ensuring the reliability of a data pipeline.

In this project, record-count checks, deduplication results, Silver-layer classifications and dashboard metrics were reviewed to verify data consistency across the Medallion Architecture.

### 8.2 Bronze Layer Validation

The Bronze layer contained 1,214 raw trip records.

The initial record-count checks identified:

| Metric | Count |
|---|---:|
| Total Raw Records | 1,214 |
| Unique Trip IDs | 1,190 |
| Duplicate Records | 24 |

The difference between raw records and unique trip IDs was 24.

These duplicate records were handled in the Silver preparation layer.

### 8.3 Silver Layer Validation

After deduplication, the Silver preparation dataset contained 1,190 unique trips.

The validation process classified the prepared records into two datasets:

| Dataset | Records |
|---|---:|
| `silver_trips_clean` | 1,168 |
| `silver_trips_rejected` | 22 |
| Total | 1,190 |

The classification check confirmed:

1,190 = 1,168 + 22

This verifies that all prepared records were accounted for in the clean and rejected datasets.

### 8.4 End-to-End Record Reconciliation

The pipeline uses record reconciliation to verify that all ingested records are accounted for after deduplication and validation.

**Reconciliation formula:**

```text
Reconciliation Difference =
Raw Records - (Clean Records + Rejected Records + Duplicate Records)
```

**Actual calculation:**

```text
1,214 - (1,168 + 22 + 24) = 0
```

| Validation Check | Result |
|---|---|
| Raw records | 1,214 |
| Clean records | 1,168 |
| Rejected records | 22 |
| Duplicate records | 24 |
| Reconciliation difference | 0 |
| Reconciliation status | PASSED |

A reconciliation difference of zero confirms that the pipeline accounts for all raw records through the clean, rejected and duplicate classifications.

### 8.5 Data Quality Checks

The project monitors several data-quality conditions, including:

- Invalid trip distances
- Negative rate values
- Missing customer IDs
- Missing customer ratings
- Missing fares
- Missing payment methods
- Missing city information

These checks help identify incomplete or potentially incorrect records.

Not every monitored issue necessarily causes a record to be rejected. Rejection depends on the validation rules implemented in the Silver layer.

Additionally, a single trip can contain multiple issues. Therefore, issue counts should not be summed to determine the number of rejected records.

### 8.6 Gold Layer Validation

The Gold layer contains five materialized views:

| Gold View | Expected Summary |
|---|---|
| `gold_monthly_trips` | 12 monthly records |
| `gold_vehicle_summary` | 5 vehicle categories |
| `gold_payment_summary` | 5 payment categories |
| `gold_city_summary` | 11 cities |
| `gold_data_quality` | 1 data-quality summary record |

The business summaries were used to calculate the dashboard KPIs and visualizations.

The Gold data-quality summary was used to monitor pipeline record counts and reconciliation status.

### 8.7 Dashboard Testing

The Databricks AI/BI dashboard was reviewed to confirm that its visualizations displayed the expected metrics.

**Executive Overview**

The dashboard displayed:

- Total Trips: 1,168
- Net Revenue: ₹237,150.56
- Total Distance: 15,769.1 km
- Platform Earnings: ₹52,639.54

**Interactive Filter Testing**

The month range filter was tested by selecting April 2025.

The filtered dashboard displayed 110 trips, and the related KPI cards and charts updated for the selected period.

**Data Quality Dashboard**

The Data Quality & Reconciliation page displayed:

- Raw records: 1,214
- Prepared records: 1,190
- Clean records: 1,168
- Rejected records: 22
- Duplicate records: 24
- Reconciliation difference: 0
- Status: PASSED

### 8.8 Validation Outcome

The pipeline successfully accounted for all 1,214 ingested trip records.

After removing 24 duplicate records and separating 22 rejected records, 1,168 clean trips were available for business analysis.

The reconciliation check returned a difference of zero, and the dashboard displayed the expected record counts and business KPIs.

These checks provide confidence in the pipeline's record accounting and the consistency of its reported results.

## 9. Project Challenges, Solutions & Key Learnings

### 9.1 Handling Duplicate Trip Records

**Challenge:**

The Bronze dataset contained 1,214 records, but only 1,190 unique trip IDs.

Duplicate trip records could inflate trip counts and financial metrics if included in downstream aggregations.

**Solution:**

Implemented deduplication in the Silver preparation layer using the SQL `ROW_NUMBER()` window function.

Records were partitioned by `trip_id`, and one record per trip was retained according to the query's ordering logic.

**Outcome:**

- Raw records: 1,214
- Duplicate records: 24
- Unique trips retained: 1,190

**Key Learning:**

Deduplication is essential for preventing double counting and maintaining accurate business metrics.

### 9.2 Separating Valid and Invalid Records

**Challenge:**

The dataset contained records with data-quality issues, including invalid distances, negative rates and missing values.

Using invalid records directly in business aggregations could produce misleading results.

**Solution:**

Implemented Silver-layer validation and separated records into:

- `silver_trips_clean` — records passing the implemented validation rules.
- `silver_trips_rejected` — records failing validation.

**Outcome:**

- Clean records: 1,168
- Rejected records: 22

**Key Learning:**

Preserving rejected records separately supports investigation and traceability without including them in validated business summaries.

### 9.3 Maintaining Data Reconciliation

**Challenge:**

After deduplication and validation, it was important to ensure that no records were left unaccounted for.

**Solution:**

Created a Gold data-quality summary to compare raw, duplicate, clean and rejected record counts.

Implemented the following reconciliation calculation:

```text
Raw Records - (Clean Records + Rejected Records + Duplicates)
```

**Outcome:**

```text
1,214 - (1,168 + 22 + 24) = 0

Reconciliation Status: PASSED
```

**Key Learning:**

Reconciliation provides a measurable way to verify record accounting across pipeline stages.

### 9.4 Designing Gold Tables at Different Aggregation Levels

**Challenge:**

The dashboard required monthly, city, vehicle and payment analysis.

These datasets represent different aggregation levels, and incorrectly combining them could inflate trip counts or revenue.

**Solution:**

Created separate Gold materialized views for each analytical dimension.

Each view was used independently for the appropriate dashboard metrics and visualizations.

**Key Learning:**

Understanding data grain is essential for accurate analytical modeling and preventing double counting.

### 9.5 Building Interactive Dashboard Filters

**Challenge:**

The Executive Overview needed to support analysis across different reporting periods rather than showing only overall totals.

**Solution:**

Implemented a month range filter in the Databricks AI/BI dashboard and connected it to the relevant KPI cards and charts.

**Outcome:**

Selecting April 2025 displayed 110 trips, with the corresponding metrics and visualizations updating for that period.

**Key Learning:**

Interactive filters improve analytical flexibility, but their behavior should be tested to ensure consistent reporting.

### 9.6 Improving Data Pipeline Traceability

**Challenge:**

A data pipeline should provide enough information to identify where records originated and when they were ingested.

**Solution:**

Included source metadata in the Bronze layer:

- `source_file` — source filename.
- `loaded_at` — ingestion processing timestamp.

**Key Learning:**

Capturing metadata improves traceability and supports future troubleshooting.

### 9.7 Key Technical Learnings

Through this project, I gained practical experience in:

1. Implementing Medallion Architecture using Databricks.
2. Managing raw data with Unity Catalog Volumes.
3. Using Auto Loader and streaming tables for incremental ingestion.
4. Building SQL transformations with Lakeflow Spark Declarative Pipelines.
5. Applying window functions for deduplication.
6. Implementing data-quality validation and rejected-record handling.
7. Creating Gold materialized views for business analytics.
8. Understanding data grain and avoiding double counting.
9. Performing end-to-end record reconciliation.
10. Building and testing interactive Databricks AI/BI dashboards.

### 9.8 Overall Project Outcome

The project successfully implemented an end-to-end ride-hailing analytics pipeline using Databricks.

The pipeline ingested 1,214 raw records, identified 24 duplicates, separated 22 rejected records and produced 1,168 clean trips for business analysis.

Five Gold materialized views supported a three-page analytics dashboard covering business performance, vehicle categories, payment behavior and data quality.

The final reconciliation difference was zero, confirming that all ingested records were accounted for.

## 10. Project Limitations & Future Enhancements

### 10.1 Current Project Limitations

Although the project successfully implements an end-to-end Databricks Medallion Architecture, it has some limitations.

**1. Limited Dataset Size**

The project processes 1,214 raw trip records.

While sufficient for demonstrating ingestion, transformation, validation and dashboard development, this dataset does not establish how the pipeline would perform with millions of daily transactions.

**2. Limited Historical Coverage**

The current dashboard analyzes trip activity across 12 months in 2025.

A longer historical dataset would support more comprehensive year-over-year comparisons and seasonal analysis.

**3. Batch-Style Source File Availability**

The source dataset was provided as CSV files in a Unity Catalog Volume.

Although Auto Loader supports incremental ingestion, the project has not demonstrated continuous real-time event ingestion from a live ride-hailing application.

**4. Data Quality Monitoring**

The pipeline identifies duplicate records, rejected records and several missing or invalid field conditions.

However, additional monitoring could be introduced for data freshness, unexpected volume changes, schema changes and recurring quality issues.

**5. Dashboard Analytics Scope**

The current dashboard focuses on descriptive analytics, including trip volumes, revenue, vehicle performance, payment methods and data quality.

It does not currently include demand forecasting, predictive analytics or live operational monitoring.

### 10.2 Future Enhancements

**1. Incremental Data Ingestion**

Extend the existing Auto Loader implementation to process newly arriving daily trip files and verify incremental updates across all pipeline layers.

**2. Pipeline Scheduling and Monitoring**

Introduce scheduled pipeline execution, failure alerts and operational monitoring to reduce manual intervention.

**3. Advanced Data Quality Framework**

Implement additional validation rules and automated checks for:

- Unexpected changes in record volumes
- Data freshness
- Schema changes
- Null-value thresholds
- Unusual revenue or distance values

**4. Historical and Comparative Analytics**

Expand the dataset to include multiple years and introduce:

- Year-over-year trip growth
- Revenue growth comparisons
- Seasonal demand analysis
- City-level growth trends

**5. Advanced Business Analytics**

Extend the Gold layer and dashboard to support:

- Revenue per trip
- Average trip distance
- Driver payout analysis
- Vehicle-category profitability
- Payment collection efficiency

**6. Performance and Scalability Testing**

Test the pipeline using larger datasets to evaluate processing time, resource consumption and transformation performance.

### 10.3 Conclusion

This project demonstrates an end-to-end data engineering and analytics workflow using Databricks.

By implementing Bronze, Silver and Gold layers, the solution transforms raw ride-hailing trip records into validated analytical datasets.

The pipeline successfully processes 1,214 raw records and produces 1,168 clean trips after accounting for duplicates and rejected records.

Five Gold materialized views support an interactive three-page Databricks AI/BI dashboard.

The project combines data ingestion, SQL transformations, data-quality validation, reconciliation and business intelligence into a single analytical workflow.

Future enhancements could extend the solution toward larger datasets, scheduled processing, advanced monitoring and more detailed business analytics.