# Vietnam Air Quality Data Pipeline

End-to-end AWS data pipeline for collecting, processing, and analyzing air quality data from 5 Vietnamese provinces using Medallion architecture (Bronze → Silver → Gold).

![ETL Pipeline Architecture](https://res.cloudinary.com/dptjhpkmv/image/upload/v1776009361/AWS-Project-CLF-Trang-2.drawio_1_im4ck4.png)

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Data Sources](#data-sources)
- [S3 Folder Structure](#s3-folder-structure)
- [System Components](#system-components)
- [Schema](#schema)
- [Installation & Deployment](#installation--deployment)
- [Data Quality Checks](#data-quality-checks)
- [Technical Notes](#technical-notes)

---

## Architecture Overview

```
EventBridge (3x/day)
        │
        ▼
Lambda: WAQI Ingestion ──────────────────────────────────────┐
        │                                                     │
        │ (one-time)                                          │
Kaggle CSV (manual upload)                                    │
        │                                                     │
        ▼                                                     ▼
┌───────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (S3)                          │
│  api_raw/ (partitioned: queried_city/year/month/day)              │
│  historical_csv/ (one-time Kaggle CSV)                            │
└───────────────────────────────────────────────────────────────────┘
        │                          │
        ▼                          ▼
 Glue Job:                  Glue Job:
 bronze_to_silver_api       bronze_to_silver_csv
 (incremental, bookmarked)  (one-time, flag-guarded)
        │                          │
        └──────────┬───────────────┘
                   ▼
┌───────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER (S3)                          │
│  reference/dim_station/                                           │
│  statistic/fact_aqi/ (partitioned: queried_city/year/month)       │
└───────────────────────────────────────────────────────────────────┘
                   │
                   ▼
        Lambda: DQ Check (Athena)
                   │
          ┌────────┴────────┐
          ▼                 ▼
    quality_passed      quality_failed
          │                 │
          ▼                 ▼
  Glue Job:              SNS Alert
  silver_to_gold         (stop pipeline)
          │
          ▼
┌───────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (S3)                           │
│  gold_aqi_daily_summary/  (partitioned: year/month)               │
│  gold_aqi_city_ranking/                                           │
│  gold_station_summary/    (partitioned: year/month)               │
└───────────────────────────────────────────────────────────────────┘
                   │
                   ▼
              SNS: Success
```

**Orchestration:** AWS Step Functions orchestrates the entire flow: API → Silver → DQ → Gold → SNS.

---

## Data Sources

### Source 1: WAQI API (real-time)
- **Endpoint:** `https://api.waqi.info/feed/{city}/?token={token}`
- **Frequency:** 3 times/day (ICT 08:00, 14:00, 20:00)
- **Cities:** `ha-noi`, `ho-chi-minh-city`, `da-nang`, `gia-lai`, `cao-bang`
- **Note:** Some stations (cao-bang, ho-chi-minh-city) may return stale data when offline

### Source 2: Kaggle CSV (historical)
- **File:** `historical_air_quality_2021_en.csv`
- **Scope:** Year 2021, 24 stations, 5 provinces
- **Run once:** Flag guard at `s3://{silver_bucket}/_control/csv_ingestion_done.flag`

### AQI Pollution Level (Vietnam Standard)

| AQI | Level |
|-----|-------|
| 0–50 | Good |
| 51–100 | Moderate |
| 101–150 | Unhealthy for Sensitive Groups |
| 151–200 | Unhealthy |
| 201–300 | Very Unhealthy |
| 300+ | Hazardous |

---

## S3 Folder Structure

```
data-pipeline-bronze-ap-dev/
├── api_raw/
│   ├── queried_city=ha-noi/
│   │   └── year=2026/month=04/day=12/
│   │       └── ha-noi_2026-04-12T01-00-00Z.json
│   ├── queried_city=da-nang/
│   ├── queried_city=ho-chi-minh-city/
│   ├── queried_city=gia-lai/
│   └── queried_city=cao-bang/
└── historical_csv/
    └── historical_air_quality_2021_en.csv

data-pipeline-silver-ap-dev/
├── _control/
│   └── csv_ingestion_done.flag
├── reference/
│   └── dim_station/          ← Parquet, overwrite
└── statistic/
    └── fact_aqi/             ← Parquet, partitioned
        └── queried_city=ha-noi/year=2026/month=04/

data-pipeline-gold-ap-dev/
├── gold_aqi_daily_summary/   ← Parquet, partitioned year/month
├── gold_aqi_city_ranking/    ← Parquet, no partition
└── gold_station_summary/     ← Parquet, partitioned year/month
```

---

## System Components

### AWS Resources

| Service | Resource | Purpose |
|---------|----------|---------|
| S3 | `data-pipeline-bronze-ap-dev` | Raw data storage |
| S3 | `data-pipeline-silver-ap-dev` | Cleaned data storage |
| S3 | `data-pipeline-gold-ap-dev` | Analytics-ready storage |
| Glue Database | `glue-pipeline-bronze-dev` | Bronze catalog |
| Glue Database | `glue-pipeline-silver-dev` | Silver catalog |
| Glue Database | `glue-pipeline-gold-dev` | Gold catalog |
| Glue Table | `api_air_quality_json` | Bronze API JSON (Crawler) |
| Glue Table | `historical_air_quality_2021` | Bronze CSV (manual) |
| Glue Table | `dim_station` | Silver |
| Glue Table | `fact_aqi` | Silver |
| Glue Table | `gold_aqi_city_ranking` | Gold |
| Glue Table | `gold_aqi_daily_summary` | Gold |
| Glue Table | `gold_station_summary` | Gold |
| Lambda | `lambda_waqi_ingestion` | WAQI API crawling |
| Lambda | `dq_check_silver` | Data quality checks |
| Step Functions | `aq-pipeline` | Orchestration |
| EventBridge Schedule | `aq-ingestion-morning/afternoon/evening` | Trigger Lambda |
| SNS Topic | `data-pipeline-alerts-dev` | Alerts & notifications |
| Athena | — | DQ queries on Silver |
| QuickSight | — | Visualization |

### Glue Jobs

| Job | Script | Trigger | Bookmark |
|-----|--------|---------|----------|
| `bronze_to_silver_statistics_csv` | `bronze_to_silver_statistics_csv.py` | Manual (one-time) | Disabled |
| `bronze_to_silver_statistics_api` | `bronze_to_silver_statistics_api.py` | Step Functions | **Enabled** |
| `silver_to_gold_analytics` | `silver_to_gold_analytics.py` | Step Functions | Disabled |

---

## Schema

### Bronze: `api_air_quality_json`
```
status          string
data            struct (nested JSON from WAQI API)
queried_city    string  [partition]
year            string  [partition]
month           string  [partition]
day             string  [partition]
```

### Bronze: `historical_air_quality_2021`
```
station_id          string
aqi_index           string
location            string
station_name        string
url                 string
dominant_pollutant  string
co                  string
dew                 string
humidity            string
no2                 string
o3                  string
pressure            string
pm10                string
pm25                string
so2                 string
temperature         string
wind                string
data_time_s         string
data_time_tz        string
status              string
alert_level         string
```

### Silver: `dim_station`
```
waqi_idx        int   
station_name    string
queried_city    string
lat             double
lon             double
url             string
source          string  ('kaggle' | 'api')
```

### Silver: `fact_aqi`
```
waqi_idx            int
measured_at         timestamp
aqi                 double
dominant_pollutant  string
pm25                double
pm10                double
co                  double
no2                 double
o3                  double
so2                 double
humidity            double
temperature         double
pressure            double
wind                double
source              string  ('kaggle' | 'api')
ingested_at         string
queried_city        string  [partition]
year                string  [partition]
month               string  [partition]
```

### Gold: `gold_aqi_daily_summary`
```
queried_city        string
date                date
avg_aqi             double
max_aqi             double
min_aqi             double
avg_pm25            double
avg_pm10            double
avg_humidity        double
avg_temperature     double
station_count       int
record_count        int
dominant_pollutant  string  (mode of the day)
pollution_level     string  (based on max_aqi)
_aggregated_at      timestamp
year                string  [partition]
month               string  [partition]
```

### Gold: `gold_aqi_city_ranking`
```
queried_city        string
year                string
month               string
avg_aqi             double
max_aqi             double
min_aqi             double
avg_pm25            double
avg_pm10            double
record_count        int
days_good           int
days_moderate       int
days_sensitive      int
days_unhealthy      int
days_very_unhealthy int
days_hazardous      int
dominant_pollutant  string  (mode of the month)
pollution_level     string  (based on avg_aqi)
aqi_rank            int     (DENSE_RANK DESC — 1 = most polluted)
pm25_rank           int     (DENSE_RANK DESC)
_aggregated_at      timestamp
```

### Gold: `gold_station_summary`
```
waqi_idx            int
station_name        string
queried_city        string
lat                 double
lon                 double
year                string
month               string
avg_aqi             double
max_aqi             double
min_aqi             double
avg_pm25            double
avg_pm10            double
avg_humidity        double
avg_temperature     double
record_count        int
dominant_pollutant  string  (mode of station/month)
pollution_level     string  (based on avg_aqi)
rank_in_city        int     (DENSE_RANK within same city/month)
_aggregated_at      timestamp
year                string  [partition]
month               string  [partition]
```

---

## Installation & Deployment

### Prerequisites

- AWS CLI configured (`aws configure`)
- Python 3.9+
- IAM permissions: S3, Glue, Lambda, EventBridge, Step Functions, SNS, Athena

### 1. Create S3 Buckets

```bash
for bucket in bronze silver gold; do
  aws s3 mb s3://data-pipeline-${bucket}-ap-dev \
    --region ap-southeast-2
done
```
### 1.1 Create more bucket for Athena (If want using AWS Athena)
### 2. Create Glue Databases

```bash
for db in bronze silver gold; do
  aws glue create-database \
    --database-input "{\"Name\": \"glue-pipeline-${db}-dev\"}" \
    --region ap-southeast-2
done
```
### 3. Upload Glue Job Scripts to S3

```bash
aws s3 cp glue_jobs/bronze_to_silver_statistics/bronze_to_silver_statistics_csv.py \
  s3://aws-glue-assets-<account-id>-<region>/scripts/

aws s3 cp glue_jobs/bronze_to_silver_statistics/bronze_to_silver_statistics_api.py \
  s3://aws-glue-assets-<account-id>-<region>/scripts/

aws s3 cp glue_jobs/silver_to_gold_analytics.py \
  s3://aws-glue-assets-<account-id>-<region>/scripts/
```

### 4. Create Glue Jobs

Create each job in Glue Console with the following job parameters:

**bronze_to_silver_statistics_csv:**
```
--bronze_database   glue-pipeline-bronze-dev
--bronze_table      historical_air_quality_2021
--bronze_bucket     data-pipeline-bronze-ap-dev
--silver_bucket     data-pipeline-silver-ap-dev
--silver_database   glue-pipeline-silver-dev
--sns_topic_arn     arn:aws:sns:<region>:<account-id>:data-pipeline-alerts-dev
```

**bronze_to_silver_statistics_api:**
```
--bronze_database   glue-pipeline-bronze-dev
--bronze_table      api_air_quality_json
--silver_bucket     data-pipeline-silver-ap-dev
--silver_database   glue-pipeline-silver-dev
--stale_hours       48
--sns_topic_arn     arn:aws:sns:<region>:<account-id>:data-pipeline-alerts-dev
```
> ⚠️ **Enable Job Bookmark** for this job: Glue Console → Job details → Advanced → Job bookmark → Enable

**silver_to_gold_analytics:**
```
--silver_database   glue-pipeline-silver-dev
--gold_bucket       data-pipeline-gold-ap-dev
--gold_database     glue-pipeline-gold-dev
```

### 5. Deploy Lambda Functions

**lambda_waqi_ingestion** — Environment variables:
```
WAQI_API_TOKEN          = <your_token>
S3_BUCKET_BRONZE        = data-pipeline-bronze-ap-dev
SNS_ALERT_TOPIC_ARN     = arn:aws:sns:<region>:<account-id>:data-pipeline-alerts-dev
WAQI_CITIES             = ha-noi,ho-chi-minh-city,da-nang,gia-lai,cao-bang
```

**dq_check_silver** — Environment variables:
```
ATHENA_DATABASE         = glue-pipeline-silver-dev
ATHENA_OUTPUT_LOCATION  = s3://data-pipeline-silver-ap-dev/athena-results/
SNS_ALERT_TOPIC_ARN     = arn:aws:sns:<region>:<account-id>:data-pipeline-alerts-dev
DQ_MIN_ROW_COUNT        = 10
DQ_MAX_NULL_PERCENT     = 5.0
DQ_FRESHNESS_HOURS      = 48
DQ_SAMPLE_ROWS          = 1000
```

### 5.1 Run Lambda and Create Crawler
Run `lambda_waqi_ingestion` and then create/run a Glue Crawler for `s3://data-pipeline-bronze-ap-dev`

### Note: Run manually "bronze_to_silver_statistics_csv" job

### 6. Configure EventBridge Schedules

| Schedule | Cron (UTC) | ICT |
|----------|-----------|-----|
| `aq-ingestion-morning` | `0 1 * * ? *` | 08:00 |
| `aq-ingestion-afternoon` | `0 7 * * ? *` | 14:00 |
| `aq-ingestion-evening` | `0 13 * * ? *` | 20:00 |

### 7. Deploy Step Functions

Go to **Step Functions Console** → **Create state machine** → paste content from `step_functions/pipeline_orchestation.json`.

Update the ARNs in the file before deploying:
- `FunctionName`: ARN of `dq_check_silver`
- `TopicArn`: ARN of SNS topic

---

## Data Quality Checks

Lambda `dq_check_silver` runs **3 Athena queries** after each Silver Job:

| Check | Description | Threshold |
|-------|-------------|-----------|
| `row_count` | Sufficient data | Min 10 rows |
| `null_pct` | Null % for 4 critical columns | Max 5% |
| `aqi_range` | 0 ≤ AQI ≤ 500 | 0 violations |
| `city_coverage` | All 5 cities present | 0 missing |
| `source_validity` | Only `kaggle` or `api` | 0 invalid |
| `freshness` | New data within 48h | > 0 fresh rows |
| `dim_station_row_count` | dim_station has data | > 0 stations |
| `dim_station_city_coverage` | Stations cover 5 cities | 0 missing |

If any check fails → SNS alert + Step Functions stops, Gold Job does not run.

---

## Technical Notes

### Glue Catalog Schema Issues
Glue Crawler parses CSV numeric columns as `STRUCT<double: DOUBLE, string: STRING>` instead of native types. Job `bronze_to_silver_statistics_csv.py` handles this by checking schema dynamically and extracting the appropriate field.

### Job Bookmark
Job `bronze_to_silver_statistics_api` uses Job Bookmark to only process new files from Bronze. To reprocess all data: **Glue Console → Job → Action → Reset job bookmark**.

### dominant_pollutant Anomaly
Value `dominant_pollutant = 'aqi'` in CSV is an artifact from Excel (`#NAME?` → parsed incorrectly). Pipeline fixes this by assigning null to this value before writing to Silver.

### SNS Notifications
Both Bronze-to-Silver jobs support optional `--sns_topic_arn` parameter to send notifications when:
- CSV job is skipped (already processed)
- API job is skipped (no new records or all records are stale)
