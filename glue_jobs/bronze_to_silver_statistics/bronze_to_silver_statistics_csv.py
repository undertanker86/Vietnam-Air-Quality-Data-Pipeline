"""
Glue Job: Bronze CSV → Silver (One-time load)
──────────────────────────────────────────────
Reads raw Kaggle CSV from Bronze, cleans and standardizes,
writes to Silver as:
  - reference/dim_station/   (overwrite)
  - statistic/fact_aqi/      (partitioned by queried_city/year/month)

Run ONCE manually. After success a flag file is written to S3
to prevent accidental re-runs.

Job Parameters:
    --JOB_NAME
    --silver_bucket     e.g. data-pipeline-silver-us1-dev
    --bronze_bucket     e.g. data-pipeline-bronze-us1-dev
    --silver_database   e.g. glue-pipeline-silver-dev
"""

import sys
import boto3
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType

# ── Setup ────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_bucket",
    "bronze_bucket",
    "bronze_database",
    "bronze_table",
    "silver_database",
    "sns_topic_arn",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger      = glueContext.get_logger()

SILVER_BUCKET   = args["silver_bucket"]
BRONZE_BUCKET   = args["bronze_bucket"]
BRONZE_DB       = args["bronze_database"]
BRONZE_TABLE    = args["bronze_table"]
SILVER_DB       = args["silver_database"]
SNS_TOPIC_ARN   = args.get("sns_topic_arn", "").strip()
INGESTED_AT     = datetime.now(timezone.utc).isoformat()

def send_sns_notification(subject: str, message: str):
    """Send SNS notification if topic ARN is configured."""
    if not SNS_TOPIC_ARN:
        logger.info("SNS topic not configured, skipping notification.")
        return
    try:
        region = SNS_TOPIC_ARN.split(":")[3]
        sns_client = boto3.client("sns", region_name=region)
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message,
        )
        logger.info(f"SNS notification sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}")

FLAG_KEY        = "_control/csv_ingestion_done.flag"
DIM_STATION_PATH = f"s3://{SILVER_BUCKET}/reference/dim_station/"
FACT_AQI_PATH    = f"s3://{SILVER_BUCKET}/statistic/fact_aqi/"
# Map station name keywords → queried_city slug
# More flexible than hardcoding Station ID
CITY_KEYWORDS_MAP = {
    "ho-chi-minh-city": ["Ho Chi Minh", "Hồ Chí Minh"],
    "ha-noi":           ["Hanoi", "Hà Nội"],
    "da-nang":          ["Da Nang", "Đà Nẵng"],
    "gia-lai":          ["Gia Lai"],
    "cao-bang":         ["Cao Bằng"],
}

# ── Guard: skip if already processed ─────────────────────────────────────────
s3 = boto3.client("s3")
try:
    s3.head_object(Bucket=SILVER_BUCKET, Key=FLAG_KEY)
    logger.info("CSV already processed (flag exists). Skipping.")
    send_sns_notification(
        subject="[AQ Pipeline] CSV Job Skipped — Already Processed",
        message=f"Glue Job: bronze_to_silver_statistics_csv\n\n"
                f"Status: SKIPPED\n"
                f"Reason: CSV data already processed (flag file exists)\n"
                f"Flag: s3://{SILVER_BUCKET}/{FLAG_KEY}\n"
                f"Time: {INGESTED_AT}"
    )
    job.commit()
    sys.exit(0)
except s3.exceptions.ClientError:
    pass  # Flag does not exist → continue

# ── Step 1: Read from Glue Catalog (Bronze) ────────────────────────────────
logger.info(f"Reading from catalog: {BRONZE_DB}.{BRONZE_TABLE}")

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_csv_read",
)
df = datasource.toDF()

# No need to cast to string because Catalog already detected correct types:
# aqi index=double, pm2.5=double, co=double, humidity=double...
# Only need to handle string columns with values '-' or '#NAME?'


# ── Step 2: Filter only 5 cities by station name keyword ───────────────────
# Rename first to avoid Spark parsing column names with spaces incorrectly
df = df     .withColumnRenamed("station id",   "station_id_str")     .withColumnRenamed("station name", "station_name_raw")

# station id is bigint in Catalog → no need to filter != "" 
df = df.filter(F.col("station_id_str").isNotNull())        .filter(F.col("station_name_raw").isNotNull())


# Build CASE WHEN from CITY_KEYWORDS_MAP
def build_city_col():
    """Map station name → queried_city based on keyword match."""
    expr = None
    for city_slug, keywords in CITY_KEYWORDS_MAP.items():
        pattern = "|".join(keywords)
        cond = F.col("station_name_raw").rlike(pattern)
        if expr is None:
            expr = F.when(cond, F.lit(city_slug))
        else:
            expr = expr.when(cond, F.lit(city_slug))
    return expr.otherwise(F.lit(None))

df = df.withColumn("queried_city", build_city_col())        .filter(F.col("queried_city").isNotNull())

logger.info(f"Rows after filter 5 cities: {df.count()}")

# ── Step 3: Clean and cast types ──────────────────────────────────────────────
def clean_numeric(col_name: str, df_schema):
    """
    Glue Catalog sometimes parses CSV numeric columns as STRUCT<double: DOUBLE, string: STRING>,
    sometimes as native DOUBLE. Check schema to handle correctly.
    """
    field_type = dict(df_schema)[col_name]
    if "struct" in field_type.lower():
        # Extract .double from struct
        return F.col(f"{col_name}.double").cast(DoubleType())
    else:
        # Already double or string → cast directly
        return F.col(col_name).cast(DoubleType())


def clean_string(col_name: str):
    """Clean string column: replace '#NAME?' and '-' with null."""
    c = F.col(col_name).cast(StringType())
    return F.when(
        c.isin("-", "", "#NAME?", "N/A"),
        F.lit(None)
    ).otherwise(c)

# Glue Catalog lowercased column names and replaced spaces
# station id, aqi index, station name, url, location
# dominent pollutant, co, dew, humidity, no2, o3, pressure, pm10, pm2.5, so2
# temperature, wind, data time s, data time tz
# Rename remaining columns with spaces/dots (station id and station name already renamed in Step 2)
df = df \
    .withColumnRenamed("aqi index",          "aqi_index_raw") \
    .withColumnRenamed("dominent pollutant", "dominant_pollutant_raw") \
    .withColumnRenamed("pm2.5",              "pm25_raw") \
    .withColumnRenamed("data time s",        "data_time_s_raw") \
    .withColumnRenamed("data time tz",       "data_time_tz_raw")

# Build schema map so clean_numeric knows the type of each column
schema_map = dict(df.dtypes)

df = df.select(
    # IDs
    F.col("station_id_str").cast(IntegerType()).alias("waqi_idx"),
    F.col("queried_city"),

    # Station info
    F.col("station_name_raw").alias("station_name"),
    F.col("url").alias("url"),
    F.split(F.col("location"), ",").getItem(0).cast(DoubleType()).alias("lat"),
    F.split(F.col("location"), ",").getItem(1).cast(DoubleType()).alias("lon"),

    # Timestamp — format +07:00 requires xxx not z
    F.to_timestamp(
        F.concat(F.col("data_time_s_raw"), F.lit(" "), F.col("data_time_tz_raw")),
        "yyyy-MM-dd HH:mm:ss xxx"
    ).alias("measured_at"),

    # AQI metrics
    clean_numeric("aqi_index_raw", schema_map).alias("aqi"),
    # dominant_pollutant 'aqi' is invalid value (from '-' being parsed incorrectly) → null
    F.when(
        clean_string("dominant_pollutant_raw").isin("aqi", ""),
        F.lit(None)
    ).otherwise(clean_string("dominant_pollutant_raw")).alias("dominant_pollutant"),
    clean_numeric("pm25_raw", schema_map).alias("pm25"),
    clean_numeric("pm10", schema_map).alias("pm10"),
    clean_numeric("co", schema_map).alias("co"),
    clean_numeric("no2", schema_map).alias("no2"),
    clean_numeric("o3", schema_map).alias("o3"),
    clean_numeric("so2", schema_map).alias("so2"),
    clean_numeric("humidity", schema_map).alias("humidity"),
    clean_numeric("temperature", schema_map).alias("temperature"),
    # pressure can be STRUCT or string
    F.when(
        F.col("pressure").cast(StringType()).isNotNull(),
        F.regexp_replace(F.col("pressure").cast(StringType()), ",", "").cast(DoubleType())
    ).otherwise(F.lit(None)).alias("pressure"),
    clean_numeric("wind", schema_map).alias("wind"),

    # Metadata
    F.lit("kaggle").alias("source"),
    F.lit(INGESTED_AT).alias("ingested_at"),
)

# ── Step 4: Dedup by (waqi_idx, measured_at) ───────────────────────────────
from pyspark.sql.window import Window

w  = Window.partitionBy("waqi_idx", "measured_at").orderBy(F.col("ingested_at").desc())
df = df.withColumn("_rn", F.row_number().over(w))        .filter(F.col("_rn") == 1)        .drop("_rn")
logger.info(f"Rows after dedup: {df.count()}")

# ── Step 5: Build dim_station ────────────────────────────────────────────────
logger.info("Building dim_station...")

dim_station = df.select(
    "waqi_idx", "station_name", "queried_city", "lat", "lon", "url",
    F.lit("kaggle").alias("source")
).dropDuplicates(["waqi_idx"])

dim_station_dynf = DynamicFrame.fromDF(dim_station, glueContext, "dim_station")

sink_dim = glueContext.getSink(
    connection_type="s3",
    path=DIM_STATION_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink_dim.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName="dim_station")
sink_dim.setFormat("glueparquet", compression="snappy")
sink_dim.writeFrame(dim_station_dynf)
logger.info(f"dim_station written: {dim_station.count()} stations")

# ── Step 6: Build fact_aqi ───────────────────────────────────────────────────
logger.info("Building fact_aqi...")

# Add partition columns from measured_at

fact_aqi = df.select(
    "waqi_idx", "measured_at",
    "aqi", "dominant_pollutant",
    "pm25", "pm10", "co", "no2", "o3", "so2",
    "humidity", "temperature", "pressure", "wind",
    "source", "ingested_at",
    # Partition keys
    F.col("queried_city"),
    F.year("measured_at").cast("string").alias("year"),
    F.month("measured_at").cast("string").alias("month"),
).filter(F.col("measured_at").isNotNull()) .filter(F.col("aqi").isNotNull()) .filter(F.col("dominant_pollutant").isNotNull())


fact_aqi_dynf = DynamicFrame.fromDF(fact_aqi, glueContext, "fact_aqi_csv")

sink_fact = glueContext.getSink(
    connection_type="s3",
    path=FACT_AQI_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["queried_city", "year", "month"],
)
sink_fact.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName="fact_aqi")
sink_fact.setFormat("glueparquet", compression="snappy")
sink_fact.writeFrame(fact_aqi_dynf)
logger.info(f"fact_aqi written: {fact_aqi.count()} rows")

# ── Step 7: Write flag ───────────────────────────────────────────────────────
s3.put_object(
    Bucket=SILVER_BUCKET,
    Key=FLAG_KEY,
    Body=INGESTED_AT.encode(),
)
logger.info(f"Flag written: s3://{SILVER_BUCKET}/{FLAG_KEY}")

job.commit()
logger.info("Job CSV → Silver complete.")