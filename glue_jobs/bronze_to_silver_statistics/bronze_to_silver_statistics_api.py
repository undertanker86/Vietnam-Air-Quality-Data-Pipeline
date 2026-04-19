"""
Glue Job: Bronze API JSON → Silver (Daily incremental)
────────────────────────────────────────────────────────
Reads new WAQI JSON files from Bronze via Glue Catalog (Job Bookmark),
flattens nested structure, filters stale data, writes to Silver:
  - statistic/fact_aqi/      (append, partitioned by queried_city/year/month)
  - reference/dim_station/   (upsert — add new stations)

Job Parameters:
    --JOB_NAME
    --bronze_database   e.g. glue-pipeline-bronze-dev
    --bronze_table      e.g. api_raw
    --silver_bucket     e.g. data-pipeline-silver-ap-dev
    --silver_database   e.g. glue-pipeline-silver-dev
    --stale_hours       hours threshold (default: 48)
"""

import sys
from datetime import datetime, timezone, timedelta

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

# ── Setup ─────────────────────────────────────────────────────────────────────
import boto3

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_table",
    "silver_bucket",
    "silver_database",
    "sns_topic_arn",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
logger      = glueContext.get_logger()

BRONZE_DB        = args["bronze_database"]
BRONZE_TABLE     = args["bronze_table"]
SILVER_BUCKET    = args["silver_bucket"]
SILVER_DB        = args["silver_database"]
SNS_TOPIC_ARN    = args.get("sns_topic_arn", "").strip()
STALE_HOURS      = int(args.get("stale_hours", "48"))
NOW              = datetime.now(timezone.utc)
INGESTED_AT      = NOW.isoformat()
STALE_THRESHOLD  = NOW - timedelta(hours=STALE_HOURS)

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

DIM_STATION_PATH = f"s3://{SILVER_BUCKET}/reference/dim_station/"
FACT_AQI_PATH    = f"s3://{SILVER_BUCKET}/statistic/fact_aqi/"

logger.info(f"Bronze: {BRONZE_DB}.{BRONZE_TABLE}")
logger.info(f"Silver bucket: {SILVER_BUCKET}")
logger.info(f"Stale threshold: {STALE_THRESHOLD.isoformat()}")

# ── Step 1: Read from Glue Catalog (Job Bookmark tracks new files) ────────────
logger.info("Reading from Bronze catalog...")

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_api_read",  # bookmark key
)

df = datasource.toDF()
raw_count = df.count()
logger.info(f"Raw records: {raw_count}")

if raw_count == 0:
    logger.info("No new records. Committing.")
    send_sns_notification(
        subject="[AQ Pipeline] API Job Skipped — No New Records",
        message=f"Glue Job: bronze_to_silver_statistics_api\n\n"
                f"Status: SKIPPED\n"
                f"Reason: No new records found in Bronze layer\n"
                f"Bronze Table: {BRONZE_DB}.{BRONZE_TABLE}\n"
                f"Time: {INGESTED_AT}"
    )
    job.commit()
    sys.exit(0)

logger.info(f"Schema: {df.dtypes}")
logger.info(f"Sample: {df.limit(1).collect()}")

# ── Step 2: Flatten JSON structure ───────────────────────────────────────────
logger.info("Flattening JSON...")

# Helper: extract numeric from STRUCT or plain value
def extract_num(col_expr):
    """data.aqi is STRUCT<int: INT, string: STRING> → extract .int"""
    return col_expr.getField("int").cast(DoubleType())

def get_field_type(schema, path):
    """
    Get DataType of nested field from schema.
    path: "data.iaqi.pm25.v" → traverse each level
    """
    from pyspark.sql.types import StructType
    parts = path.split(".")
    current = schema
    for part in parts:
        if isinstance(current, StructType):
            field = current[part]
            current = field.dataType
        else:
            return None
    return current

def extract_iaqi(field, schema):
    """
    iaqi.{field}.v can be:
      - STRUCT<double, int>  → coalesce(.double, .int)
      - STRUCT<int>          → .int
      - plain INT/DOUBLE     → cast directly
    """
    from pyspark.sql.types import StructType
    v = F.col(f"data.iaqi.{field}.v")
    field_type = get_field_type(schema, f"data.iaqi.{field}.v")

    if field_type is None:
        return F.lit(None).cast(DoubleType())

    if isinstance(field_type, StructType):
        field_names = [f.name for f in field_type.fields]
        if "double" in field_names and "int" in field_names:
            return F.coalesce(
                v.getField("double").cast(DoubleType()),
                v.getField("int").cast(DoubleType()),
            )
        elif "double" in field_names:
            return v.getField("double").cast(DoubleType())
        elif "int" in field_names:
            return v.getField("int").cast(DoubleType())
        else:
            return v.cast(DoubleType())
    else:
        return v.cast(DoubleType())

df_flat = df.select(
    # Station info
    F.col("data.idx").cast(IntegerType()).alias("waqi_idx"),
    F.col("data.city.name").alias("station_name"),
    F.col("data.city.url").alias("url"),
    F.col("data.city.geo").getItem(0).cast(DoubleType()).alias("lat"),
    F.col("data.city.geo").getItem(1).cast(DoubleType()).alias("lon"),

    # queried_city from Hive partition
    F.col("queried_city"),

    # Timestamp
    F.to_timestamp(F.col("data.time.iso")).alias("measured_at"),

    # AQI — STRUCT<int: INT, string: STRING>
    extract_num(F.col("data.aqi")).alias("aqi"),
    F.col("data.dominentpol").alias("dominant_pollutant"),

    # iaqi — v is STRUCT<double, int> or plain int
    # Pass df.schema to check type dynamically
    extract_iaqi("pm25", df.schema).alias("pm25"),
    extract_iaqi("pm10", df.schema).alias("pm10"),
    extract_iaqi("co", df.schema).alias("co"),
    extract_iaqi("no2", df.schema).alias("no2"),
    extract_iaqi("o3", df.schema).alias("o3"),
    extract_iaqi("so2", df.schema).alias("so2"),
    extract_iaqi("h", df.schema).alias("humidity"),
    extract_iaqi("t", df.schema).alias("temperature"),
    extract_iaqi("p", df.schema).alias("pressure"),
    extract_iaqi("w", df.schema).alias("wind"),

    # Metadata
    F.lit("api").alias("source"),
    F.lit(INGESTED_AT).alias("ingested_at"),
)

logger.info(f"After flatten: {df_flat.count()} records")

# ── Step 3: Filter stale và invalid ──────────────────────────────────────────
logger.info(f"Filtering (stale>{STALE_HOURS}h, null aqi/dominant_pollutant)...")

stale_ts = STALE_THRESHOLD.strftime("%Y-%m-%d %H:%M:%S")

df_flat = df_flat \
    .filter(F.col("measured_at").isNotNull()) \
    .filter(F.col("aqi").isNotNull()) \
    .filter(F.col("dominant_pollutant").isNotNull()) \
    .filter(F.col("measured_at") >= F.lit(stale_ts).cast("timestamp"))

fresh_count = df_flat.count()
logger.info(f"Fresh records after filter: {fresh_count}")

if fresh_count == 0:
    logger.info("No fresh records. Committing.")
    send_sns_notification(
        subject="[AQ Pipeline] API Job Skipped — No Fresh Records",
        message=f"Glue Job: bronze_to_silver_statistics_api\n\n"
                f"Status: SKIPPED\n"
                f"Reason: All records are stale (older than {STALE_HOURS}h)\n"
                f"Raw records found: {raw_count}\n"
                f"Fresh records: 0\n"
                f"Stale threshold: {STALE_THRESHOLD.isoformat()}\n"
                f"Time: {INGESTED_AT}"
    )
    job.commit()
    sys.exit(0)

# ── Step 4: Dedup theo (waqi_idx, measured_at) ───────────────────────────────
w = Window.partitionBy("waqi_idx", "measured_at").orderBy(F.col("ingested_at").desc())
df_flat = df_flat.withColumn("_rn", F.row_number().over(w)) \
                 .filter(F.col("_rn") == 1) \
                 .drop("_rn")

logger.info(f"After dedup: {df_flat.count()} records")

# ── Step 5: Upsert dim_station ────────────────────────────────────────────────
logger.info("Updating dim_station...")

new_stations = df_flat.select(
    "waqi_idx", "station_name", "queried_city",
    "lat", "lon", "url",
    F.lit("api").alias("source")
).dropDuplicates(["waqi_idx"])

try:
    existing_dim = spark.read.parquet(DIM_STATION_PATH)
    new_only = new_stations.join(
        existing_dim.select("waqi_idx"),
        on="waqi_idx",
        how="left_anti"
    )
    merged_dim = existing_dim.union(new_only)
    logger.info(f"dim_station: {existing_dim.count()} existing + {new_only.count()} new")
except Exception:
    merged_dim = new_stations
    logger.info("dim_station does not exist, creating new from API data.")

dim_dynf = DynamicFrame.fromDF(merged_dim, glueContext, "dim_station")
sink_dim = glueContext.getSink(
    connection_type="s3",
    path=DIM_STATION_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink_dim.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName="dim_station")
sink_dim.setFormat("glueparquet", compression="snappy")
sink_dim.writeFrame(dim_dynf)
logger.info(f"dim_station written: {merged_dim.count()} total stations")

# ── Step 6: Write fact_aqi ────────────────────────────────────────────────────
logger.info("Writing fact_aqi...")

fact_aqi = df_flat.select(
    "waqi_idx", "measured_at",
    "aqi", "dominant_pollutant",
    "pm25", "pm10", "co", "no2", "o3", "so2",
    "humidity", "temperature", "pressure", "wind",
    "source", "ingested_at",
    F.col("queried_city"),
    F.year("measured_at").cast(StringType()).alias("year"),
    F.lpad(F.month("measured_at").cast(StringType()), 2, "0").alias("month"),
)

fact_dynf = DynamicFrame.fromDF(fact_aqi, glueContext, "fact_aqi_api")
sink_fact = glueContext.getSink(
    connection_type="s3",
    path=FACT_AQI_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["queried_city", "year", "month"],
)
sink_fact.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName="fact_aqi")
sink_fact.setFormat("glueparquet", compression="snappy")
sink_fact.writeFrame(fact_dynf)
logger.info(f"fact_aqi written: {fact_aqi.count()} rows")

job.commit()
logger.info("Job API JSON -> Silver complete.")



# Prepare partition keys
fact_aqi = df_final.withColumn("year", F.year("measured_at").cast("string")) \
                   .withColumn("month", F.lpad(F.month("measured_at"), 2, "0"))

# Write to S3 Silver with 'dynamic' partition overwrite mode
sink_fact = glueContext.getSink(
    connection_type="s3",
    path=FACT_AQI_PATH,
    partitionKeys=["queried_city", "year", "month"]
)
sink_fact.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName="fact_aqi")
sink_fact.writeFrame(DynamicFrame.fromDF(fact_aqi, glueContext, "fact_aqi"))