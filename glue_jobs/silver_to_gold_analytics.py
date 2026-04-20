"""
Glue Job: Silver → Gold (Air Quality Analytics)
─────────────────────────────────────────────────
Reads cleansed fact_aqi from Silver, produces 2 Gold tables:

  1. gold_aqi_daily_summary  — Daily AQI summary per city (Dashboard/BI)
  2. gold_aqi_city_ranking   — Monthly city ranking by pollution (Reports)

Job Parameters:
    --JOB_NAME
    --silver_database   e.g. glue-pipeline-silver-dev
    --gold_bucket       e.g. data-pipeline-gold-ap-dev
    --gold_database     e.g. glue-pipeline-gold-dev
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Job Setup ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "gold_bucket",
    "gold_database",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger      = glueContext.get_logger()

SILVER_DB  = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB    = args["gold_database"]

logger.info(f"Silver DB: {SILVER_DB}")
logger.info(f"Gold bucket: {GOLD_BUCKET}")

# ── Read Silver fact_aqi ──────────────────────────────────────────────────────
logger.info("Reading Silver fact_aqi...")

fact_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name="fact_aqi",
    transformation_ctx="fact_aqi",
)
df = fact_dyf.toDF()
logger.info(f"Silver records: {df.count()}")

# Add date column from measured_at
df = df.withColumn("date", F.to_date(F.col("measured_at")))

# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 1: gold_aqi_daily_summary
# Daily AQI summary per city — for Dashboard/BI
# pollution_level calculated based on max_aqi per day (Vietnam standard)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: gold_aqi_daily_summary...")

# dominant_pollutant: get the most frequent pollutant per day
# subquery: GROUP BY city+date+pollutant → RANK → take rank=1
dominant_daily = df.groupBy("queried_city", "date", "dominant_pollutant") \
    .agg(F.count("*").alias("pollutant_count"))

# w_dominant = Window.partitionBy("queried_city", "date") \
#     .orderBy(F.col("pollutant_count").desc())
w_dominant = Window.partitionBy("queried_city", "date") \
    .orderBy(
        F.col("pollutant_count").desc(), 
        F.col("dominant_pollutant").asc() # If count are equal, choose by alphabetical order (PM10 > PM2.5)
    )
dominant_daily = dominant_daily \
    .withColumn("_rank", F.row_number().over(w_dominant)) \
    .filter(F.col("_rank") == 1) \
    .select(
        "queried_city",
        "date",
        F.col("dominant_pollutant").alias("dominant_pollutant_mode")
    )

# Aggregate daily metrics
daily = df.groupBy("queried_city", "date").agg(
    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
    F.max("aqi").alias("max_aqi"),
    F.min("aqi").alias("min_aqi"),
    F.round(F.avg("pm25"), 2).alias("avg_pm25"),
    F.round(F.avg("pm10"), 2).alias("avg_pm10"),
    F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    F.round(F.avg("temperature"), 2).alias("avg_temperature"),
    F.countDistinct("waqi_idx").alias("station_count"),
    F.count("*").alias("record_count"),
)

# Join dominant_pollutant
daily = daily.join(dominant_daily, on=["queried_city", "date"], how="left")

# Add pollution_level based on max_aqi (Vietnam standard)
daily = daily.withColumn(
    "pollution_level",
    F.when(F.col("max_aqi") <= 50,  "Good")
     .when(F.col("max_aqi") <= 100, "Moderate")
     .when(F.col("max_aqi") <= 150, "Unhealthy for Sensitive Groups")
     .when(F.col("max_aqi") <= 200, "Unhealthy")
     .when(F.col("max_aqi") <= 300, "Very Unhealthy")
     .otherwise("Hazardous")
).withColumnRenamed("dominant_pollutant_mode", "dominant_pollutant")

# Add partition columns
daily = daily \
    .withColumn("year",  F.year("date").cast("string")) \
    .withColumn("month", F.lpad(F.month("date").cast("string"), 2, "0"))

daily = daily.withColumn("_aggregated_at", F.current_timestamp())

logger.info(f"gold_aqi_daily_summary rows: {daily.count()}")

daily_path = f"s3://{GOLD_BUCKET}/gold_aqi_daily_summary/"
daily_dyf  = DynamicFrame.fromDF(daily, glueContext, "gold_aqi_daily_summary")

sink1 = glueContext.getSink(
    connection_type="s3",
    path=daily_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
)
sink1.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="gold_aqi_daily_summary")
sink1.setFormat("glueparquet", compression="snappy")
sink1.writeFrame(daily_dyf)
logger.info(f"gold_aqi_daily_summary written → {daily_path}")

# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 2: gold_aqi_city_ranking
# Monthly city ranking by pollution — for city comparison reports
# Built from gold_aqi_daily_summary to avoid recalculating from Silver
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: gold_aqi_city_ranking...")

# dominant_pollutant for month: get mode within the month
dominant_monthly = df.groupBy("queried_city", "year", "month", "dominant_pollutant") \
    .agg(F.count("*").alias("pollutant_count"))

w_dom_monthly = Window.partitionBy("queried_city", "year", "month") \
    .orderBy(F.col("pollutant_count").desc())

dominant_monthly = dominant_monthly \
    .withColumn("_rank", F.row_number().over(w_dom_monthly)) \
    .filter(F.col("_rank") == 1) \
    .select(
        "queried_city", "year", "month",
        F.col("dominant_pollutant").alias("dominant_pollutant_mode")
    )

# Aggregate monthly from daily (already has year/month partition)
monthly = daily.groupBy("queried_city", "year", "month").agg(
    F.round(F.avg("avg_aqi"), 2).alias("avg_aqi"),
    F.max("max_aqi").alias("max_aqi"),
    F.min("min_aqi").alias("min_aqi"),
    F.round(F.avg("avg_pm25"), 2).alias("avg_pm25"),
    F.round(F.avg("avg_pm10"), 2).alias("avg_pm10"),
    F.sum("record_count").alias("record_count"),
    # Count days by pollution level
    F.sum(F.when(F.col("pollution_level") == "Good", 1).otherwise(0))
     .alias("days_good"),
    F.sum(F.when(F.col("pollution_level") == "Moderate", 1).otherwise(0))
     .alias("days_moderate"),
    F.sum(F.when(F.col("pollution_level") == "Unhealthy for Sensitive Groups", 1).otherwise(0))
     .alias("days_sensitive"),
    F.sum(F.when(F.col("pollution_level") == "Unhealthy", 1).otherwise(0))
     .alias("days_unhealthy"),
    F.sum(F.when(F.col("pollution_level") == "Very Unhealthy", 1).otherwise(0))
     .alias("days_very_unhealthy"),
    F.sum(F.when(F.col("pollution_level") == "Hazardous", 1).otherwise(0))
     .alias("days_hazardous"),
)

# Join monthly dominant_pollutant
monthly = monthly.join(dominant_monthly, on=["queried_city", "year", "month"], how="left") \
    .withColumnRenamed("dominant_pollutant_mode", "dominant_pollutant")

# Monthly pollution_level based on avg_aqi
monthly = monthly.withColumn(
    "pollution_level",
    F.when(F.col("avg_aqi") <= 50,  "Good")
     .when(F.col("avg_aqi") <= 100, "Moderate")
     .when(F.col("avg_aqi") <= 150, "Unhealthy for Sensitive Groups")
     .when(F.col("avg_aqi") <= 200, "Unhealthy")
     .when(F.col("avg_aqi") <= 300, "Very Unhealthy")
     .otherwise("Hazardous")
)

# DENSE_RANK by avg_aqi DESC within same year+month
w_rank = Window.partitionBy("year", "month").orderBy(F.col("avg_aqi").desc())
w_rank_pm25 = Window.partitionBy("year", "month").orderBy(F.col("avg_pm25").desc())

monthly = monthly \
    .withColumn("aqi_rank",  F.dense_rank().over(w_rank)) \
    .withColumn("pm25_rank", F.dense_rank().over(w_rank_pm25)) \
    .withColumn("_aggregated_at", F.current_timestamp())

logger.info(f"gold_aqi_city_ranking rows: {monthly.count()}")

ranking_path = f"s3://{GOLD_BUCKET}/gold_aqi_city_ranking/"
ranking_dyf  = DynamicFrame.fromDF(monthly, glueContext, "gold_aqi_city_ranking")

sink2 = glueContext.getSink(
    connection_type="s3",
    path=ranking_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
)
sink2.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="gold_aqi_city_ranking")
sink2.setFormat("glueparquet", compression="snappy")
sink2.writeFrame(ranking_dyf)
logger.info(f"gold_aqi_city_ranking written → {ranking_path}")

# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 3: gold_station_summary
# Monthly station-level summary — for map visualization and station comparison
# Join fact_aqi with dim_station to get station name + coordinates
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: gold_station_summary...")

# Read dim_station
dim_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name="dim_station",
    transformation_ctx="dim_station",
)
dim_df = dim_dyf.toDF()
logger.info(f"dim_station records: {dim_df.count()}")

# Join fact_aqi with dim_station
station_df = df.join(
    dim_df.select("waqi_idx", "station_name", "lat", "lon"),
    on="waqi_idx",
    how="left"
)

# dominant_pollutant per station per month
dominant_station = df.groupBy("waqi_idx", "year", "month", "dominant_pollutant")     .agg(F.count("*").alias("pollutant_count"))

w_dom_station = Window.partitionBy("waqi_idx", "year", "month")     .orderBy(F.col("pollutant_count").desc())

dominant_station = dominant_station     .withColumn("_rank", F.row_number().over(w_dom_station))     .filter(F.col("_rank") == 1)     .select(
        "waqi_idx", "year", "month",
        F.col("dominant_pollutant").alias("dominant_pollutant_mode")
    )

# Aggregate monthly per station
station_monthly = station_df.groupBy(
    "waqi_idx", "station_name", "queried_city", "lat", "lon", "year", "month"
).agg(
    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
    F.max("aqi").alias("max_aqi"),
    F.min("aqi").alias("min_aqi"),
    F.round(F.avg("pm25"), 2).alias("avg_pm25"),
    F.round(F.avg("pm10"), 2).alias("avg_pm10"),
    F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    F.round(F.avg("temperature"), 2).alias("avg_temperature"),
    F.count("*").alias("record_count"),
)

# Join dominant_pollutant
station_monthly = station_monthly     .join(dominant_station, on=["waqi_idx", "year", "month"], how="left")     .withColumnRenamed("dominant_pollutant_mode", "dominant_pollutant")

# pollution_level based on avg_aqi
station_monthly = station_monthly.withColumn(
    "pollution_level",
    F.when(F.col("avg_aqi") <= 50,  "Good")
     .when(F.col("avg_aqi") <= 100, "Moderate")
     .when(F.col("avg_aqi") <= 150, "Unhealthy for Sensitive Groups")
     .when(F.col("avg_aqi") <= 200, "Unhealthy")
     .when(F.col("avg_aqi") <= 300, "Very Unhealthy")
     .otherwise("Hazardous")
)

# Rank stations within same city by avg_aqi DESC
w_station_rank = Window.partitionBy("queried_city", "year", "month")     .orderBy(F.col("avg_aqi").desc())

station_monthly = station_monthly     .withColumn("rank_in_city", F.dense_rank().over(w_station_rank))     .withColumn("_aggregated_at", F.current_timestamp())

logger.info(f"gold_station_summary rows: {station_monthly.count()}")

station_path = f"s3://{GOLD_BUCKET}/gold_station_summary/"
station_dynf = DynamicFrame.fromDF(station_monthly, glueContext, "gold_station_summary")

sink3 = glueContext.getSink(
    connection_type="s3",
    path=station_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
)
sink3.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="gold_station_summary")
sink3.setFormat("glueparquet", compression="snappy")
sink3.writeFrame(station_dynf)
logger.info(f"gold_station_summary written → {station_path}")

logger.info("Gold layer build complete.")
job.commit()