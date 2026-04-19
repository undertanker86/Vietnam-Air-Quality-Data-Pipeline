"""
Lambda: WAQI Air Quality API Ingestion (Bronze Layer)
──────────────────────────────────────────────────────
Triggered by EventBridge 3 times/day (morning, afternoon, evening).
Calls WAQI city endpoint for each configured Vietnamese city and writes
raw JSON responses to the Bronze S3 bucket, preserving full API response.
 
S3 structure:
  {BUCKET}/api_raw/
    queried_city={city}/
      year={YYYY}/month={MM}/day={DD}/
        {city}_{ISO_timestamp}.json
 
Environment Variables:
    WAQI_CITIES           — Comma-separated city slugs (default: see CITIES below)
    S3_BUCKET_BRONZE - data-pipeline-bronze-ap-dev
    SNS_ALERT_TOPIC_ARN - arn:aws:sns:us-east-1:597720049681:data-pipeline-alerts-dev
    WAQI_API_TOKEN - e5e167e2886cd9ae2ecxxxxx
"""
# IAM - lambda-plo-crawler-role
 
import json
import os
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
 
import boto3
 
# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
# ── AWS Clients ──────────────────────────────────────────────────────────────
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
 
# ── Config ───────────────────────────────────────────────────────────────────
API_TOKEN = os.environ["WAQI_API_TOKEN"]
BUCKET = os.environ["S3_BUCKET_BRONZE"]
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "").strip()
 
DEFAULT_CITIES = (
    "ha-noi,"
    "ho-chi-minh-city,"
    "da-nang,"
    "gia-lai,"
    "cao-bang,"
)
CITIES = os.environ.get("WAQI_CITIES", DEFAULT_CITIES).split(",")
 
API_BASE = "https://api.waqi.info/feed"
 
 
def fetch_city_aqi(city_slug: str) -> dict:
    """
    Call WAQI city endpoint: GET /feed/{city}/?token={token}
    Returns the full raw JSON response as a dict.
    Raises HTTPError / URLError on network failure.
    """
    url = f"{API_BASE}/{city_slug}/?token={API_TOKEN}"
    req = Request(url, headers={"Accept": "application/json"})
 
    with urlopen(req, timeout=50) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
 
    if raw.get("status") != "ok":
        raise ValueError(f"WAQI returned non-ok status for '{city_slug}': {raw.get('status')}")
 
    return raw
 
 
def build_s3_key(city_slug: str, now: datetime) -> str:
    """
    Build Hive-style S3 key for Bronze layer.
 
    Pattern:
      api_raw/queried_city={city}/year={YYYY}/month={MM}/day={DD}/{city}_{timestamp}.json
 
    Timestamp in filename uses ISO format with colons replaced by hyphens
    to avoid S3 key issues: 2026-04-11T17-00-00Z
    """
    year  = now.strftime("%Y")
    month = now.strftime("%m")
    day   = now.strftime("%d")
    # Safe timestamp for filename (no colons)
    ts_safe = now.strftime("%Y-%m-%dT%H-%M-%SZ")
 
    return (
        f"api_raw/"
        f"queried_city={city_slug}/"
        f"year={year}/month={month}/day={day}/"
        f"{city_slug}_{ts_safe}.json"
    )
 
 
def write_to_s3(payload: dict, bucket: str, key: str, ingested_at: str) -> None:
    """
    Write JSON payload to S3.
    Adds pipeline metadata as S3 object metadata (not touching the JSON body,
    so Bronze stays truly raw).
    """
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        # Metadata lives in S3 object headers — Bronze JSON is untouched
        Metadata={
            "ingested_at": ingested_at,
            "source": "waqi_city_endpoint",
        },
    )
 
 
def send_alert(subject: str, message: str) -> None:
    if not SNS_TOPIC:
        return
    region = SNS_TOPIC.split(":")[3]
    client = boto3.client("sns", region_name=region)
    client.publish(
        TopicArn=SNS_TOPIC,
        Subject=subject[:100],
        Message=message,
    )
 
 
def lambda_handler(event, context):
    """
    Main entry point.
 
    Iterates over all configured cities:
      1. Call WAQI city endpoint
      2. Write raw response (+ pipeline metadata in S3 headers) to Bronze S3
      3. Track success / failure per city
 
    Returns a summary dict. SNS alert fires if any city fails.
 
    EventBridge schedule (set 3 rules in AWS Console or Terraform):
      Morning:   cron(0 1 * * ? *)   → UTC 01:00 = ICT 08:00
      Afternoon: cron(0 7 * * ? *)   → UTC 07:00 = ICT 14:00
      Evening:   cron(0 13 * * ? *)  → UTC 13:00 = ICT 20:00
    """
    now = datetime.now(timezone.utc)
    ingested_at = now.isoformat()          # e.g. 2026-04-11T13:00:00+00:00
    ingestion_id = now.strftime("%Y%m%d_%H%M%S")
 
    results = {"success": [], "failed": []}
 
    for city in CITIES:
        city = city.strip().lower()
        if not city:
            continue
 
        logger.info(f"Fetching city: {city}")
 
        try:
            raw_response = fetch_city_aqi(city)
 
            # Derive info for logging (not modifying the raw JSON itself)
            aqi_value   = raw_response["data"].get("aqi", "N/A")
            station_idx = raw_response["data"].get("idx", "N/A")
            measured_at = raw_response["data"]["time"].get("iso", "N/A")
 
            s3_key = build_s3_key(city, now)
            write_to_s3(raw_response, BUCKET, s3_key, ingested_at)
 
            logger.info(
                f"  OK | city={city} idx={station_idx} "
                f"aqi={aqi_value} measured_at={measured_at} "
                f"→ s3://{BUCKET}/{s3_key}"
            )
            results["success"].append({
                "city": city,
                "idx": station_idx,
                "aqi": aqi_value,
                "measured_at": measured_at,
                "s3_key": s3_key,
            })
 
        except ValueError as e:
            # WAQI returned status != "ok" (station offline, token issue, etc.)
            logger.warning(f"  WARN | city={city} → {e}")
            results["failed"].append({"city": city, "error": str(e), "type": "api_status"})
 
        except (HTTPError, URLError) as e:
            logger.error(f"  ERROR | city={city} network error → {e}")
            results["failed"].append({"city": city, "error": str(e), "type": "network"})
 
        except Exception as e:
            logger.error(f"  ERROR | city={city} unexpected → {e}")
            results["failed"].append({"city": city, "error": str(e), "type": "unknown"})
 
    # ── Summary & Alert ──────────────────────────────────────────────────────
    n_ok   = len(results["success"])
    n_fail = len(results["failed"])
    summary = (
        f"[{ingestion_id}] WAQI ingestion complete. "
        f"Success: {n_ok}/{len(CITIES)} cities. Failed: {n_fail}."
    )
    logger.info(summary)
 
    if results["failed"]:
        send_alert(
            subject=f"[AQ Pipeline] Ingestion partial failure — {ingestion_id}",
            message=json.dumps({"summary": summary, "results": results}, indent=2),
        )
 
    return {
        "statusCode": 200,
        "ingestion_id": ingestion_id,
        "summary": summary,
        "results": results,
    }
