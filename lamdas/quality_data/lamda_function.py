"""
Lambda: Data Quality Checks — Silver fact_aqi + dim_station
──────────────────────────────────────────────────────────────
Uses 3 Athena queries:
  - Query 1: fact_aqi metrics (sample 1000 rows to avoid full scan)
  - Query 2: freshness check (WHERE ingested_at >= cutoff — partition filter)
  - Query 3: dim_station check (lightweight, small data)

Checks:
  1. Row count              — does fact_aqi have enough data?
  2. Null percentage        — critical columns populated?
  3. AQI range              — 0 ≤ aqi ≤ 500?
  4. City coverage          — all 5 cities present?
  5. Source validity        — only 'kaggle' or 'api'?
  6. Freshness              — new data within 48h? (filter first)
  7. dim_station row count  — is there station data?
  8. dim_station coverage   — stations cover all 5 cities?

Environment Variables:
    ATHENA_DATABASE         — Silver Glue database
    ATHENA_OUTPUT_LOCATION  — S3 path for Athena query results
    SNS_ALERT_TOPIC_ARN     — SNS topic for alerts
    DQ_MIN_ROW_COUNT        — min rows fact_aqi (default: 10)
    DQ_MAX_NULL_PERCENT     — max null % (default: 5.0)
    DQ_FRESHNESS_HOURS      — freshness window (default: 48)
    DQ_SAMPLE_ROWS          — sample size for DQ scan (default: 1000)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client("athena")

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE        = os.environ["ATHENA_DATABASE"]
OUTPUT_LOCATION = os.environ["ATHENA_OUTPUT_LOCATION"]
SNS_TOPIC       = os.environ.get("SNS_ALERT_TOPIC_ARN", "").strip()

MIN_ROW_COUNT   = int(os.environ.get("DQ_MIN_ROW_COUNT",   "10"))
MAX_NULL_PCT    = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
FRESHNESS_HOURS = int(os.environ.get("DQ_FRESHNESS_HOURS", "48"))
SAMPLE_ROWS     = int(os.environ.get("DQ_SAMPLE_ROWS",     "1000"))

EXPECTED_CITIES = {"ha-noi", "ho-chi-minh-city", "da-nang", "gia-lai", "cao-bang"}
AQI_MIN, AQI_MAX = 0, 500


# ── Athena Helper ─────────────────────────────────────────────────────────────
def run_query(sql: str) -> dict | None:
    """Run Athena query, return first row as dict."""
    resp = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    execution_id = resp["QueryExecutionId"]

    for _ in range(60):
        status = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "CANCELLED"):
            reason = athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {status}: {reason}")
        time.sleep(2)
    else:
        raise TimeoutError(f"Athena query timed out: {execution_id}")

    result  = athena_client.get_query_results(QueryExecutionId=execution_id)
    rows    = result["ResultSet"]["Rows"]
    if len(rows) <= 1:
        return None

    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    return {
        headers[i]: col.get("VarCharValue", None)
        for i, col in enumerate(rows[1]["Data"])
    }


# ── Query 1: fact_aqi metrics on sample ────────────────────────────────────
def build_fact_query() -> str:
    """
    Sample SAMPLE_ROWS rows from fact_aqi to avoid full table scan.
    Uses TABLESAMPLE BERNOULLI or LIMIT in subquery.
    Checks: row_count, null_pct, aqi_range, city_coverage, source_validity.
    """
    return f"""
SELECT
    COUNT(*)    AS total_rows,
    SUM(CASE WHEN aqi               IS NULL THEN 1 ELSE 0 END) AS null_aqi,
    SUM(CASE WHEN measured_at       IS NULL THEN 1 ELSE 0 END) AS null_measured_at,
    SUM(CASE WHEN dominant_pollutant IS NULL THEN 1 ELSE 0 END) AS null_dominant_pollutant,
    SUM(CASE WHEN queried_city      IS NULL THEN 1 ELSE 0 END) AS null_queried_city,
    SUM(CASE WHEN aqi < {AQI_MIN} OR aqi > {AQI_MAX} THEN 1 ELSE 0 END) AS aqi_out_of_range,
    ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT queried_city)), ',') AS distinct_cities,
    ARRAY_JOIN(
        ARRAY_AGG(DISTINCT CASE WHEN source NOT IN ('kaggle', 'api') THEN source END),
        ','
    ) AS invalid_sources
FROM (
    SELECT * FROM "fact_aqi"
    LIMIT {SAMPLE_ROWS}
)
"""


# ── Query 2: Freshness — filter partition first ───────────────────────────────
def build_freshness_query(cutoff_str: str) -> str:
    """
    Use WHERE ingested_at >= cutoff to filter partition — no full scan.
    Only need COUNT > 0 to pass.
    """
    return f"""
SELECT COUNT(*) AS fresh_rows
FROM "fact_aqi"
WHERE ingested_at >= '{cutoff_str}'
LIMIT 1
"""


# ── Query 3: dim_station metrics ──────────────────────────────────────────────
DIM_STATION_QUERY = """
SELECT
    COUNT(*)    AS total_stations,
    ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT queried_city)), ',') AS station_cities
FROM "dim_station"
"""


# ── Evaluate fact_aqi checks ──────────────────────────────────────────────────
def evaluate_fact_checks(m: dict) -> list[dict]:
    results = []
    total   = int(m.get("total_rows") or 0)

    # 1. Row count
    passed = total >= MIN_ROW_COUNT
    results.append({
        "check": "row_count",
        "table": "fact_aqi",
        "value": total,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"fact_aqi row count: {total} (min: {MIN_ROW_COUNT})",
    })

    # 2. Null percentage
    for col, key in [
        ("aqi",                "null_aqi"),
        ("measured_at",        "null_measured_at"),
        ("dominant_pollutant", "null_dominant_pollutant"),
        ("queried_city",       "null_queried_city"),
    ]:
        null_count = int(m.get(key) or 0)
        null_pct   = round(null_count / total * 100, 2) if total > 0 else 0
        passed     = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct",
            "table": "fact_aqi",
            "column": col,
            "value": null_pct,
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"fact_aqi.{col} null%: {null_pct}% (max: {MAX_NULL_PCT}%)",
        })

    # 3. AQI range
    aqi_invalid = int(m.get("aqi_out_of_range") or 0)
    results.append({
        "check": "aqi_range",
        "table": "fact_aqi",
        "invalid_count": aqi_invalid,
        "passed": aqi_invalid == 0,
        "message": f"AQI out of range [0-500]: {aqi_invalid} records",
    })

    # 4. City coverage
    cities_str = m.get("distinct_cities") or ""
    found      = set(cities_str.split(",")) - {""} if cities_str else set()
    missing    = EXPECTED_CITIES - found
    results.append({
        "check": "city_coverage",
        "table": "fact_aqi",
        "found": sorted(found),
        "missing": sorted(missing),
        "passed": len(missing) == 0,
        "message": f"Missing cities: {sorted(missing)}" if missing else "All 5 cities present",
    })

    # 5. Source validity
    inv_str = m.get("invalid_sources") or ""
    invalid = [s for s in inv_str.split(",") if s and s != "null"]
    results.append({
        "check": "source_validity",
        "table": "fact_aqi",
        "invalid_sources": invalid,
        "passed": len(invalid) == 0,
        "message": f"Invalid sources: {invalid}" if invalid else "All sources valid",
    })

    return results


# ── Evaluate freshness check ──────────────────────────────────────────────────
def evaluate_freshness(m: dict | None, cutoff: datetime) -> dict:
    if not m:
        return {
            "check": "freshness",
            "table": "fact_aqi",
            "passed": False,
            "message": f"No data ingested in last {FRESHNESS_HOURS}h",
        }
    fresh_rows = int(m.get("fresh_rows") or 0)
    passed     = fresh_rows > 0
    return {
        "check": "freshness",
        "table": "fact_aqi",
        "fresh_rows": fresh_rows,
        "cutoff": str(cutoff),
        "passed": passed,
        "message": f"{fresh_rows} rows ingested since {cutoff}" if passed
                   else f"No fresh data since {cutoff} ({FRESHNESS_HOURS}h window)",
    }


# ── Evaluate dim_station checks ───────────────────────────────────────────────
def evaluate_dim_checks(m: dict | None) -> list[dict]:
    results = []

    if not m:
        results.append({
            "check": "dim_station_row_count",
            "table": "dim_station",
            "passed": False,
            "message": "dim_station is empty or missing",
        })
        return results

    total = int(m.get("total_stations") or 0)
    results.append({
        "check": "dim_station_row_count",
        "table": "dim_station",
        "value": total,
        "passed": total > 0,
        "message": f"dim_station: {total} stations",
    })

    cities_str = m.get("station_cities") or ""
    found      = set(cities_str.split(",")) - {""} if cities_str else set()
    missing    = EXPECTED_CITIES - found
    results.append({
        "check": "dim_station_city_coverage",
        "table": "dim_station",
        "found": sorted(found),
        "missing": sorted(missing),
        "passed": len(missing) == 0,
        "message": f"Stations missing for cities: {sorted(missing)}" if missing
                   else "All 5 cities have stations",
    })

    return results


# ── Main Handler ──────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    logger.info(f"Running DQ checks on {DATABASE}")

    cutoff     = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")

    all_results    = []
    overall_passed = True

    # ── Query 1: fact_aqi sample metrics ─────────────────────────────────────
    logger.info(f"Query 1: fact_aqi sample ({SAMPLE_ROWS} rows)...")
    try:
        fact_metrics = run_query(build_fact_query())
        if not fact_metrics:
            raise RuntimeError("fact_aqi returned no rows")
        all_results.extend(evaluate_fact_checks(fact_metrics))
    except Exception as e:
        logger.error(f"fact_aqi query failed: {e}")
        all_results.append({
            "check": "fact_aqi_query",
            "passed": False,
            "message": str(e),
        })

    # ── Query 2: Freshness (partition filter) ─────────────────────────────────
    logger.info(f"Query 2: freshness check (cutoff: {cutoff_str})...")
    try:
        fresh_metrics = run_query(build_freshness_query(cutoff_str))
        all_results.append(evaluate_freshness(fresh_metrics, cutoff))
    except Exception as e:
        logger.error(f"Freshness query failed: {e}")
        all_results.append({
            "check": "freshness",
            "table": "fact_aqi",
            "passed": False,
            "message": str(e),
        })

    # ── Query 3: dim_station ──────────────────────────────────────────────────
    logger.info("Query 3: dim_station metrics...")
    try:
        dim_metrics = run_query(DIM_STATION_QUERY)
        all_results.extend(evaluate_dim_checks(dim_metrics))
    except Exception as e:
        logger.error(f"dim_station query failed: {e}")
        all_results.append({
            "check": "dim_station_query",
            "passed": False,
            "message": str(e),
        })

    # ── Summary ───────────────────────────────────────────────────────────────
    overall_passed = all(r["passed"] for r in all_results)
    passed_count   = sum(1 for r in all_results if r["passed"])
    total_count    = len(all_results)

    for r in all_results:
        status = "PASS" if r["passed"] else "FAIL"
        logger.info(f"  [{status}] {r['check']}: {r['message']}")

    logger.info(
        f"DQ Summary: {passed_count}/{total_count} passed. "
        f"Overall: {'PASS' if overall_passed else 'FAIL'}"
    )

    if not overall_passed and SNS_TOPIC:
        failed = [r for r in all_results if not r["passed"]]
        region = SNS_TOPIC.split(":")[3]
        boto3.client("sns", region_name=region).publish(
            TopicArn=SNS_TOPIC,
            Subject="[AQ Pipeline] Silver DQ checks FAILED",
            Message=json.dumps({
                "database": DATABASE,
                "failed_checks": failed,
            }, indent=2, default=str),
        )

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed":  int(passed_count),
        "checks_total":   int(total_count),
        "database":       DATABASE,
        "details":        json.loads(json.dumps(all_results, default=str)),
    }




