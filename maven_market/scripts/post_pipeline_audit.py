"""
post_pipeline_audit.py — Custom Logging for Maven Market Pipelines.

Runs as the FINAL task in the orchestration job (after gold pipeline).
Extracts structured custom log entries from each pipeline's event_log()
and writes them to the centralized audit table: {catalog}.audit.audit_logs.

Custom logs captured per layer:
  - Pipeline start / completion status
  - Per-table flow progress (table name, rows written, rows dropped)
  - Per-table data quality expectations (expectation name, passed/failed)
  - Error details (if any flow or update failed)
  - Pipeline timing (start, end, duration)

Usage (from jobs.yml):
    spark_python_task:
      python_file: ../scripts/post_pipeline_audit.py
      parameters: ["${var.target_catalog}"]
"""

import sys
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, get_json_object, lit, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType,
)


# ── Pipeline IDs (set via bundle or hardcode per environment) ────────
PIPELINE_IDS = {
    "bronze": "7cbdfbb8-29e8-4bf1-b2de-20abc21418f1",
    "silver": None,   # TODO: populate after first silver pipeline deploy
    "gold":   None,   # TODO: populate after first gold pipeline deploy
}

# ── Target table schema (matches audit.audit_logs) ──────────────────
AUDIT_LOG_SCHEMA = StructType([
    StructField("timestamp",  TimestampType(), True),
    StructField("run_id",     StringType(),    True),
    StructField("pipeline",   StringType(),    True),
    StructField("layer",      StringType(),    True),
    StructField("stage",      StringType(),    True),
    StructField("level",      StringType(),    True),
    StructField("message",    StringType(),    True),
    StructField("status",     StringType(),    True),
    StructField("row_count",  LongType(),      True),
    StructField("error",      StringType(),    True),
])


def extract_custom_logs(spark, pipeline_id, layer, run_id):
    """
    Query event_log() for a single pipeline and extract structured
    custom log entries for: flow progress, data quality, and errors.
    Returns a list of dicts matching the audit_logs schema.
    """
    logs = []
    ts_now = datetime.now(timezone.utc)

    if not pipeline_id:
        logs.append({
            "timestamp": ts_now,
            "run_id": run_id, "pipeline": "maven_market",
            "layer": layer, "stage": "pipeline_config",
            "level": "WARN",
            "message": f"{layer} pipeline ID not configured — skipping audit extraction",
            "status": "SKIPPED", "row_count": None, "error": None,
        })
        return logs

    try:
        event_log = spark.sql(f"SELECT * FROM event_log(\'{pipeline_id}\')")

        # ── Find latest update ───────────────────────────────────────
        latest_update = (
            event_log
            .filter(col("event_type") == "create_update")
            .orderBy(col("timestamp").desc())
            .select("origin.update_id", "timestamp")
            .first()
        )

        if latest_update is None:
            logs.append({
                "timestamp": ts_now,
                "run_id": run_id, "pipeline": "maven_market",
                "layer": layer, "stage": "pipeline_update",
                "level": "WARN",
                "message": f"No updates found in event log for {layer} pipeline",
                "status": "NO_UPDATES", "row_count": None, "error": None,
            })
            return logs

        update_id = latest_update["update_id"]
        latest_events = event_log.filter(col("origin.update_id") == update_id)

        # ── 1. Pipeline start/end timing ─────────────────────────────
        start_ts = latest_events.agg(F.min("timestamp")).first()[0]
        end_ts = latest_events.agg(F.max("timestamp")).first()[0]
        duration = int((end_ts - start_ts).total_seconds()) if start_ts and end_ts else 0

        logs.append({
            "timestamp": start_ts or ts_now,
            "run_id": run_id, "pipeline": "maven_market",
            "layer": layer, "stage": "pipeline_start",
            "level": "INFO",
            "message": f"{layer.upper()} pipeline started (update_id: {update_id})",
            "status": "RUNNING", "row_count": None, "error": None,
        })

        # ── 2. Pipeline completion status ────────────────────────────
        status_row = (
            latest_events
            .filter(col("event_type") == "update_progress")
            .filter(
                col("details").contains("COMPLETED") |
                col("details").contains("FAILED") |
                col("details").contains("CANCELED")
            )
            .orderBy(col("timestamp").desc())
            .first()
        )

        if status_row:
            details_str = status_row["details"]
            if "COMPLETED" in details_str:
                pipeline_status = "COMPLETED"
            elif "FAILED" in details_str:
                pipeline_status = "FAILED"
            elif "CANCELED" in details_str:
                pipeline_status = "CANCELED"
            else:
                pipeline_status = "UNKNOWN"
        else:
            pipeline_status = "IN_PROGRESS"

        logs.append({
            "timestamp": end_ts or ts_now,
            "run_id": run_id, "pipeline": "maven_market",
            "layer": layer, "stage": "pipeline_complete",
            "level": "INFO" if pipeline_status == "COMPLETED" else "ERROR",
            "message": f"{layer.upper()} pipeline {pipeline_status} in {duration}s",
            "status": pipeline_status, "row_count": None, "error": None,
        })

        # ── 3. Per-table flow progress (rows written / dropped) ──────
        flow_rows = (
            latest_events
            .filter(col("event_type") == "flow_progress")
            .filter(col("details").contains("COMPLETED"))
            .select(
                col("timestamp"),
                get_json_object(col("details"), "$.flow_progress.flow_name").alias("table_name"),
                get_json_object(col("details"), "$.flow_progress.metrics.num_output_rows")
                    .cast("long").alias("rows_written"),
                get_json_object(col("details"), "$.flow_progress.data_quality.dropped_records")
                    .cast("long").alias("dropped_records"),
                get_json_object(col("details"), "$.flow_progress.status").alias("flow_status"),
            )
            .collect()
        )

        for row in flow_rows:
            table_name = row["table_name"] or "unknown"
            rows_written = row["rows_written"] or 0
            dropped = row["dropped_records"] or 0

            logs.append({
                "timestamp": row["timestamp"],
                "run_id": run_id, "pipeline": "maven_market",
                "layer": layer, "stage": f"flow_{table_name}",
                "level": "INFO",
                "message": f"Table '{table_name}' completed: {rows_written} rows written, {dropped} rows dropped",
                "status": "SUCCESS", "row_count": rows_written, "error": None,
            })

            if dropped and dropped > 0:
                logs.append({
                    "timestamp": row["timestamp"],
                    "run_id": run_id, "pipeline": "maven_market",
                    "layer": layer, "stage": f"dq_drop_{table_name}",
                    "level": "WARN",
                    "message": f"Table '{table_name}': {dropped} rows dropped by data quality expectations",
                    "status": "DQ_DROP", "row_count": dropped, "error": None,
                })

        # ── 4. Data quality expectations ─────────────────────────────
        dq_rows = (
            latest_events
            .filter(col("event_type") == "flow_progress")
            .select(
                col("timestamp"),
                get_json_object(col("details"), "$.flow_progress.flow_name").alias("table_name"),
                get_json_object(col("details"), "$.flow_progress.data_quality.expectations").alias("expectations"),
            )
            .filter(col("expectations").isNotNull())
            .collect()
        )

        for row in dq_rows:
            table_name = row["table_name"] or "unknown"
            expectations_json = row["expectations"]

            logs.append({
                "timestamp": row["timestamp"],
                "run_id": run_id, "pipeline": "maven_market",
                "layer": layer, "stage": f"dq_expectations_{table_name}",
                "level": "INFO",
                "message": f"Data quality expectations for '{table_name}': {expectations_json[:500]}",
                "status": "DQ_CHECK", "row_count": None, "error": None,
            })

        # ── 5. Errors ────────────────────────────────────────────────
        error_rows = (
            latest_events
            .filter(
                col("event_type").isin("flow_progress", "update_progress") &
                (col("details").contains("FAILED") | col("details").contains('"error"'))
            )
            .select(
                col("timestamp"),
                col("event_type"),
                get_json_object(col("details"), "$.flow_progress.flow_name").alias("table_name"),
                col("details"),
            )
            .collect()
        )

        for row in error_rows:
            table_name = row["table_name"] or "pipeline"
            error_detail = str(row["details"])[:2000]

            logs.append({
                "timestamp": row["timestamp"],
                "run_id": run_id, "pipeline": "maven_market",
                "layer": layer, "stage": f"error_{table_name}",
                "level": "ERROR",
                "message": f"Error in '{table_name}': {error_detail[:200]}",
                "status": "FAILED", "row_count": None, "error": error_detail,
            })

        # ── Summary log ──────────────────────────────────────────────
        total_rows = sum(r["rows_written"] or 0 for r in flow_rows)
        total_tables = len(flow_rows)

        logs.append({
            "timestamp": end_ts or ts_now,
            "run_id": run_id, "pipeline": "maven_market",
            "layer": layer, "stage": "pipeline_summary",
            "level": "INFO",
            "message": f"{layer.upper()} summary: {total_tables} tables, {total_rows} total rows, status={pipeline_status}, duration={duration}s",
            "status": pipeline_status, "row_count": total_rows, "error": None,
        })

        print(f"[AUDIT] {layer}: extracted {len(logs)} custom log entries")
        return logs

    except Exception as e:
        print(f"[AUDIT] ERROR extracting {layer} logs: {e}")
        logs.append({
            "timestamp": ts_now,
            "run_id": run_id, "pipeline": "maven_market",
            "layer": layer, "stage": "audit_extraction",
            "level": "ERROR",
            "message": f"Failed to extract custom logs for {layer}: {str(e)[:200]}",
            "status": "AUDIT_ERROR", "row_count": None, "error": str(e)[:2000],
        })
        return logs


def main():
    catalog = sys.argv[1] if len(sys.argv) > 1 else "maven_market_uc"
    audit_table = f"{catalog}.audit.audit_logs"
    audit_schema_name = f"{catalog}.audit"

    spark = SparkSession.builder.getOrCreate()
    run_id = str(uuid.uuid4())

    print(f"[AUDIT] ═══════════════════════════════════════════════")
    print(f"[AUDIT] Custom Logging — Post-Pipeline Audit")
    print(f"[AUDIT] Run ID:  {run_id}")
    print(f"[AUDIT] Target:  {audit_table}")
    print(f"[AUDIT] ═══════════════════════════════════════════════")

    # ── Ensure audit schema exists ───────────────────────────────────
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {audit_schema_name}")

    # ── Extract custom logs from each pipeline layer ─────────────────
    all_logs = []
    for layer, pipeline_id in PIPELINE_IDS.items():
        print(f"\n[AUDIT] Processing {layer} layer...")
        layer_logs = extract_custom_logs(spark, pipeline_id, layer, run_id)
        all_logs.extend(layer_logs)
        print(f"[AUDIT] {layer}: {len(layer_logs)} log entries extracted")

    # ── Add orchestration-level summary ──────────────────────────────
    ts_now = datetime.now(timezone.utc)
    layer_statuses = {}
    for log in all_logs:
        if log["stage"] == "pipeline_complete":
            layer_statuses[log["layer"]] = log["status"]

    active_statuses = [s for s in layer_statuses.values() if s != "SKIPPED"]
    overall = "COMPLETED" if all(s == "COMPLETED" for s in active_statuses) else "PARTIAL_FAILURE" if active_statuses else "NO_PIPELINES"

    all_logs.append({
        "timestamp": ts_now,
        "run_id": run_id, "pipeline": "maven_market",
        "layer": "orchestration", "stage": "job_complete",
        "level": "INFO",
        "message": f"Full orchestration {overall}: bronze={layer_statuses.get('bronze', 'N/A')}, silver={layer_statuses.get('silver', 'N/A')}, gold={layer_statuses.get('gold', 'N/A')}",
        "status": overall, "row_count": len(all_logs), "error": None,
    })

    # ── Write all custom logs to Delta ───────────────────────────────
    if not all_logs:
        print("[AUDIT] No log entries to write.")
        return

    audit_df = spark.createDataFrame(all_logs, schema=AUDIT_LOG_SCHEMA)
    audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table)

    # ── Final report ─────────────────────────────────────────────────
    log_counts = {}
    for log in all_logs:
        layer = log["layer"]
        log_counts[layer] = log_counts.get(layer, 0) + 1

    print(f"\n[AUDIT] ═══════════════════════════════════════════════")
    print(f"[AUDIT] Custom Logging Complete")
    print(f"[AUDIT] Total entries written: {len(all_logs)}")
    for layer, count in log_counts.items():
        print(f"[AUDIT]   {layer}: {count} entries")
    print(f"[AUDIT] Target table: {audit_table}")
    print(f"[AUDIT] Overall status: {overall}")
    print(f"[AUDIT] ═══════════════════════════════════════════════")


if __name__ == "__main__":
    main()
