"""
post_pipeline_audit.py — Post-pipeline audit logging task.

Runs as the FINAL task in the orchestration job (after gold pipeline).
Queries the Lakeflow Declarative Pipeline event_log() for each layer to
capture real metrics:
  - Pipeline update status (COMPLETED / FAILED / CANCELED)
  - Per-table row counts (rows written per flow)
  - Data quality violations (expectation pass/fail counts)
  - Error details (if any flow failed)
  - Pipeline start/end timestamps and duration

Writes structured audit records to {catalog}.audit.pipeline_audit_log.

Usage (from jobs.yml):
    spark_python_task:
      python_file: ../scripts/post_pipeline_audit.py
      parameters: ["${var.target_catalog}"]
"""

import sys
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, IntegerType,
)


# ── Pipeline IDs (set via bundle or hardcode per environment) ────────
# These are resolved at deploy time by Databricks Asset Bundles.
# Update these if pipeline IDs change after redeployment.
PIPELINE_IDS = {
    "bronze": "7cbdfbb8-29e8-4bf1-b2de-20abc21418f1",
    "silver": None,   # TODO: populate after first silver pipeline deploy
    "gold":   None,   # TODO: populate after first gold pipeline deploy
}


def audit_single_pipeline(spark, pipeline_id, layer, run_id, ts):
    """
    Query event_log() for a single pipeline and return an audit summary dict.
    Returns None if the pipeline ID is not set or event log is empty.
    """
    if not pipeline_id:
        print(f"[AUDIT] Skipping {layer} — pipeline ID not configured.")
        return {
            "layer": layer, "pipeline_id": "NOT_CONFIGURED",
            "update_id": None, "status": "SKIPPED",
            "started_at": None, "ended_at": None, "duration_seconds": None,
            "tables_processed": 0, "total_rows_written": 0,
            "total_rows_dropped": 0, "dq_violations": 0,
            "errors_found": 0, "error_details": None,
        }

    try:
        event_log = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")

        # ── Latest update ID ─────────────────────────────────────────
        latest_update = (
            event_log
            .filter(col("event_type") == "create_update")
            .orderBy(col("timestamp").desc())
            .select("origin.update_id")
            .first()
        )

        if latest_update is None:
            print(f"[AUDIT] No updates found for {layer} pipeline.")
            return {
                "layer": layer, "pipeline_id": pipeline_id,
                "update_id": None, "status": "NO_UPDATES",
                "started_at": None, "ended_at": None, "duration_seconds": None,
                "tables_processed": 0, "total_rows_written": 0,
                "total_rows_dropped": 0, "dq_violations": 0,
                "errors_found": 0, "error_details": None,
            }

        update_id = latest_update["update_id"]
        latest_events = event_log.filter(col("origin.update_id") == update_id)

        # ── Pipeline status ──────────────────────────────────────────
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
                status = "COMPLETED"
            elif "FAILED" in details_str:
                status = "FAILED"
            elif "CANCELED" in details_str:
                status = "CANCELED"
            else:
                status = "UNKNOWN"
        else:
            status = "IN_PROGRESS"

        # ── Timing ───────────────────────────────────────────────────
        start_ts = latest_events.agg(F.min("timestamp")).first()[0]
        end_ts = latest_events.agg(F.max("timestamp")).first()[0]
        duration = None
        if start_ts and end_ts:
            duration = int((end_ts - start_ts).total_seconds())

        # ── Per-table row counts ─────────────────────────────────────
        flow_progress = (
            latest_events
            .filter(col("event_type") == "flow_progress")
            .filter(col("details").contains("COMPLETED"))
            .select(
                get_json_object(col("details"), "$.flow_progress.metrics.num_output_rows")
                    .cast("long").alias("rows_written"),
                get_json_object(col("details"), "$.flow_progress.data_quality.dropped_records")
                    .cast("long").alias("dropped_records"),
                get_json_object(col("details"), "$.flow_progress.flow_name")
                    .alias("table_name"),
            )
        )

        total_rows = flow_progress.agg(
            F.coalesce(F.sum("rows_written"), F.lit(0)).alias("total_rows")
        ).first()["total_rows"]

        total_dropped = flow_progress.agg(
            F.coalesce(F.sum("dropped_records"), F.lit(0)).alias("total_dropped")
        ).first()["total_dropped"]

        tables_processed = flow_progress.select("table_name").distinct().count()

        # ── Data quality violations ──────────────────────────────────
        dq_events = (
            latest_events
            .filter(col("event_type") == "flow_progress")
            .select(
                get_json_object(col("details"), "$.flow_progress.data_quality.expectations")
                    .alias("expectations"),
            )
            .filter(col("expectations").isNotNull())
        )
        dq_violation_count = dq_events.count()

        # ── Errors ───────────────────────────────────────────────────
        errors_df = (
            latest_events
            .filter(
                col("event_type").isin("flow_progress", "update_progress") &
                (col("details").contains("FAILED") | col("details").contains("error"))
            )
            .select(col("details"))
        )
        error_count = errors_df.count()
        error_details = None
        if error_count > 0:
            first_error = errors_df.first()
            error_details = str(first_error["details"])[:2000]  # truncate

        # ── Print summary ────────────────────────────────────────────
        print(f"[AUDIT] {layer}: status={status}, tables={tables_processed}, "
              f"rows={total_rows}, dropped={total_dropped}, errors={error_count}")

        return {
            "layer": layer, "pipeline_id": pipeline_id,
            "update_id": update_id, "status": status,
            "started_at": str(start_ts) if start_ts else None,
            "ended_at": str(end_ts) if end_ts else None,
            "duration_seconds": duration,
            "tables_processed": int(tables_processed),
            "total_rows_written": int(total_rows),
            "total_rows_dropped": int(total_dropped),
            "dq_violations": int(dq_violation_count),
            "errors_found": int(error_count),
            "error_details": error_details,
        }

    except Exception as e:
        print(f"[AUDIT] ERROR auditing {layer}: {e}")
        return {
            "layer": layer, "pipeline_id": pipeline_id,
            "update_id": None, "status": "AUDIT_ERROR",
            "started_at": None, "ended_at": None, "duration_seconds": None,
            "tables_processed": 0, "total_rows_written": 0,
            "total_rows_dropped": 0, "dq_violations": 0,
            "errors_found": 1, "error_details": str(e)[:2000],
        }


def main():
    catalog = sys.argv[1] if len(sys.argv) > 1 else "maven_market_uc"
    audit_table = f"{catalog}.audit.pipeline_audit_log"
    audit_schema = f"{catalog}.audit"

    spark = SparkSession.builder.getOrCreate()
    run_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)

    print(f"[AUDIT] Starting post-pipeline audit — run_id: {run_id}")
    print(f"[AUDIT] Target: {audit_table}")

    # ── Ensure audit schema and table exist ──────────────────────────
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {audit_schema}")

    # ── Audit each pipeline layer ────────────────────────────────────
    audit_records = []
    for layer, pipeline_id in PIPELINE_IDS.items():
        result = audit_single_pipeline(spark, pipeline_id, layer, run_id, ts)
        result["run_id"] = run_id
        result["audit_logged_at"] = ts.strftime("%Y-%m-%d %H:%M:%S")
        audit_records.append(result)

    # ── Also write an orchestration-level summary ────────────────────
    all_statuses = [r["status"] for r in audit_records]
    overall_status = "COMPLETED" if all(s == "COMPLETED" for s in all_statuses if s not in ("SKIPPED",)) else "PARTIAL_FAILURE"
    total_rows_all = sum(r["total_rows_written"] for r in audit_records)
    total_errors_all = sum(r["errors_found"] for r in audit_records)

    audit_records.append({
        "layer": "orchestration",
        "pipeline_id": "ALL",
        "update_id": None,
        "status": overall_status,
        "started_at": audit_records[0].get("started_at"),
        "ended_at": audit_records[-2].get("ended_at") if len(audit_records) > 1 else None,
        "duration_seconds": None,
        "tables_processed": sum(r["tables_processed"] for r in audit_records[:-1]),
        "total_rows_written": total_rows_all,
        "total_rows_dropped": sum(r["total_rows_dropped"] for r in audit_records[:-1]),
        "dq_violations": sum(r["dq_violations"] for r in audit_records[:-1]),
        "errors_found": total_errors_all,
        "error_details": None,
        "run_id": run_id,
        "audit_logged_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
    })

    # ── Write to Delta ───────────────────────────────────────────────
    audit_df = spark.createDataFrame(audit_records)
    audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table)

    print(f"\n[AUDIT] ═══════════════════════════════════════════════")
    print(f"[AUDIT] Flushed {len(audit_records)} audit records to {audit_table}")
    print(f"[AUDIT] Run ID:          {run_id}")
    print(f"[AUDIT] Overall Status:  {overall_status}")
    print(f"[AUDIT] Total Rows:      {total_rows_all}")
    print(f"[AUDIT] Total Errors:    {total_errors_all}")
    print(f"[AUDIT] ═══════════════════════════════════════════════")


if __name__ == "__main__":
    main()
