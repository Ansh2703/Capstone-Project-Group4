import uuid
from datetime import datetime, timezone


class PipelineLogger:
    """
    Structured audit logger for Maven Market pipelines.

    Behaviour:
      - ALWAYS prints to stdout (captured by DLT event logs).
      - When running OUTSIDE a DLT context and a live SparkSession is
        available, logs are also persisted to
        ``{catalog}.audit.audit_logs``.
      - Inside DLT (detected by attempting ``import dlt``), table writes
        are skipped to avoid side-effect restrictions.
      - Call ``flush()`` at the end of a pipeline stage to batch-insert
        all accumulated log rows in one go (more efficient than per-row
        INSERT).
    """

    def __init__(self, spark=None, layer="unknown",
                 pipeline="maven_market", catalog="maven_market_uc"):
        self.spark = spark
        self.layer = layer
        self.pipeline = pipeline
        self.catalog = catalog
        self.run_id = str(uuid.uuid4())
        self._buffer = []          # accumulator for flush()
        self._in_dlt = self._detect_dlt()

    # ------------------------------------------------------------------
    # DLT detection
    # ------------------------------------------------------------------
    @staticmethod
    def _detect_dlt():
        """Return True when running inside a DLT pipeline."""
        try:
            import dlt  # noqa: F401
            return True
        except ImportError:
            return False

    # ------------------------------------------------------------------
    # Core logging
    # ------------------------------------------------------------------
    def log(self, level, message, stage,
            status="RUNNING", row_count=None, error=None):
        """Create a structured log entry, print it, and buffer it."""

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        log_entry = {
            "timestamp": ts,
            "run_id":    self.run_id,
            "pipeline":  self.pipeline,
            "layer":     self.layer,
            "stage":     stage,
            "level":     level,
            "message":   message,
            "status":    status,
            "row_count": row_count,
            "error":     error,
        }

        # Always print (DLT-safe)
        print(f"[LOG] {log_entry}")

        # Buffer for later persistence
        self._buffer.append(log_entry)

    # ------------------------------------------------------------------
    # Convenience methods
    # ------------------------------------------------------------------
    def info(self, message, stage, **kwargs):
        """Log at INFO level."""
        self.log("INFO", message, stage, **kwargs)

    def warn(self, message, stage, **kwargs):
        """Log at WARN level."""
        self.log("WARN", message, stage, **kwargs)

    def error(self, message, stage, **kwargs):
        """Log at ERROR level."""
        self.log("ERROR", message, stage, **kwargs)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def flush(self):
        """
        Batch-insert all buffered log rows into the audit_logs table.

        Skipped when:
          - No SparkSession is available.
          - Running inside a DLT context (side-effects not allowed).
          - The buffer is empty.
        """
        if not self._buffer:
            print("[LOG] Nothing to flush — buffer is empty.")
            return

        if self._in_dlt:
            print(f"[LOG] DLT context detected — skipping table persist "
                  f"({len(self._buffer)} entries printed only).")
            self._buffer.clear()
            return

        if self.spark is None:
            print(f"[LOG] No SparkSession — skipping table persist "
                  f"({len(self._buffer)} entries printed only).")
            self._buffer.clear()
            return

        target_table = f"{self.catalog}.audit.audit_logs"

        try:
            from pyspark.sql import Row
            from pyspark.sql.types import (
                StructType, StructField, StringType,
                TimestampType, LongType,
            )

            schema = StructType([
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

            rows = []
            for entry in self._buffer:
                rows.append(Row(
                    timestamp=datetime.strptime(entry["timestamp"],
                                                "%Y-%m-%d %H:%M:%S"),
                    run_id=entry["run_id"],
                    pipeline=entry["pipeline"],
                    layer=entry["layer"],
                    stage=entry["stage"],
                    level=entry["level"],
                    message=entry["message"],
                    status=entry["status"],
                    row_count=int(entry["row_count"])
                             if entry["row_count"] is not None else None,
                    error=entry["error"],
                ))

            df = self.spark.createDataFrame(rows, schema=schema)
            df.write.mode("append").saveAsTable(target_table)

            count = len(self._buffer)
            self._buffer.clear()
            print(f"[LOG] Flushed {count} log entries to {target_table}.")

        except Exception as e:
            print(f"[LOG] WARNING — flush to {target_table} failed: {e}")
            # Buffer is NOT cleared so user can retry
