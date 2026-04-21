import uuid
from datetime import datetime

class PipelineLogger:

    def __init__(self, spark, layer="unknown", pipeline="maven_market"):
        self.spark = spark
        self.layer = layer
        self.pipeline = pipeline
        self.run_id = str(uuid.uuid4())

    def log(self, level, message, stage,
            status="RUNNING", row_count=None, error=None):

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": self.run_id,
            "level": level,
            "layer": self.layer,
            "stage": stage,
            "message": message,
            "status": status,
            "row_count": row_count,
            "error": error
        }

        self.spark.createDataFrame([log_entry]) \
            .write.mode("append") \
            .saveAsTable("maven_market_dev.audit_logs")

        print(f"[{level}] [{self.layer}] {stage} | {message}")