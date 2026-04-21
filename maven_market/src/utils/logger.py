import uuid
from datetime import datetime

class PipelineLogger:

    def __init__(self, spark=None, layer="unknown", pipeline="maven_market"):
        self.spark = spark
        self.layer = layer
        self.pipeline = pipeline
        self.run_id = str(uuid.uuid4())

    def log(self, level, message, stage,
            status="RUNNING", row_count=None, error=None):

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "layer": self.layer,
            "stage": stage,
            "level": level,
            "message": message,
            "status": status,
            "row_count": row_count,
            "error": error
        }

        # DLT-safe logging (NO saveAsTable)
        print(f"[LOG] {log_entry}")