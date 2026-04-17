import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

class MavenMarketLogger:
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.audit_table = f"{catalog}.{schema}.audit_logs"
        
    def log_event(self, pipeline_name: str, status: str, record_count: int = 0, error_message: str = None):
        """Writes a log entry to the Unity Catalog audit table."""
        
        # Create the log data
        log_data = [(
            pipeline_name,
            status,
            record_count,
            error_message,
            datetime.now()
        )]
        
        schema = StructType([
            StructField("pipeline_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("record_count", LongType(), True),
            StructField("error_message", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # Append to the Delta table
        df = self.spark.createDataFrame(log_data, schema)
        df.write.format("delta").mode("append").saveAsTable(self.audit_table)
        print(f"Log event saved for {pipeline_name}: {status}")