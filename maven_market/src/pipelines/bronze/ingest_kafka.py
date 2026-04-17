# 1. Load your config and secrets
# (Assuming your Member 5 built the config_parser.py)
from src.utils.config_parser import load_config
config = load_config("dev")

# Get secrets from Azure Key Vault linked to Databricks
kafka_key = dbutils.secrets.get(scope="maven_secrets", key="kafka_api_key")
kafka_secret = dbutils.secrets.get(scope="maven_secrets", key="kafka_api_secret")

# 2. Define the secure SASL config string
jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'

# 3. Read the Kafka Stream
df_orders = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "YOUR_SERVER") # Pull this from config later
    .option("subscribe", "orders_topic")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Write the Stream to Bronze
# Use a checkpoint folder inside your ADLS storage, NOT /path/
checkpoint_path = f"{config['storage_root']}checkpoints/orders_kafka/"

(df_orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .toTable("maven_market_uc.bronze.orders_kafka")
)