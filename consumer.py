import json
import os
from kafka import KafkaConsumer
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, lit, current_timestamp
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Snowpark session ──────────────────────────────────────────
connection_params = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "database":  os.getenv("SNOWFLAKE_DATABASE"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}
session = Session.builder.configs(connection_params).create()
print("✅ Snowpark connected")

# ── Kafka consumer ────────────────────────────────────────────
consumer = KafkaConsumer(
    "retail-invoices",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="snowpark-loader",
)
print("✅ Kafka consumer connected, waiting for messages...")

# ── Process in batches ────────────────────────────────────────
BATCH_SIZE = 200
batch = []

def process_batch(records):
    df_pandas = pd.DataFrame(records)

    # Rename columns to uppercase first
    df_pandas.columns = [
        "INVOICE_NO", "STOCK_CODE", "DESCRIPTION", "QUANTITY",
        "INVOICE_DATE", "UNIT_PRICE", "CUSTOMER_ID", "COUNTRY"
    ]

    # Add computed columns
    df_pandas["LINE_REVENUE"] = (df_pandas["QUANTITY"] * df_pandas["UNIT_PRICE"]).round(2)
    df_pandas["IS_RETURN"]    = df_pandas["QUANTITY"] < 0
    df_pandas["INGESTED_AT"]  = pd.Timestamp.now()

    # Filter zero quantity
    df_pandas = df_pandas[df_pandas["QUANTITY"] != 0]

    # Create Snowpark DataFrame and write
    df = session.create_dataframe(df_pandas)
    df.write.mode("append").save_as_table("RETAIL_INVOICES")
    print(f"✅ Batch of {len(df_pandas)} rows written to Snowflake")


for message in consumer:
    batch.append(message.value)

    if len(batch) >= BATCH_SIZE:
        process_batch(batch)
        batch = []

# flush remaining
if batch:
    process_batch(batch)