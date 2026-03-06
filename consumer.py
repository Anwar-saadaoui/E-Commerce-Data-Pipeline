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

    # Create Snowpark DataFrame from pandas
    df = session.create_dataframe(df_pandas)

    # ── Transformations with Snowpark ──────────────────────────
    df = (
        df
        .with_column("LINE_REVENUE",
            (col('"Quantity"') * col('"UnitPrice"')).cast("float"))
        .with_column("IS_RETURN",
            when(col('"Quantity"') < 0, lit(True)).otherwise(lit(False)))
        .with_column("INGESTED_AT", current_timestamp())
        .filter(col('"Quantity"') != 0)
        .rename({
            '"InvoiceNo"':   "INVOICE_NO",
            '"StockCode"':   "STOCK_CODE",
            '"Description"': "DESCRIPTION",
            '"Quantity"':    "QUANTITY",
            '"InvoiceDate"': "INVOICE_DATE",
            '"UnitPrice"':   "UNIT_PRICE",
            '"CustomerID"':  "CUSTOMER_ID",
            '"Country"':     "COUNTRY",
        })
    )

    # Write to Snowflake
    df.write.mode("append").save_as_table("RETAIL_INVOICES")
    print(f"✅ Batch of {len(records)} rows written to Snowflake")


for message in consumer:
    batch.append(message.value)

    if len(batch) >= BATCH_SIZE:
        process_batch(batch)
        batch = []

# flush remaining
if batch:
    process_batch(batch)