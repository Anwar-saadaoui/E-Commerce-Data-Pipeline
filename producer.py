import csv
import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BROKER = "localhost:9092"
TOPIC  = "retail-invoices"

# ── Create topic automatically ────────────────────────────────
admin = KafkaAdminClient(bootstrap_servers=BROKER)

try:
    admin.create_topics([
        NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)
    ])
    print(f"✅ Topic '{TOPIC}' created")
except TopicAlreadyExistsError:
    print(f"Topic '{TOPIC}' already exists, skipping...")
finally:
    admin.close()

# ── rest of the code stays exactly the same ──────────────────
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("data/data.csv", newline="", encoding="latin-1") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        record = {
            "InvoiceNo":   row["InvoiceNo"].strip(),
            "StockCode":   row["StockCode"].strip(),
            "Description": row["Description"].strip(),
            "Quantity":    int(row["Quantity"] or 0),
            "InvoiceDate": row["InvoiceDate"].strip(),
            "UnitPrice":   float(row["UnitPrice"] or 0),
            "CustomerID":  row["CustomerID"].strip(),
            "Country":     row["Country"].strip(),
        }
        producer.send(TOPIC, value=record)
        if i % 500 == 0:
            print(f"Sent {i} rows...")
        time.sleep(0.01)

producer.flush()
print("Done sending all rows!")