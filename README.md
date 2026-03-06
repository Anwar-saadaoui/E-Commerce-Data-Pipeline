# 🛒 E-Commerce Data Pipeline

A real-time data engineering pipeline that streams retail invoice data from a CSV file into Snowflake using Kafka and Snowpark.

## Architecture

```
data.csv → Kafka Producer → Kafka Topic → Snowpark Consumer → Snowflake
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Messaging | Apache Kafka |
| Ingestion | Python (kafka-python-ng) |
| Transformation | Snowflake Snowpark |
| Storage | Snowflake |
| Containerization | Docker |

---

## Project Structure

```
retail-pipeline/
├── data/
│   └── data.csv
├── .env
├── .env.example
├── docker-compose.yml
├── producer.py
├── consumer.py
└── requirements.txt
```

---

## Dataset

UK-based online retail transactions dataset with the following columns:

| Column | Description |
|--------|-------------|
| `InvoiceNo` | Unique invoice identifier |
| `StockCode` | Product code |
| `Description` | Product name |
| `Quantity` | Units per transaction |
| `InvoiceDate` | Date and time of invoice |
| `UnitPrice` | Price per unit (£) |
| `CustomerID` | Unique customer identifier |
| `Country` | Customer country |

---

## Prerequisites

- Python 3.13+
- Docker Desktop
- Snowflake account

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/Anwar-saadaoui/E-Commerce-Data-Pipeline.git
cd E-Commerce-Data-Pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=your_account.aws
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=RETAIL_DW
SNOWFLAKE_SCHEMA=ECOMMERCE
SNOWFLAKE_WAREHOUSE=RETAIL_WH
```

> **How to find your Snowflake account identifier:**
> Check your browser URL when logged in → `https://XXXXXX.aws.snowflakecomputing.com` → use `XXXXXX.aws`

### 4. Setup Snowflake

Run this once in your Snowflake worksheet:

```sql
CREATE DATABASE IF NOT EXISTS RETAIL_DW;
CREATE SCHEMA   IF NOT EXISTS RETAIL_DW.ECOMMERCE;

CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME  = TRUE;

CREATE TABLE IF NOT EXISTS RETAIL_DW.ECOMMERCE.RETAIL_INVOICES (
    INVOICE_NO    VARCHAR,
    STOCK_CODE    VARCHAR,
    DESCRIPTION   VARCHAR,
    QUANTITY      INT,
    INVOICE_DATE  VARCHAR,
    UNIT_PRICE    FLOAT,
    LINE_REVENUE  FLOAT,
    IS_RETURN     BOOLEAN,
    CUSTOMER_ID   VARCHAR,
    COUNTRY       VARCHAR,
    INGESTED_AT   TIMESTAMP_NTZ
);
```

### 5. Start Kafka

```bash
docker compose up -d
```

---

## Running the Pipeline

Open two terminals:

**Terminal 1 — Start the consumer first:**
```bash
python consumer.py
```

**Terminal 2 — Start the producer:**
```bash
python producer.py
```

The producer reads `data/data.csv` and streams rows into the `retail-invoices` Kafka topic. The consumer picks them up in batches of 200, applies Snowpark transformations, and writes to Snowflake.

---

## Transformations (Snowpark)

| Column | Description |
|--------|-------------|
| `LINE_REVENUE` | Computed as `Quantity × UnitPrice` |
| `IS_RETURN` | `True` if Quantity is negative |
| `INGESTED_AT` | Timestamp when the row was loaded |

Rows with zero quantity are filtered out.

---

## Querying the Data

```sql
-- Total revenue by country
SELECT COUNTRY, SUM(LINE_REVENUE) AS TOTAL_REVENUE
FROM RETAIL_DW.ECOMMERCE.RETAIL_INVOICES
WHERE IS_RETURN = FALSE
GROUP BY COUNTRY
ORDER BY TOTAL_REVENUE DESC;

-- Top products
SELECT STOCK_CODE, DESCRIPTION, SUM(QUANTITY) AS UNITS_SOLD
FROM RETAIL_DW.ECOMMERCE.RETAIL_INVOICES
WHERE IS_RETURN = FALSE
GROUP BY STOCK_CODE, DESCRIPTION
ORDER BY UNITS_SOLD DESC
LIMIT 20;
```

---

## Known Issues

- `kafka-python` is incompatible with Python 3.13 — use `kafka-python-ng` instead
- CSV must be encoded as `latin-1` due to special characters in product descriptions
- Snowflake account identifier must include the cloud region (e.g. `ES96342.aws`)

---
