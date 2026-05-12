# Lab 2: Lakehouse Federation & High-Speed Analytics

This lab demonstrates the power of **Converged Analytics** by federating queries between the **Apache Iceberg** Data Lakehouse (on MinIO) and native **WarehousePG (WHPG) AOCO** tables. You will observe how the **PGAA DirectScan** engine achieves vectorized execution speeds on external data that rival or occasionally exceed native storage performance.

---

## Context

### 1. Benchmark Architecture
The benchmark environment (`lab2.py`) compares eight analytical queries side-by-side. It uses a multi-threaded execution model (up to 4 threads) to test the concurrency and throughput of both engines.


#### The Comparison Engines:
* **Apache Iceberg (via PGAA):** Data stored in Parquet format on MinIO, accessed via PGAA DirectScan.
* **Native WHPG (AOCO):** Data stored in native Append-Only Column-Oriented tables within the WarehousePG cluster.

---

### 2. Query Catalog & Logic
Each query is optimized to trigger the **DirectScan** vectorized path by using explicit column names and aliasing, avoiding positional references that can trigger compatibility mode fallbacks.

| ID | Query | Benchmark Description |
| :--- | :--- | :--- |
| **top20** | Top 20 Customers | Joins `customers` × `orders` to rank top spenders by total revenue. |
| **category** | Revenue by Category v2 | Alternative category revenue view focusing on unit volume and revenue. |
| **funnel** | Conversion Funnel | Calculates rates for `page_view` → `add_to_cart` → `purchase` from the `events` table. |
| **daily** | Daily Dashboard | A massive **5-table join** across all fact and dimension tables for a 30-day snapshot. |
| **summary** | Executive Summary | A single-row KPI snapshot providing total counts across the entire schema. |

---

### Tabs preparation

Prepare 2 Shell Tabs:
- Your local host under this repo (Terminal Tab)
- Connection to `cdw` envionment shell (WarehousePG Tab):
```bash
docker exec -u gpadmin -w /home/gpadmin -it cdw /bin/bash
```

---

##  Hands-On

### 0.Check Iceberg Catalog (**⚠️Terminal Tab**)

> [!NOTE]
> Minio credentials
> * `username`: minioadmin
> * `password`: minioadmin

Check Iceberg Data exists in MinIO:

* Connect to Minio container
```bash
docker exec -it minio bash
```

* Set Alias of MinIO
```bash
mc alias set local http://minio:9000 minioadmin minioadmin
```
* View bucket
```bash
mc ls local/
```
* View bucket content
```bash
mc ls local/whpg-lakehouse
```
* Check Iceberg files
```bash
mc ls --recursive local/whpg-lakehouse
```




### 1. Initialize the Analytics Engine (**⚠️WarehousePG Tab**)

Explore PGAA and PGFS in WarehousePG:

Run following in demo database:
```bash
psql demo
```

Before running the benchmarks, you must enable the **Postgres AI & Analytics (PGAA)** extension.
```sql
CREATE EXTENSION IF NOT EXISTS pgaa CASCADE;
```
This activates the high-performance FDW and the vectorized **DirectScan** executor.

---

### 2. PGFS Storage Location (**⚠️WarehousePG Tab**)

#### STEP 1: Create the connection to MinIO
First, we define where the Iceberg data lives. The **Postgres File System (PGFS)** handles the low-level connectivity to S3-compatible storage like MinIO.

```sql
SELECT pgfs.create_storage_location(
    name        => 'minio_iceberg',
    url         => 's3://whpg-lakehouse/iceberg',
    options     => '{"endpoint": "http://minio:9000", "allow_http": "true"}',
    credentials => '{"access_key_id": "minioadmin", "secret_access_key": "minioadmin"}'
);
```
* Verify the location
```sql
SELECT * FROM pgfs.list_storage_locations();
```
> [!NOTE]
> Clean up existing location if necessary.
>
> `SELECT pgfs.delete_storage_location('minio_iceberg');`

#### STEP 2: PGAA Iceberg Tables
Now, we create the foreign tables using the **PGAA** access method. Note that we don't need to define columns manually; PGAA infers the schema directly from the Iceberg metadata.

```sql
DROP TABLE IF EXISTS customers_iceberg;
CREATE TABLE customers_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/customers',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS products_iceberg;
CREATE TABLE products_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/products',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS orders_iceberg;
CREATE TABLE orders_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/orders',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS order_items_iceberg;
CREATE TABLE order_items_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/order_items',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS events_iceberg;
CREATE TABLE events_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/events',
    pgaa.format           = 'iceberg'
);
```

* Quick Check on rows

``` sql
SELECT 'customers_iceberg' AS tbl, COUNT(*) FROM customers_iceberg
UNION ALL SELECT 'products_iceberg',    COUNT(*) FROM products_iceberg
UNION ALL SELECT 'orders_iceberg',      COUNT(*) FROM orders_iceberg
UNION ALL SELECT 'order_items_iceberg', COUNT(*) FROM order_items_iceberg
UNION ALL SELECT 'events_iceberg',      COUNT(*) FROM events_iceberg
ORDER BY 2 DESC;
```

#### STEP 3: Native WHPG Tables (AO Columnar + ZSTD)
To compare performance, we load the same data into native WarehousePG tables using **Append-Only Columnar (AOCO)** storage with **ZSTD** compression.

```sql
CREATE SCHEMA IF NOT EXISTS demo;

DROP TABLE IF EXISTS demo.customers CASCADE;
CREATE TABLE demo.customers (
    customer_id     BIGINT,
    email           TEXT,
    first_name      TEXT,
    last_name       TEXT,
    country         TEXT,
    city            TEXT,
    signup_date     DATE,
    is_active       BOOLEAN,
    lifetime_value  NUMERIC(38,2)
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (customer_id);
INSERT INTO demo.customers SELECT * FROM customers_iceberg;
ANALYZE demo.customers;

DROP TABLE IF EXISTS demo.products CASCADE;
CREATE TABLE demo.products (
    product_id      BIGINT,
    sku             TEXT,
    name            TEXT,
    category        TEXT,
    subcategory     TEXT,
    price           NUMERIC(38,2),
    cost            NUMERIC(38,2),
    stock_quantity  BIGINT,
    is_available    BOOLEAN
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (product_id);
INSERT INTO demo.products SELECT * FROM products_iceberg;
ANALYZE demo.products;

DROP TABLE IF EXISTS demo.orders CASCADE;
CREATE TABLE demo.orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    order_date      DATE,
    order_timestamp TIMESTAMP,
    status          TEXT,
    shipping_country TEXT,
    shipping_city   TEXT,
    total_amount    NUMERIC(38,2),
    discount_amount NUMERIC(38,2)
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (order_id);
INSERT INTO demo.orders SELECT * FROM orders_iceberg;
ANALYZE demo.orders;

DROP TABLE IF EXISTS demo.order_items CASCADE;
CREATE TABLE demo.order_items (
    item_id         BIGINT,
    order_id        BIGINT,
    product_id      BIGINT,
    quantity        BIGINT,
    unit_price      NUMERIC(38,2),
    line_total      NUMERIC(38,2)
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (item_id);
INSERT INTO demo.order_items SELECT * FROM order_items_iceberg;
ANALYZE demo.order_items;

DROP TABLE IF EXISTS demo.events CASCADE;
CREATE TABLE demo.events (
    event_id        BIGINT,
    event_timestamp TIMESTAMP,
    event_date      DATE,
    customer_id     BIGINT,
    event_type      TEXT,
    page_url        TEXT,
    product_id      BIGINT,
    session_id      TEXT,
    device_type     TEXT,
    country         TEXT
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (event_id);
INSERT INTO demo.events SELECT * FROM events_iceberg;
ANALYZE demo.events;
```

#### STEP 4: Verify Row Counts
Run the verification block to ensure data parity between your S3 Data Lake and your local NVMe-backed Warehouse storage.

```sql
SELECT
    rpad(tbl, 14) as table_name,
    ice as iceberg_count,
    nat as native_count,
    CASE WHEN ice = nat THEN '✓' ELSE '✗' END as status
FROM (
    SELECT 'customers' AS tbl, (SELECT COUNT(*) FROM customers_iceberg) AS ice, (SELECT COUNT(*) FROM demo.customers) AS nat
    UNION ALL SELECT 'products', (SELECT COUNT(*) FROM products_iceberg), (SELECT COUNT(*) FROM demo.products)
    UNION ALL SELECT 'orders', (SELECT COUNT(*) FROM orders_iceberg), (SELECT COUNT(*) FROM demo.orders)
    UNION ALL SELECT 'order_items', (SELECT COUNT(*) FROM order_items_iceberg), (SELECT COUNT(*) FROM demo.order_items)
    UNION ALL SELECT 'events', (SELECT COUNT(*) FROM events_iceberg), (SELECT COUNT(*) FROM demo.events)
) r
ORDER BY ice DESC;
```
---

### 3. Running Benchmark
1.  **Start the Dashboard:** (**⚠️ Terminal Tab**)
    ```bash
    python3.9 /scripts/apps/app2.py
    ```
2.  **Access the Interface:** Open application via your browser
`http://localhost:5000`

3.  **Analyze Speedup:** 

    Click **"Run Full Benchmark"**. The dashboard will display the execution time for both engines and the **Speedup Factor**.

---

### 4. Technical Performance Notes
* **DirectScan Utilization:** Look for `DirectScan` in the EXPLAIN ANALYZE logs. This indicates the query bypassed the standard Postgres executor for vectorized processing.
* **Data Locality:** While Iceberg data is external (MinIO), PGAA uses **Arrow Flight SQL** and vectorized readers to minimize the "external table tax".
* **AOCO Advantage:** Native tables benefit from tighter integration with the Greenplum resource manager and local disk I/O, providing a baseline for elite performance.

### 5. Check Understanding
Two questions to surface what stuck. Talk to the person next to you - compare answers, then click "Reveal" to see what we're listening for.

### 6. Challenge: Find the top 5 products by revenue

You're handed this query but the JOIN condition is missing. Fill in the blank to make it run. Hint: products and order_items share one obvious key.

```sql
-- Top 5 products by revenue
SELECT p.product_id,
       p.name        AS product_name,
       p.category,
       SUM(oi.quantity)                                    AS units_sold,
       ROUND(SUM(oi.quantity * oi.unit_price)::numeric, 2) AS revenue
FROM   products_iceberg    p
JOIN   order_items_iceberg oi
       ON   [ FILL IN ]
GROUP  BY p.product_id, p.name, p.category
ORDER  BY revenue DESC
LIMIT  5
```