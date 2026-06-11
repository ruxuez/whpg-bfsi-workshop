# Lab 3: Lakehouse Federation & Analytics

This lab demonstrates **Lakehouse Federation** by querying **Apache Iceberg** tables stored in MinIO object storage directly from WarehousePG. You'll see how **PGAA DirectScan** enables analytics on external data without ETL, achieving impressive performance for lakehouse queries. Then you'll learn how to materialize hot data into native **WHPG AOCO** tables when maximum performance is needed.

## ⚠️ DO NOT HIT "Next" button without instructions ! ⚠️
---

## Context

### 1. What You'll Learn
* Query Iceberg data lakehouse directly with SQL (no ETL required)
* Understand PGAA DirectScan vectorized execution performance
* Create native WHPG tables from Iceberg data when needed
* Choose the right storage for different workloads

---

### 2. Query Catalog
Five analytical queries demonstrate lakehouse federation capabilities:

| Query | Description |
| :--- | :--- |
| **Revenue by Category** | Simple JOIN - shows baseline lakehouse performance |
| **Top 20 Customers** | Multi-table JOIN - ranked by spend |
| **Conversion Funnel** | CTE on events - view → cart → purchase rates |
| **Executive Summary** | 5 parallel COUNT(*) - quick scan test |
| **Daily Dashboard** | Complex **5-table join** - the most demanding query |

---

## Hands-On

---

## Phase 1: Query the Data Lakehouse

### 0. Check Iceberg Catalog ([button label="⚠️MinIO Tab"](tab-0))

> [!NOTE]
> Minio credentials
> * `username`: minioadmin
> * `password`: minioadmin

Check Iceberg Data exists in MinIO:

* Set Alias of MinIO
```run
mc alias set local http://minio:9000 minioadmin minioadmin
```
* View bucket
```run
mc ls local/
```
* View bucket content
```run
mc ls local/whpg-lakehouse
```
* Check Iceberg files
```run
mc ls --recursive local/whpg-lakehouse
```




### 1. Initialize the Analytics Engine ([button label="⚠️WarehousePG Tab"](tab-1))

Explore PGAA and PGFS in WarehousePG:

Run following in demo database:
```run
psql demo
```

Before running the benchmarks, you must enable the **Postgres AI & Analytics (PGAA)** extension.
```run
CREATE EXTENSION IF NOT EXISTS pgaa CASCADE;
```
This activates the high-performance FDW and the vectorized **DirectScan** executor.

---

### 2. PGFS Storage Location ([button label="⚠️WarehousePG Tab"](tab-1))

#### STEP 1: Create the connection to MinIO
First, we define where the Iceberg data lives. The **Postgres File System (PGFS)** handles the low-level connectivity to S3-compatible storage like MinIO.

```run
SELECT pgfs.create_storage_location(
    name        => 'minio_iceberg',
    url         => 's3://whpg-lakehouse/iceberg',
    options     => '{"endpoint": "http://minio:9000", "allow_http": "true"}',
    credentials => '{"access_key_id": "minioadmin", "secret_access_key": "minioadmin"}'
);
```
* Verify the location
```run
SELECT * FROM pgfs.list_storage_locations();
```
> [!NOTE]
> Clean up existing location if necessary.
>
> `SELECT pgfs.delete_storage_location('minio_iceberg');`

#### STEP 2: Create PGAA Iceberg Tables
Now, we create foreign tables using the **PGAA** access method. Note that we don't need to define columns manually; PGAA infers the schema directly from the Iceberg metadata.

**This is where the magic happens** - these tables point to data in MinIO, not in the database!

```run
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

* Verify Row Counts - Make sure data is accessible

``` run
SELECT 'customers_iceberg' AS tbl, COUNT(*) FROM customers_iceberg
UNION ALL SELECT 'products_iceberg',    COUNT(*) FROM products_iceberg
UNION ALL SELECT 'orders_iceberg',      COUNT(*) FROM orders_iceberg
UNION ALL SELECT 'order_items_iceberg', COUNT(*) FROM order_items_iceberg
UNION ALL SELECT 'events_iceberg',      COUNT(*) FROM events_iceberg
ORDER BY 2 DESC;
```

> **Key Point:** You just counted millions of rows stored in MinIO object storage - no data was loaded into the database!

---

### 3. Run Analytics on Lakehouse Data ([button label="⚠️Terminal Tab"](tab-2))

Now let's see these queries in action!

1. **Start the Dashboard:**
   ```run
   python3.9 /scripts/apps/app3.py
   ```

2. **Access the Interface:** ([button label="⚠️Lakehouse Federation Tab"](tab-3))

3. **Try Some Queries:**
   - Click **"Run"** on individual queries in the "Query Lakehouse Data" tab
   - Notice the execution times (typically 2-5 seconds for complex queries)
   - Click the **"Run All Queries"** button in the "Performance Results" tab

4. **What to Observe:**
   - **These queries run directly on data in MinIO** (no ETL!)
   - DirectScan appears in EXPLAIN plans (vectorized execution)
   - Performance is impressive for external data:
     - Simple queries: sub-second
     - Complex multi-table JOINs: 2-5 seconds
     - This is **lakehouse federation** in action!

> **💡 This is impressive!** Traditional databases require loading data first. WHPG queries the lakehouse directly where it lives.

---

### 4. Check Understanding
In the dashboard app ([button label="⚠️Lakehouse Federation Tab"](tab-3)), go to the **"Check Understanding"** tab. Discuss the two questions with a colleague, then click "Reveal" to see the answers.

### 5. Challenge: Find the top 5 products by revenue

In the dashboard app ([button label="⚠️Lakehouse Federation Tab"](tab-3)), go to the **"Challenge"** tab and complete the SQL query by filling in the missing JOIN condition.

---

## Phase 2: Materialize Hot Data to Native Tables

When you need maximum performance for frequently-accessed data, materialize it into native WHPG tables.

### 6. Create Native WHPG Tables from Iceberg ([button label="⚠️WarehousePG Tab"](tab-1))
Now let's materialize the Iceberg data into native WarehousePG tables using **Append-Only Columnar (AOCO)** storage with **ZSTD** compression.

**Notice:** We're using `CREATE TABLE ... AS SELECT * FROM <iceberg_table>` - direct data movement from lakehouse to warehouse!

```run
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

### 7. Verify Data Parity ([button label="⚠️WarehousePG Tab"](tab-1))
Run the verification to ensure the lakehouse data was correctly materialized into native tables:

```run
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

> **Success!** You now have the same data in two places:
> - **Iceberg tables** (lakehouse) - flexible, no ETL, good performance
> - **Native AOCO tables** (warehouse) - maximum performance, optimized for speed

---

## Understanding Your Options

### When to Use Each Approach:

| Storage Type | Best For | Performance | Use Cases |
|--------------|----------|-------------|-----------|
| **Iceberg Tables (Lakehouse)** | Ad-hoc analysis, data exploration, infrequent queries | Good (1-2s for complex queries) | Data science, exploratory analytics, shared datasets |
| **Native AOCO Tables (Warehouse)** | Frequent queries, dashboards, critical paths | Excellent (sub-second) | Production dashboards, high-concurrency workloads |
| **Hybrid (Both)** | Best of both worlds | Flexible | Hot data native, cold data lakehouse |

**The Power:** You can query both in the same SQL statement - join native and Iceberg tables together!



---

## Summary

**What You Learned:**
1. ✅ Query Iceberg data lakehouse directly with PGAA (no ETL!)
2. ✅ Achieve good performance on external data (2-5s for complex queries)
3. ✅ Materialize hot data into native AOCO tables when needed
4. ✅ Understand when to use lakehouse vs warehouse storage

**Key Takeaway:** WHPG gives you **flexibility** - query your lakehouse where it lives, materialize when performance matters, or use both together in hybrid queries.

> [!WARNING]
> ⚠️Please DON'T click on "Next" button now, wait for our instruction ! ⚠️