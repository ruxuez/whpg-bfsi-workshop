#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
Lab 2: Iceberg Data Generator for WHPG/PGAA Workshop
═══════════════════════════════════════════════════════════════════════════════

Generates an e-commerce dataset (customers, products, orders, order_items,
events) and writes to Apache Iceberg tables on a local MinIO instance.

This data is used alongside the native WHPG network analytics tables
to demonstrate federated queries across both storage tiers.

Prerequisites:
    pip install "pyiceberg[s3fs]" pyarrow --break-system-packages

MinIO setup:
    docker run -d --name minio -p 9000:9000 -p 9001:9001 \
      -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
      -v /data/minio:/data \
      minio/minio server /data --console-address ":9001"

    mc alias set local http://localhost:9000 minioadmin minioadmin
    mc mb local/whpg-lakehouse

Usage:
    python3 iceberg_data_generator.py
    python3 iceberg_data_generator.py --scale 10    # 10x data (10K customers, 50K orders...)

Environment variables (all optional, defaults shown):
    MINIO_ENDPOINT=http://minio:9000
    MINIO_ACCESS_KEY=minioadmin
    MINIO_SECRET_KEY=minioadmin
    MINIO_BUCKET=whpg-lakehouse
    CATALOG_DB=/home/gpadmin/iceberg_catalog.db
"""

import os
import sys
import random
import argparse
import time
from datetime import datetime, timedelta
from decimal import Decimal

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, DateType, DecimalType, LongType,
    NestedField, StringType, TimestampType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — override via environment variables
# ═════════════════════════════════════════════════════════════════════════════

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET   = os.environ.get("MINIO_BUCKET", "whpg-lakehouse")
CATALOG_DB     = os.environ.get("CATALOG_DB", "/home/gpadmin/iceberg_catalog.db")

WAREHOUSE = f"s3://{MINIO_BUCKET}/iceberg"
NAMESPACE = "analytics"

# ═════════════════════════════════════════════════════════════════════════════
# REFERENCE DATA
# ═════════════════════════════════════════════════════════════════════════════

FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Kate", "Leo", "Maya", "Noah", "Olivia", "Peter",
    "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier", "Yuki", "Zoe",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Martinez", "Anderson", "Taylor", "Thomas", "Moore", "Jackson",
    "Martin", "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Young",
]
COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil", "India", "Israel"]
CITIES = {
    "USA": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "UK": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Brazil": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"],
    "India": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"],
    "Israel": ["Tel Aviv", "Jerusalem", "Haifa", "Beer Sheva", "Eilat"],
}
CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Accessories", "Audio"],
    "Clothing": ["Men's", "Women's", "Kids", "Shoes", "Accessories"],
    "Home": ["Furniture", "Kitchen", "Bedding", "Decor", "Garden"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Winter Sports"],
    "Books": ["Fiction", "Non-Fiction", "Technical", "Children", "Comics"],
}
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
EVENT_TYPES = [
    "page_view", "product_view", "add_to_cart", "remove_from_cart",
    "checkout_start", "purchase", "search", "login", "logout", "signup",
]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]

# ═════════════════════════════════════════════════════════════════════════════
# ICEBERG SCHEMAS
# ═════════════════════════════════════════════════════════════════════════════

SCHEMAS = {
    "customers": Schema(
        NestedField(1, "customer_id", LongType(), required=False),
        NestedField(2, "email", StringType(), required=False),
        NestedField(3, "first_name", StringType(), required=False),
        NestedField(4, "last_name", StringType(), required=False),
        NestedField(5, "country", StringType(), required=False),
        NestedField(6, "city", StringType(), required=False),
        NestedField(7, "signup_date", DateType(), required=False),
        NestedField(8, "is_active", BooleanType(), required=False),
        NestedField(9, "lifetime_value", DecimalType(38, 2), required=False),
    ),
    "products": Schema(
        NestedField(1, "product_id", LongType(), required=False),
        NestedField(2, "sku", StringType(), required=False),
        NestedField(3, "name", StringType(), required=False),
        NestedField(4, "category", StringType(), required=False),
        NestedField(5, "subcategory", StringType(), required=False),
        NestedField(6, "price", DecimalType(38, 2), required=False),
        NestedField(7, "cost", DecimalType(38, 2), required=False),
        NestedField(8, "stock_quantity", LongType(), required=False),
        NestedField(9, "is_available", BooleanType(), required=False),
    ),
    "orders": Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "customer_id", LongType(), required=False),
        NestedField(3, "order_date", DateType(), required=False),
        NestedField(4, "order_timestamp", TimestampType(), required=False),
        NestedField(5, "status", StringType(), required=False),
        NestedField(6, "shipping_country", StringType(), required=False),
        NestedField(7, "shipping_city", StringType(), required=False),
        NestedField(8, "total_amount", DecimalType(38, 2), required=False),
        NestedField(9, "discount_amount", DecimalType(38, 2), required=False),
    ),
    "order_items": Schema(
        NestedField(1, "item_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "product_id", LongType(), required=False),
        NestedField(4, "quantity", LongType(), required=False),
        NestedField(5, "unit_price", DecimalType(38, 2), required=False),
        NestedField(6, "line_total", DecimalType(38, 2), required=False),
    ),
    "events": Schema(
        NestedField(1, "event_id", LongType(), required=False),
        NestedField(2, "event_timestamp", TimestampType(), required=False),
        NestedField(3, "event_date", DateType(), required=False),
        NestedField(4, "customer_id", LongType(), required=False),
        NestedField(5, "event_type", StringType(), required=False),
        NestedField(6, "page_url", StringType(), required=False),
        NestedField(7, "product_id", LongType(), required=False),
        NestedField(8, "session_id", StringType(), required=False),
        NestedField(9, "device_type", StringType(), required=False),
        NestedField(10, "country", StringType(), required=False),
    ),
}

PARTITION_SPECS = {
    "orders": PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="order_date_day")
    ),
    "events": PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="event_date_day")
    ),
}

# ═════════════════════════════════════════════════════════════════════════════
# CATALOG
# ═════════════════════════════════════════════════════════════════════════════

def get_catalog():
    os.makedirs(os.path.dirname(CATALOG_DB) or ".", exist_ok=True)
    catalog = load_catalog(
        "sql",
        uri=f"sqlite:///{CATALOG_DB}",
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint":          MINIO_ENDPOINT,
            "s3.access-key-id":     MINIO_ACCESS,
            "s3.secret-access-key": MINIO_SECRET,
            "s3.path-style-access": "true",
            "s3.region":            "us-east-1",
        },
    )
    return catalog


def ensure_namespace(catalog):
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"  Created namespace: {NAMESPACE}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Namespace exists: {NAMESPACE}")
        else:
            raise

# ═════════════════════════════════════════════════════════════════════════════
# DATA GENERATORS
# ═════════════════════════════════════════════════════════════════════════════

def _rand_date(start_year=2023, end_year=2024):
    start = datetime(start_year, 1, 1)
    delta = (datetime(end_year, 12, 31) - start).days
    return (start + timedelta(days=random.randint(0, delta))).date()


def _rand_ts(base_date):
    return datetime.combine(base_date, datetime.min.time()) + timedelta(
        hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)
    )


def generate_customers(n):
    records = {k: [] for k in ["customer_id", "email", "first_name", "last_name",
                                "country", "city", "signup_date", "is_active", "lifetime_value"]}
    for i in range(1, n + 1):
        first, last = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
        country = random.choice(COUNTRIES)
        records["customer_id"].append(i)
        records["email"].append(f"{first.lower()}.{last.lower()}{i}@example.com")
        records["first_name"].append(first)
        records["last_name"].append(last)
        records["country"].append(country)
        records["city"].append(random.choice(CITIES[country]))
        records["signup_date"].append(_rand_date(2020, 2024))
        records["is_active"].append(random.random() > 0.15)
        records["lifetime_value"].append(Decimal(str(round(random.uniform(0, 10000), 2))))

    return pa.table(records, schema=pa.schema([
        ("customer_id", pa.int64()), ("email", pa.string()),
        ("first_name", pa.string()), ("last_name", pa.string()),
        ("country", pa.string()), ("city", pa.string()),
        ("signup_date", pa.date32()), ("is_active", pa.bool_()),
        ("lifetime_value", pa.decimal128(38, 2)),
    ]))


def generate_products(n):
    adj = ["Premium", "Basic", "Pro", "Ultra", "Lite", "Max", "Mini", "Plus"]
    nouns = ["Widget", "Gadget", "Device", "Tool", "Item", "Product", "Unit", "System"]
    records = {k: [] for k in ["product_id", "sku", "name", "category",
                                "subcategory", "price", "cost", "stock_quantity", "is_available"]}
    for i in range(1, n + 1):
        cat = random.choice(list(CATEGORIES.keys()))
        sub = random.choice(CATEGORIES[cat])
        price = round(random.uniform(9.99, 999.99), 2)
        records["product_id"].append(i)
        records["sku"].append(f"SKU-{cat[:3].upper()}-{i:05d}")
        records["name"].append(f"{random.choice(adj)} {random.choice(nouns)} {sub}")
        records["category"].append(cat)
        records["subcategory"].append(sub)
        records["price"].append(Decimal(str(price)))
        records["cost"].append(Decimal(str(round(price * random.uniform(0.3, 0.7), 2))))
        records["stock_quantity"].append(random.randint(0, 1000))
        records["is_available"].append(random.random() > 0.1)

    return pa.table(records, schema=pa.schema([
        ("product_id", pa.int64()), ("sku", pa.string()), ("name", pa.string()),
        ("category", pa.string()), ("subcategory", pa.string()),
        ("price", pa.decimal128(38, 2)), ("cost", pa.decimal128(38, 2)),
        ("stock_quantity", pa.int64()), ("is_available", pa.bool_()),
    ]))


def generate_orders(n, num_customers):
    records = {k: [] for k in ["order_id", "customer_id", "order_date", "order_timestamp",
                                "status", "shipping_country", "shipping_city",
                                "total_amount", "discount_amount"]}
    for i in range(1, n + 1):
        od = _rand_date(2023, 2024)
        country = random.choice(COUNTRIES)
        total = round(random.uniform(25, 2500), 2)
        records["order_id"].append(i)
        records["customer_id"].append(random.randint(1, num_customers))
        records["order_date"].append(od)
        records["order_timestamp"].append(_rand_ts(od))
        records["status"].append(random.choice(ORDER_STATUSES))
        records["shipping_country"].append(country)
        records["shipping_city"].append(random.choice(CITIES[country]))
        records["total_amount"].append(Decimal(str(total)))
        records["discount_amount"].append(Decimal(str(round(total * random.uniform(0, 0.2), 2))))

    return pa.table(records, schema=pa.schema([
        ("order_id", pa.int64()), ("customer_id", pa.int64()),
        ("order_date", pa.date32()), ("order_timestamp", pa.timestamp("us")),
        ("status", pa.string()), ("shipping_country", pa.string()),
        ("shipping_city", pa.string()), ("total_amount", pa.decimal128(38, 2)),
        ("discount_amount", pa.decimal128(38, 2)),
    ]))


def generate_order_items(n, num_orders, num_products):
    records = {k: [] for k in ["item_id", "order_id", "product_id",
                                "quantity", "unit_price", "line_total"]}
    for i in range(1, n + 1):
        qty = random.randint(1, 10)
        price = round(random.uniform(9.99, 499.99), 2)
        records["item_id"].append(i)
        records["order_id"].append(random.randint(1, num_orders))
        records["product_id"].append(random.randint(1, num_products))
        records["quantity"].append(qty)
        records["unit_price"].append(Decimal(str(price)))
        records["line_total"].append(Decimal(str(round(qty * price, 2))))

    return pa.table(records, schema=pa.schema([
        ("item_id", pa.int64()), ("order_id", pa.int64()), ("product_id", pa.int64()),
        ("quantity", pa.int64()), ("unit_price", pa.decimal128(38, 2)),
        ("line_total", pa.decimal128(38, 2)),
    ]))


def generate_events(n, num_customers, num_products):
    pages = ["/", "/products", "/category", "/cart", "/checkout", "/account", "/search", "/about"]
    records = {k: [] for k in ["event_id", "event_timestamp", "event_date", "customer_id",
                                "event_type", "page_url", "product_id", "session_id",
                                "device_type", "country"]}
    for i in range(1, n + 1):
        ed = _rand_date(2024, 2024)
        et = random.choice(EVENT_TYPES)
        cid = random.randint(1, num_customers) if random.random() > 0.3 else None
        pid = random.randint(1, num_products) if et in ("product_view", "add_to_cart", "purchase") else None
        records["event_id"].append(i)
        records["event_timestamp"].append(_rand_ts(ed))
        records["event_date"].append(ed)
        records["customer_id"].append(cid)
        records["event_type"].append(et)
        records["page_url"].append(f"https://shop.example.com{random.choice(pages)}")
        records["product_id"].append(pid)
        records["session_id"].append(f"sess_{random.randint(100000, 999999)}")
        records["device_type"].append(random.choice(DEVICE_TYPES))
        records["country"].append(random.choice(COUNTRIES))

    return pa.table(records, schema=pa.schema([
        ("event_id", pa.int64()), ("event_timestamp", pa.timestamp("us")),
        ("event_date", pa.date32()), ("customer_id", pa.int64()),
        ("event_type", pa.string()), ("page_url", pa.string()),
        ("product_id", pa.int64()), ("session_id", pa.string()),
        ("device_type", pa.string()), ("country", pa.string()),
    ]))

# ═════════════════════════════════════════════════════════════════════════════
# PGAA SQL GENERATOR
# ═════════════════════════════════════════════════════════════════════════════

def generate_pgaa_sql(table_locations):
    return f"""-- ═══════════════════════════════════════════════════════════════════════════════
-- PGAA Foreign Tables for Iceberg Data on MinIO
-- Generated: {datetime.now().isoformat()}
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Create extension
CREATE EXTENSION IF NOT EXISTS pgaa;

-- 2. Create server for MinIO/Iceberg
DROP SERVER IF EXISTS iceberg_minio CASCADE;

CREATE SERVER iceberg_minio
    FOREIGN DATA WRAPPER pgaa_fdw
    OPTIONS (
        format 'iceberg',
        endpoint '{MINIO_ENDPOINT}',
        path_style_access 'true'
    );

-- 3. Create user mapping
CREATE USER MAPPING FOR gpadmin
    SERVER iceberg_minio
    OPTIONS (
        access_key_id '{MINIO_ACCESS}',
        secret_access_key '{MINIO_SECRET}'
    );

-- ═══════════════════════════════════════════════════════════════════════════════
-- Foreign Tables
-- ═══════════════════════════════════════════════════════════════════════════════

DROP FOREIGN TABLE IF EXISTS customers_iceberg;
CREATE FOREIGN TABLE customers_iceberg (
    customer_id BIGINT, email TEXT, first_name TEXT, last_name TEXT,
    country TEXT, city TEXT, signup_date DATE, is_active BOOLEAN,
    lifetime_value NUMERIC(38,2)
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["customers"]}');

DROP FOREIGN TABLE IF EXISTS products_iceberg;
CREATE FOREIGN TABLE products_iceberg (
    product_id BIGINT, sku TEXT, name TEXT, category TEXT, subcategory TEXT,
    price NUMERIC(38,2), cost NUMERIC(38,2), stock_quantity BIGINT, is_available BOOLEAN
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["products"]}');

DROP FOREIGN TABLE IF EXISTS orders_iceberg;
CREATE FOREIGN TABLE orders_iceberg (
    order_id BIGINT, customer_id BIGINT, order_date DATE,
    order_timestamp TIMESTAMP, status TEXT, shipping_country TEXT,
    shipping_city TEXT, total_amount NUMERIC(38,2), discount_amount NUMERIC(38,2)
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["orders"]}');

DROP FOREIGN TABLE IF EXISTS order_items_iceberg;
CREATE FOREIGN TABLE order_items_iceberg (
    item_id BIGINT, order_id BIGINT, product_id BIGINT,
    quantity BIGINT, unit_price NUMERIC(38,2), line_total NUMERIC(38,2)
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["order_items"]}');

DROP FOREIGN TABLE IF EXISTS events_iceberg;
CREATE FOREIGN TABLE events_iceberg (
    event_id BIGINT, event_timestamp TIMESTAMP, event_date DATE,
    customer_id BIGINT, event_type TEXT, page_url TEXT,
    product_id BIGINT, session_id TEXT, device_type TEXT, country TEXT
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["events"]}');

-- ═══════════════════════════════════════════════════════════════════════════════
-- Also create native copies for performance comparison (Lab 2 demo)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS demo;

DROP TABLE IF EXISTS demo.customers CASCADE;
CREATE TABLE demo.customers AS SELECT * FROM customers_iceberg DISTRIBUTED BY (customer_id);

DROP TABLE IF EXISTS demo.products CASCADE;
CREATE TABLE demo.products AS SELECT * FROM products_iceberg DISTRIBUTED BY (product_id);

DROP TABLE IF EXISTS demo.orders CASCADE;
CREATE TABLE demo.orders AS SELECT * FROM orders_iceberg DISTRIBUTED BY (order_id);

DROP TABLE IF EXISTS demo.order_items CASCADE;
CREATE TABLE demo.order_items AS SELECT * FROM order_items_iceberg DISTRIBUTED BY (item_id);

DROP TABLE IF EXISTS demo.events CASCADE;
CREATE TABLE demo.events AS SELECT * FROM events_iceberg DISTRIBUTED BY (event_id);

ANALYZE demo.customers; ANALYZE demo.products; ANALYZE demo.orders;
ANALYZE demo.order_items; ANALYZE demo.events;

-- ═══════════════════════════════════════════════════════════════════════════════
-- Test Queries
-- ═══════════════════════════════════════════════════════════════════════════════

-- Verify row counts
SELECT 'customers' AS tbl, COUNT(*) FROM customers_iceberg
UNION ALL SELECT 'products', COUNT(*) FROM products_iceberg
UNION ALL SELECT 'orders', COUNT(*) FROM orders_iceberg
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items_iceberg
UNION ALL SELECT 'events', COUNT(*) FROM events_iceberg
ORDER BY 2 DESC;
"""


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Generate Iceberg e-commerce data on MinIO")
    parser.add_argument("--scale", type=int, default=300,
                        help="Scale factor (300=default, 10=10x rows)")
    args = parser.parse_args()

    s = args.scale
    counts = {
        "customers":   1_000 * s,
        "products":      500 * s,
        "orders":      5_000 * s,
        "order_items": 15_000 * s,
        "events":      50_000 * s,
    }
    total = sum(counts.values())

    print("=" * 70)
    print("  Iceberg Data Generator — E-Commerce Dataset")
    print("=" * 70)
    print(f"  MinIO:     {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    print(f"  Warehouse: {WAREHOUSE}")
    print(f"  Catalog:   sqlite:///{CATALOG_DB}")
    print(f"  Scale:     {s}x  →  {total:,} total rows")
    for t, c in counts.items():
        print(f"    {t:<15} {c:>10,}")
    print("=" * 70)

    # Connect
    print("\n[1/4] Connecting to Iceberg catalog on MinIO...")
    catalog = get_catalog()
    ensure_namespace(catalog)
    print(f"  ✓ Ready")

    # Generate & write
    print(f"\n[2/4] Generating data and writing to Iceberg...\n")
    table_locations = {}
    t_start = time.perf_counter()

    generators = [
        ("customers",   generate_customers,   [counts["customers"]]),
        ("products",    generate_products,     [counts["products"]]),
        ("orders",      generate_orders,       [counts["orders"], counts["customers"]]),
        ("order_items", generate_order_items,  [counts["order_items"], counts["orders"], counts["products"]]),
        ("events",      generate_events,       [counts["events"], counts["customers"], counts["products"]]),
    ]

    for name, gen_fn, gen_args in generators:
        print(f"  {name}...")

        # Generate
        t0 = time.perf_counter()
        data = gen_fn(*gen_args)
        gen_time = time.perf_counter() - t0

        # Create or replace table
        full = f"{NAMESPACE}.{name}"
        try:
            catalog.drop_table(full)
        except Exception:
            pass

        spec = PARTITION_SPECS.get(name)
        if spec:
            tbl = catalog.create_table(full, schema=SCHEMAS[name], partition_spec=spec)
        else:
            tbl = catalog.create_table(full, schema=SCHEMAS[name])

        # Write
        t1 = time.perf_counter()
        tbl.append(data)
        write_time = time.perf_counter() - t1

        table_locations[name] = tbl.location()
        print(f"    {len(data):>10,} rows   gen={gen_time:.1f}s  write={write_time:.1f}s  ✓")

    total_time = time.perf_counter() - t_start

    # Verify
    print(f"\n[3/4] Verifying tables...\n")
    for name in ["customers", "products", "orders", "order_items", "events"]:
        try:
            t = catalog.load_table(f"{NAMESPACE}.{name}")
            count = t.scan().to_arrow().num_rows
            print(f"    {name:<15} {count:>10,} rows  ✓")
        except Exception as e:
            print(f"    {name:<15} ERROR: {e}")

    # Generate PGAA SQL
    print(f"\n[4/4] Generating PGAA SQL...")
    sql = generate_pgaa_sql(table_locations)
    sql_path = os.path.join(os.path.dirname(CATALOG_DB) or ".", "pgaa_tables.sql")
    try:
        with open(sql_path, "w") as f:
            f.write(sql)
        print(f"  ✓ Saved to {sql_path}")
    except Exception:
        sql_path = "pgaa_tables.sql"
        with open(sql_path, "w") as f:
            f.write(sql)
        print(f"  ✓ Saved to {sql_path}")
    
    # Optimization
    # print(f"\n[5/5] Generating PGAA SQL...")
    # table_list = ["customers", "products", "orders", "order_items", "events"]
    # optimize_tables(catalog, table_list)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  Done!                                                           ║
║  Tables:       5 Optimized Iceberg tables on MinIO               ║
║  Total rows:   {total:>10,}                                       ║
║  Time:         {total_time:>6.1f}s                                          ║
║                                                                  ║
║  Next steps:                                                     ║
║    1. Run on WHPG:  psql -f {sql_path:<35}║
║    2. Start dashboard: python3 pgaa_dashboard_app.py             ║
║    3. Open: http://localhost:5000                                 ║
╚══════════════════════════════════════════════════════════════════╝
""")

    # Print locations for reference
    print("Table locations (for manual PGAA setup):")
    for name, loc in table_locations.items():
        print(f"  {name}: {loc}")


if __name__ == "__main__":
    main()
