#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
Lab 2: Iceberg Data Generator for WHPG/PGAA Workshop
═══════════════════════════════════════════════════════════════════════════════

Generates a BFSI analytics dataset (customers, card_products, txn_archive,
txn_lines, digital_events) and writes Apache Iceberg tables on a local MinIO
instance. Mirrors the schema in 05_pgaa_tables.sql so PGAA can read it in place
and the native AOCO copies (schema bfsi_analytics) give the MPP speed comparison.

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
CARD_CATEGORIES = {
    "credit":     ["classic", "gold", "platinum", "world"],
    "debit":      ["classic", "gold", "platinum"],
    "prepaid":    ["classic", "gold"],
    "commercial": ["gold", "platinum", "world"],
}
TXN_STATUSES = ["settled", "settled", "settled", "settled", "reversed", "disputed", "refunded"]
EVENT_TYPES  = ["login", "login", "view_statement", "transfer", "payee_add", "card_freeze"]
DEVICE_TYPES = ["web", "ios", "android"]

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
    "card_products": Schema(
        NestedField(1, "product_id", LongType(), required=False),
        NestedField(2, "product_code", StringType(), required=False),
        NestedField(3, "name", StringType(), required=False),
        NestedField(4, "category", StringType(), required=False),
        NestedField(5, "tier", StringType(), required=False),
        NestedField(6, "annual_fee", DecimalType(38, 2), required=False),
        NestedField(7, "issuance_cost", DecimalType(38, 2), required=False),
        NestedField(8, "active_cards", LongType(), required=False),
        NestedField(9, "is_available", BooleanType(), required=False),
    ),
    "txn_archive": Schema(
        NestedField(1, "archive_id", LongType(), required=False),
        NestedField(2, "customer_id", LongType(), required=False),
        NestedField(3, "post_date", DateType(), required=False),
        NestedField(4, "txn_timestamp", TimestampType(), required=False),
        NestedField(5, "status", StringType(), required=False),
        NestedField(6, "txn_country", StringType(), required=False),
        NestedField(7, "txn_city", StringType(), required=False),
        NestedField(8, "total_amount", DecimalType(38, 2), required=False),
        NestedField(9, "fee_amount", DecimalType(38, 2), required=False),
    ),
    "txn_lines": Schema(
        NestedField(1, "line_id", LongType(), required=False),
        NestedField(2, "archive_id", LongType(), required=False),
        NestedField(3, "product_id", LongType(), required=False),
        NestedField(4, "quantity", LongType(), required=False),
        NestedField(5, "unit_price", DecimalType(38, 2), required=False),
        NestedField(6, "line_total", DecimalType(38, 2), required=False),
    ),
    "digital_events": Schema(
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
    "txn_archive": PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="post_date_day")
    ),
    "digital_events": PartitionSpec(
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
        records["email"].append(f"{first.lower()}.{last.lower()}{i}@meridianbank.example")
        records["first_name"].append(first)
        records["last_name"].append(last)
        records["country"].append(country)
        records["city"].append(random.choice(CITIES[country]))
        records["signup_date"].append(_rand_date(2018, 2024))
        records["is_active"].append(random.random() > 0.12)
        records["lifetime_value"].append(Decimal(str(round(random.uniform(0, 250000), 2))))

    return pa.table(records, schema=pa.schema([
        ("customer_id", pa.int64()), ("email", pa.string()),
        ("first_name", pa.string()), ("last_name", pa.string()),
        ("country", pa.string()), ("city", pa.string()),
        ("signup_date", pa.date32()), ("is_active", pa.bool_()),
        ("lifetime_value", pa.decimal128(38, 2)),
    ]))


def generate_card_products(n):
    names = ["Everyday", "Rewards", "Travel", "Cashback", "Business", "Student",
             "Signature", "Infinite", "Secured", "Premier"]
    records = {k: [] for k in ["product_id", "product_code", "name", "category",
                                "tier", "annual_fee", "issuance_cost", "active_cards", "is_available"]}
    for i in range(1, n + 1):
        cat = random.choice(list(CARD_CATEGORIES.keys()))
        tier = random.choice(CARD_CATEGORIES[cat])
        fee = {"classic": 0, "gold": 95, "platinum": 250, "world": 550}[tier]
        records["product_id"].append(i)
        records["product_code"].append(f"{cat[:3].upper()}-{tier[:1].upper()}-{i:05d}")
        records["name"].append(f"Meridian {random.choice(names)} {tier.title()}")
        records["category"].append(cat)
        records["tier"].append(tier)
        records["annual_fee"].append(Decimal(str(round(fee * random.uniform(0.8, 1.2), 2))))
        records["issuance_cost"].append(Decimal(str(round(random.uniform(2.5, 18.0), 2))))
        records["active_cards"].append(random.randint(0, 500000))
        records["is_available"].append(random.random() > 0.1)

    return pa.table(records, schema=pa.schema([
        ("product_id", pa.int64()), ("product_code", pa.string()), ("name", pa.string()),
        ("category", pa.string()), ("tier", pa.string()),
        ("annual_fee", pa.decimal128(38, 2)), ("issuance_cost", pa.decimal128(38, 2)),
        ("active_cards", pa.int64()), ("is_available", pa.bool_()),
    ]))


def generate_txn_archive(n, num_customers):
    records = {k: [] for k in ["archive_id", "customer_id", "post_date", "txn_timestamp",
                                "status", "txn_country", "txn_city", "total_amount", "fee_amount"]}
    for i in range(1, n + 1):
        od = _rand_date(2023, 2024)
        country = random.choice(COUNTRIES)
        total = round(random.uniform(5, 5000), 2)
        records["archive_id"].append(i)
        records["customer_id"].append(random.randint(1, num_customers))
        records["post_date"].append(od)
        records["txn_timestamp"].append(_rand_ts(od))
        records["status"].append(random.choice(TXN_STATUSES))
        records["txn_country"].append(country)
        records["txn_city"].append(random.choice(CITIES[country]))
        records["total_amount"].append(Decimal(str(total)))
        records["fee_amount"].append(Decimal(str(round(total * random.uniform(0, 0.03), 2))))

    return pa.table(records, schema=pa.schema([
        ("archive_id", pa.int64()), ("customer_id", pa.int64()),
        ("post_date", pa.date32()), ("txn_timestamp", pa.timestamp("us")),
        ("status", pa.string()), ("txn_country", pa.string()),
        ("txn_city", pa.string()), ("total_amount", pa.decimal128(38, 2)),
        ("fee_amount", pa.decimal128(38, 2)),
    ]))


def generate_txn_lines(n, num_archive, num_products):
    records = {k: [] for k in ["line_id", "archive_id", "product_id",
                                "quantity", "unit_price", "line_total"]}
    for i in range(1, n + 1):
        qty = random.randint(1, 6)
        price = round(random.uniform(1.99, 899.99), 2)
        records["line_id"].append(i)
        records["archive_id"].append(random.randint(1, num_archive))
        records["product_id"].append(random.randint(1, num_products))
        records["quantity"].append(qty)
        records["unit_price"].append(Decimal(str(price)))
        records["line_total"].append(Decimal(str(round(qty * price, 2))))

    return pa.table(records, schema=pa.schema([
        ("line_id", pa.int64()), ("archive_id", pa.int64()), ("product_id", pa.int64()),
        ("quantity", pa.int64()), ("unit_price", pa.decimal128(38, 2)),
        ("line_total", pa.decimal128(38, 2)),
    ]))


def generate_digital_events(n, num_customers, num_products):
    pages = ["/", "/accounts", "/statements", "/transfer", "/payees", "/cards", "/settings", "/support"]
    records = {k: [] for k in ["event_id", "event_timestamp", "event_date", "customer_id",
                                "event_type", "page_url", "product_id", "session_id",
                                "device_type", "country"]}
    for i in range(1, n + 1):
        ed = _rand_date(2024, 2024)
        et = random.choice(EVENT_TYPES)
        cid = random.randint(1, num_customers) if random.random() > 0.2 else None
        pid = random.randint(1, num_products) if et in ("transfer", "card_freeze") else None
        records["event_id"].append(i)
        records["event_timestamp"].append(_rand_ts(ed))
        records["event_date"].append(ed)
        records["customer_id"].append(cid)
        records["event_type"].append(et)
        records["page_url"].append(f"https://app.meridianbank.example{random.choice(pages)}")
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

DROP FOREIGN TABLE IF EXISTS card_products_iceberg;
CREATE FOREIGN TABLE card_products_iceberg (
    product_id BIGINT, product_code TEXT, name TEXT, category TEXT, tier TEXT,
    annual_fee NUMERIC(38,2), issuance_cost NUMERIC(38,2), active_cards BIGINT, is_available BOOLEAN
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["card_products"]}');

DROP FOREIGN TABLE IF EXISTS txn_archive_iceberg;
CREATE FOREIGN TABLE txn_archive_iceberg (
    archive_id BIGINT, customer_id BIGINT, post_date DATE,
    txn_timestamp TIMESTAMP, status TEXT, txn_country TEXT,
    txn_city TEXT, total_amount NUMERIC(38,2), fee_amount NUMERIC(38,2)
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["txn_archive"]}');

DROP FOREIGN TABLE IF EXISTS txn_lines_iceberg;
CREATE FOREIGN TABLE txn_lines_iceberg (
    line_id BIGINT, archive_id BIGINT, product_id BIGINT,
    quantity BIGINT, unit_price NUMERIC(38,2), line_total NUMERIC(38,2)
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["txn_lines"]}');

DROP FOREIGN TABLE IF EXISTS digital_events_iceberg;
CREATE FOREIGN TABLE digital_events_iceberg (
    event_id BIGINT, event_timestamp TIMESTAMP, event_date DATE,
    customer_id BIGINT, event_type TEXT, page_url TEXT,
    product_id BIGINT, session_id TEXT, device_type TEXT, country TEXT
) SERVER iceberg_minio OPTIONS (table_location '{table_locations["digital_events"]}');

-- ═══════════════════════════════════════════════════════════════════════════════
-- Also create native copies for performance comparison (Lab 2 demo)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS bfsi_analytics;

DROP TABLE IF EXISTS bfsi_analytics.customers CASCADE;
CREATE TABLE bfsi_analytics.customers AS SELECT * FROM customers_iceberg DISTRIBUTED BY (customer_id);

DROP TABLE IF EXISTS bfsi_analytics.card_products CASCADE;
CREATE TABLE bfsi_analytics.card_products AS SELECT * FROM card_products_iceberg DISTRIBUTED BY (product_id);

DROP TABLE IF EXISTS bfsi_analytics.txn_archive CASCADE;
CREATE TABLE bfsi_analytics.txn_archive AS SELECT * FROM txn_archive_iceberg DISTRIBUTED BY (archive_id);

DROP TABLE IF EXISTS bfsi_analytics.txn_lines CASCADE;
CREATE TABLE bfsi_analytics.txn_lines AS SELECT * FROM txn_lines_iceberg DISTRIBUTED BY (line_id);

DROP TABLE IF EXISTS bfsi_analytics.digital_events CASCADE;
CREATE TABLE bfsi_analytics.digital_events AS SELECT * FROM digital_events_iceberg DISTRIBUTED BY (event_id);

ANALYZE bfsi_analytics.customers; ANALYZE bfsi_analytics.card_products; ANALYZE bfsi_analytics.txn_archive;
ANALYZE bfsi_analytics.txn_lines; ANALYZE bfsi_analytics.digital_events;

-- ═══════════════════════════════════════════════════════════════════════════════
-- Test Queries
-- ═══════════════════════════════════════════════════════════════════════════════

-- Verify row counts
SELECT 'customers' AS tbl, COUNT(*) FROM customers_iceberg
UNION ALL SELECT 'card_products', COUNT(*) FROM card_products_iceberg
UNION ALL SELECT 'txn_archive', COUNT(*) FROM txn_archive_iceberg
UNION ALL SELECT 'txn_lines', COUNT(*) FROM txn_lines_iceberg
UNION ALL SELECT 'digital_events', COUNT(*) FROM digital_events_iceberg
ORDER BY 2 DESC;
"""


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Generate Iceberg BFSI lakehouse data on MinIO")
    parser.add_argument("--scale", type=int, default=300,
                        help="Scale factor (300=default, 10=10x rows)")
    args = parser.parse_args()

    s = args.scale
    counts = {
        "customers":      1_000 * s,
        "card_products":     50 * s,
        "txn_archive":    5_000 * s,
        "txn_lines":     15_000 * s,
        "digital_events":50_000 * s,
    }
    total = sum(counts.values())

    print("=" * 70)
    print("  Iceberg Data Generator — Meridian Bank BFSI Lakehouse")
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
        ("customers",      generate_customers,      [counts["customers"]]),
        ("card_products",  generate_card_products,  [counts["card_products"]]),
        ("txn_archive",    generate_txn_archive,    [counts["txn_archive"], counts["customers"]]),
        ("txn_lines",      generate_txn_lines,      [counts["txn_lines"], counts["txn_archive"], counts["card_products"]]),
        ("digital_events", generate_digital_events, [counts["digital_events"], counts["customers"], counts["card_products"]]),
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
    for name in ["customers", "card_products", "txn_archive", "txn_lines", "digital_events"]:
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
    # table_list = ["customers", "card_products", "txn_archive", "txn_lines", "digital_events"]
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
║    2. Start dashboard: python3 app2.py                           ║
║    3. Open: http://localhost:5000                                 ║
╚══════════════════════════════════════════════════════════════════╝
""")

    # Print locations for reference
    print("Table locations (for manual PGAA setup):")
    for name, loc in table_locations.items():
        print(f"  {name}: {loc}")


if __name__ == "__main__":
    main()
