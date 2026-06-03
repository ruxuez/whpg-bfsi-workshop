-- ═══════════════════════════════════════════════════════════════════════════════
-- Lab 2: PGAA Iceberg Tables on MinIO + Native WHPG Comparison (BFSI analytics)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Demonstrates EDB WarehousePG reading open-format Iceberg data in place (PGAA over
-- pgfs/MinIO), then materialising it into native AO-columnar tables for MPP speed.
-- The dataset is a banking analytics mart that complements the fraud/AML model:
--   customers · card_products · txn_archive · txn_lines · digital_events
--
-- Run AFTER the companion data generator has populated MinIO. The generator must
-- emit Iceberg datasets at the paths below with the column order shown in STEP 3
-- (PGAA infers the Iceberg schema; the native INSERT ... SELECT * is positional).
--
-- Adjust the S3 URL, endpoint, and credentials below to match your environment.
-- ═══════════════════════════════════════════════════════════════════════════════

-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  STEP 1: PGFS Storage Location                                             ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

SELECT pgfs.delete_storage_location('minio_iceberg');

SELECT pgfs.create_storage_location(
    name        => 'minio_iceberg',
    url         => 's3://warehouse/iceberg',
    options     => '{"endpoint": "http://minio:9000", "allow_http": "true"}',
    credentials => '{"access_key_id": "minioadmin", "secret_access_key": "minioadmin"}'
);

SELECT * FROM pgfs.list_storage_locations();


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  STEP 2: PGAA Iceberg Tables (read open-format data in place)              ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

DROP TABLE IF EXISTS customers_iceberg;
CREATE TABLE customers_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/customers',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS card_products_iceberg;
CREATE TABLE card_products_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/card_products',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS txn_archive_iceberg;
CREATE TABLE txn_archive_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/txn_archive',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS txn_lines_iceberg;
CREATE TABLE txn_lines_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/txn_lines',
    pgaa.format           = 'iceberg'
);

DROP TABLE IF EXISTS digital_events_iceberg;
CREATE TABLE digital_events_iceberg ()
USING PGAA WITH (
    pgaa.storage_location = 'minio_iceberg',
    pgaa.path             = 'analytics/digital_events',
    pgaa.format           = 'iceberg'
);

-- Quick check
SELECT 'customers_iceberg'      AS tbl, COUNT(*) FROM customers_iceberg
UNION ALL SELECT 'card_products_iceberg',  COUNT(*) FROM card_products_iceberg
UNION ALL SELECT 'txn_archive_iceberg',    COUNT(*) FROM txn_archive_iceberg
UNION ALL SELECT 'txn_lines_iceberg',      COUNT(*) FROM txn_lines_iceberg
UNION ALL SELECT 'digital_events_iceberg', COUNT(*) FROM digital_events_iceberg
ORDER BY 2 DESC;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  STEP 3: Native WHPG Tables (AO Columnar + ZSTD)                          ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

CREATE SCHEMA IF NOT EXISTS bfsi_analytics;

-- Customers (analytical copy of the bank's customer base)
DROP TABLE IF EXISTS bfsi_analytics.customers CASCADE;
CREATE TABLE bfsi_analytics.customers (
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
INSERT INTO bfsi_analytics.customers SELECT * FROM customers_iceberg;
ANALYZE bfsi_analytics.customers;

-- Card Products (credit / debit / prepaid programs)
DROP TABLE IF EXISTS bfsi_analytics.card_products CASCADE;
CREATE TABLE bfsi_analytics.card_products (
    product_id      BIGINT,
    product_code    TEXT,
    name            TEXT,
    category        TEXT,          -- credit | debit | prepaid | commercial
    tier            TEXT,          -- classic | gold | platinum | world
    annual_fee      NUMERIC(38,2),
    issuance_cost   NUMERIC(38,2),
    active_cards    BIGINT,
    is_available    BOOLEAN
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (product_id);
INSERT INTO bfsi_analytics.card_products SELECT * FROM card_products_iceberg;
ANALYZE bfsi_analytics.card_products;

-- Transaction Archive (historical settled transactions for analytics)
DROP TABLE IF EXISTS bfsi_analytics.txn_archive CASCADE;
CREATE TABLE bfsi_analytics.txn_archive (
    archive_id      BIGINT,
    customer_id     BIGINT,
    post_date       DATE,
    txn_timestamp   TIMESTAMP,
    status          TEXT,          -- settled | reversed | disputed | refunded
    txn_country     TEXT,
    txn_city        TEXT,
    total_amount    NUMERIC(38,2),
    fee_amount      NUMERIC(38,2)
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (archive_id);
INSERT INTO bfsi_analytics.txn_archive SELECT * FROM txn_archive_iceberg;
ANALYZE bfsi_analytics.txn_archive;

-- Transaction Lines (itemised charges / instalments)
DROP TABLE IF EXISTS bfsi_analytics.txn_lines CASCADE;
CREATE TABLE bfsi_analytics.txn_lines (
    line_id         BIGINT,
    archive_id      BIGINT,
    product_id      BIGINT,
    quantity        BIGINT,
    unit_price      NUMERIC(38,2),
    line_total      NUMERIC(38,2)
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (line_id);
INSERT INTO bfsi_analytics.txn_lines SELECT * FROM txn_lines_iceberg;
ANALYZE bfsi_analytics.txn_lines;

-- Digital Events (mobile / web banking clickstream)
DROP TABLE IF EXISTS bfsi_analytics.digital_events CASCADE;
CREATE TABLE bfsi_analytics.digital_events (
    event_id        BIGINT,
    event_timestamp TIMESTAMP,
    event_date      DATE,
    customer_id     BIGINT,
    event_type      TEXT,          -- login | view_statement | transfer | payee_add | card_freeze
    page_url        TEXT,
    product_id      BIGINT,
    session_id      TEXT,
    device_type     TEXT,
    country         TEXT
) WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED BY (event_id);
INSERT INTO bfsi_analytics.digital_events SELECT * FROM digital_events_iceberg;
ANALYZE bfsi_analytics.digital_events;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  STEP 4: Verify Row Counts (Iceberg-in-place vs native AOCO)              ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

DO $$
DECLARE r RECORD;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '╔══════════════════════════════════════════════════════════════╗';
    RAISE NOTICE '║  PGAA Lab 2 (BFSI) — Setup Complete                        ║';
    RAISE NOTICE '╠══════════════════════════════════════════════════════════════╣';
    FOR r IN
        SELECT 'customers'   AS tbl,
               (SELECT COUNT(*) FROM customers_iceberg)        AS ice,
               (SELECT COUNT(*) FROM bfsi_analytics.customers) AS nat
        UNION ALL SELECT 'card_products',
               (SELECT COUNT(*) FROM card_products_iceberg),
               (SELECT COUNT(*) FROM bfsi_analytics.card_products)
        UNION ALL SELECT 'txn_archive',
               (SELECT COUNT(*) FROM txn_archive_iceberg),
               (SELECT COUNT(*) FROM bfsi_analytics.txn_archive)
        UNION ALL SELECT 'txn_lines',
               (SELECT COUNT(*) FROM txn_lines_iceberg),
               (SELECT COUNT(*) FROM bfsi_analytics.txn_lines)
        UNION ALL SELECT 'digital_events',
               (SELECT COUNT(*) FROM digital_events_iceberg),
               (SELECT COUNT(*) FROM bfsi_analytics.digital_events)
        ORDER BY 2 DESC
    LOOP
        RAISE NOTICE '║  %  iceberg=% native=% %',
            RPAD(r.tbl, 14),
            LPAD(r.ice::text, 7),
            LPAD(r.nat::text, 7),
            CASE WHEN r.ice = r.nat THEN '✓' ELSE '✗' END;
    END LOOP;
    RAISE NOTICE '╚══════════════════════════════════════════════════════════════╝';
    RAISE NOTICE '';
    RAISE NOTICE 'Iceberg tables are queryable in place; native AOCO copies give MPP speed.';
END $$;
