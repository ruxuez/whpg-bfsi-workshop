-- ============================================================================
-- Meridian Retail Bank × EDB WarehousePG — Card-Fraud & AML Data Model
-- ============================================================================
-- Card fraud + AML monitoring | Native int8range + JSONB/ISO 20022 | WHPG MPP
-- Target: EDB WarehousePG (Greenplum 7 lineage, PostgreSQL 12.12 core)
--
-- Run order:
--   01_schema.sql            -> Tables, types, indexes, views
--   02_seed_reference.sql    -> Reference / lookup data
--   03_seed_traffic_with_personas.sql -> Synthetic transactions (4 personas)
--   04_demo_queries.sql      -> Lab 1 hero queries (JSONB + int8range)
--   05_pgaa_tables.sql       -> Lab 2 lakehouse (Iceberg + native AOCO)
--   06_lab3_ai_analytics.sql -> Lab 3 pgvector + MADlib K-Means
--   07_kmeans_fallback.sql   -> Lab 3 clustering (MADlib or pure-SQL)
--   08_add_diverse_narratives.sql -> Richer SAR/case text for semantic search
--
-- PG12 NOTES (do NOT regress to newer syntax):
--   * No underscore numeric separators (write 100000000, not 100_000_000)
--   * No gen_random_uuid() builtin (PG13+); use serials/bigints
--   * No multiranges / MERGE / date_bin (PG14+/PG15+)
--   * SQL/JSON path (@?, @@, jsonpath) IS available in PG12 — used in Lab 1
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS bfsi_demo;
SET search_path TO bfsi_demo, public;

-- ============================================================================
-- REFERENCE / DIMENSION TABLES
-- ============================================================================

-- ─── Regions (booking entities / processing regions) ────────────────────────
CREATE TABLE regions (
    region_id       SERIAL,
    region_code     VARCHAR(16)  NOT NULL,
    region_name     VARCHAR(64)  NOT NULL,
    timezone        VARCHAR(40)  NOT NULL
) DISTRIBUTED REPLICATED;   -- tiny lookup: replicate so joins are local

-- ─── BIN ranges (card scheme master) ────────────────────────────────────────
-- KEY: bin_range is int8range — native range containment, no UDF / string math.
-- A card's BIN (first 8 digits) is contained in exactly one issuing program.
CREATE TABLE bin_ranges (
    bin_range_id    SERIAL,
    bin_range       int8range     NOT NULL,   -- e.g. int8range(40000000, 40009999, '[]')
    scheme          VARCHAR(16)   NOT NULL,    -- VISA | MASTERCARD | AMEX | DISCOVER
    issuer_name     VARCHAR(128)  NOT NULL,
    product_type    VARCHAR(20)   NOT NULL DEFAULT 'credit',
                    -- credit | debit | prepaid | commercial
    country_code    CHAR(2),
    region_id       INT           NOT NULL,
    parent_range    int8range,
    created_at      TIMESTAMP     NOT NULL DEFAULT now()
) DISTRIBUTED REPLICATED;   -- small range master: replicate for local <@ joins

-- Native GiST index over the int8range — accelerates <@ / && / @> containment
CREATE INDEX idx_bin_ranges_gist ON bin_ranges USING gist (bin_range);

-- ─── Customers (bank customers) ─────────────────────────────────────────────
-- account_range: the contiguous PAN/account band issued to this portfolio.
CREATE TABLE customers (
    customer_id     SERIAL,
    customer_name   VARCHAR(128)  NOT NULL,
    region_id       INT           NOT NULL,
    segment         VARCHAR(16)   NOT NULL DEFAULT 'retail',
                    -- private | retail | sme
    kyc_risk        VARCHAR(8)    NOT NULL DEFAULT 'low',  -- low | medium | high
    account_range   int8range,                              -- portfolio PAN band
    onboarded_at    DATE          NOT NULL DEFAULT CURRENT_DATE
) DISTRIBUTED BY (customer_id);

-- ─── Risk Profiles (expected-behaviour thresholds per customer) ─────────────
-- Replaces the SLA contract: the monitored "limits" a portfolio should respect.
CREATE TABLE risk_profiles (
    profile_id          SERIAL,
    customer_id         INT           NOT NULL,
    max_daily_amount    NUMERIC(18,2) NOT NULL DEFAULT 10000.00,
    max_txn_velocity    INT           NOT NULL DEFAULT 60,    -- txns/hour
    max_decline_rate    NUMERIC(5,2)  NOT NULL DEFAULT 5.00,  -- %
    expected_avg_ticket NUMERIC(12,2) NOT NULL DEFAULT 75.00,
    effective_from      DATE          NOT NULL DEFAULT CURRENT_DATE,
    effective_to        DATE
) DISTRIBUTED BY (customer_id);

-- ─── Fraud Watchlists (compromised BINs, sanctions, mules) ──────────────────
-- KEY: bin_range is int8range — enables native <@ containment joins.
-- Compromised-BIN feeds (Visa CAMS, MC ADC) arrive as BIN *ranges*.
CREATE TABLE fraud_watchlists (
    feed_id         SERIAL,
    feed_name       VARCHAR(64)   NOT NULL,    -- Visa CAMS | MC ADC | OFAC | Internal
    bin_range       int8range,                 -- compromised BIN band (nullable)
    single_account  BIGINT,                    -- specific mule / fraud account
    category        VARCHAR(32)   NOT NULL,
                    -- compromised_bin | sanctioned | mule | known_fraud
                    -- | high_risk_corridor | velocity_abuse
    confidence      INT           NOT NULL,
    country_code    CHAR(2),
    first_seen      TIMESTAMP     NOT NULL DEFAULT now(),
    last_seen       TIMESTAMP     NOT NULL DEFAULT now(),
    active          BOOLEAN       NOT NULL DEFAULT TRUE
) DISTRIBUTED REPLICATED;   -- small watchlist: replicate so the 13M-row fact table is NOT broadcast

CREATE INDEX idx_watchlist_gist ON fraud_watchlists USING gist (bin_range);

-- ─── Country Risk (AML geo enrichment) ──────────────────────────────────────
CREATE TABLE country_risk (
    country_code    CHAR(2)       NOT NULL,
    country_name    VARCHAR(64)   NOT NULL,
    region          VARCHAR(32),
    risk_score      INT           NOT NULL DEFAULT 0,   -- 0 (low) .. 100 (high)
    fatf_status     VARCHAR(12)   NOT NULL DEFAULT 'compliant', -- compliant|grey|black
    is_sanctioned   BOOLEAN       NOT NULL DEFAULT FALSE,
    latitude        NUMERIC(8,5),
    longitude       NUMERIC(8,5)
) DISTRIBUTED REPLICATED;   -- tiny country dimension: replicate


-- ============================================================================
-- FACT / EVENT TABLES  (Append-optimized columnar, partitioned by time)
-- ============================================================================

-- ─── CORE: Card / Payment Transactions ──────────────────────────────────────
-- KEY 1: card_bin BIGINT  -> native int8range containment vs BIN ranges/feeds
-- KEY 2: iso_msg JSONB     -> ISO 20022 payment message (Lab 1 hero, GIN-indexed)
--
-- NOTE on GIN + append-optimized: WarehousePG (GP7) supports GIN over AO/CO
-- tables. If a given build does not, change orientation to 'row' (heap) for
-- this table, or drop the GIN index and rely on MPP columnar scan for @>.
CREATE TABLE transactions (
    txn_id          BIGSERIAL,
    ts              TIMESTAMP     NOT NULL,
    account_id      BIGINT        NOT NULL,
    card_bin        BIGINT        NOT NULL,    -- 8-digit BIN, drives int8range join
    pan_last4       CHAR(4),
    amount          NUMERIC(18,2) NOT NULL,
    currency        CHAR(3)       NOT NULL DEFAULT 'USD',
    mcc             INT,                        -- merchant category code
    merchant_id     BIGINT,
    merchant_name   VARCHAR(96),
    merchant_country CHAR(2),
    channel         VARCHAR(8)    NOT NULL,     -- POS | ECOM | ATM | WIRE | INST | P2P
    txn_type        VARCHAR(12)   NOT NULL,     -- purchase | withdrawal | transfer | refund
    auth_response   CHAR(2)       NOT NULL DEFAULT '00', -- 00=approve, else decline
    beneficiary_account BIGINT,
    beneficiary_country CHAR(2),
    iso_msg         JSONB,                       -- ISO 20022 pacs.008 / pain.001
    region_id       INT
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (txn_id)
PARTITION BY RANGE (ts) (
    START ('2025-12-01'::timestamp) INCLUSIVE
    END   ('2026-12-01'::timestamp) EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- GIN over the ISO 20022 message: accelerates @> containment and @? jsonpath.
CREATE INDEX idx_txn_iso_gin ON transactions USING gin (iso_msg jsonb_path_ops);

-- ─── Case / SAR Narratives (free text for semantic search) ──────────────────
CREATE TABLE case_narratives (
    note_id         BIGSERIAL,
    ts              TIMESTAMP     NOT NULL,
    account_id      BIGINT,
    card_bin        BIGINT,
    analyst         VARCHAR(64),
    queue           VARCHAR(32),                -- triage queue / source system
    severity        SMALLINT      NOT NULL,     -- 1=critical .. 5=info
    narrative       TEXT,
    region_id       INT
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (account_id, card_bin)
PARTITION BY RANGE (ts) (
    START ('2025-12-01'::timestamp) INCLUSIVE
    END   ('2026-12-01'::timestamp) EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- ─── Authorization Decisions (approve / decline / step-up) ──────────────────
CREATE TABLE auth_decisions (
    decision_id     BIGSERIAL,
    ts              TIMESTAMP     NOT NULL,
    account_id      BIGINT        NOT NULL,
    card_bin        BIGINT,
    mcc             INT,
    amount          NUMERIC(18,2),
    decision        VARCHAR(10)   NOT NULL,     -- APPROVE | DECLINE | STEP_UP | BLOCK
    rule_id         INT,
    channel         VARCHAR(8),
    merchant_country CHAR(2),
    region_id       INT
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (account_id, card_bin)
PARTITION BY RANGE (ts) (
    START ('2025-12-01'::timestamp) INCLUSIVE
    END   ('2026-12-01'::timestamp) EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- ─── Digital / Device Events (login, payee-add, new device) ─────────────────
CREATE TABLE device_events (
    device_evt_id   BIGSERIAL,
    ts              TIMESTAMP     NOT NULL,
    account_id      BIGINT        NOT NULL,
    device_fingerprint VARCHAR(64),
    ip_country      CHAR(2),
    channel         VARCHAR(8),
    event_type      VARCHAR(24)   NOT NULL,     -- login | payee_add | pwd_reset | new_device
    result          VARCHAR(8)    NOT NULL,     -- OK | FAIL | FLAG
    region_id       INT
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (device_evt_id)
PARTITION BY RANGE (ts) (
    START ('2025-12-01'::timestamp) INCLUSIVE
    END   ('2026-12-01'::timestamp) EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- ─── Wire / Interbank Events (SWIFT / SEPA / ACH routing) ───────────────────
CREATE TABLE wire_events (
    wire_id         BIGSERIAL,
    ts              TIMESTAMP     NOT NULL,
    ordering_account BIGINT       NOT NULL,
    beneficiary_bic VARCHAR(11),
    beneficiary_country CHAR(2),
    event_type      VARCHAR(12)   NOT NULL,     -- SENT | RETURNED | RECALLED | AMENDED
    rail            VARCHAR(10),                -- SWIFT | SEPA | ACH | FEDWIRE
    amount          NUMERIC(18,2),
    region_id       INT
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (wire_id)
PARTITION BY RANGE (ts) (
    START ('2025-12-01'::timestamp) INCLUSIVE
    END   ('2026-12-01'::timestamp) EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- ─── BIN Inventory (issued BIN allocations — utilization) ───────────────────
CREATE TABLE bin_inventory (
    alloc_id        SERIAL,
    bin             BIGINT        NOT NULL,
    bin_range_id    INT           NOT NULL,
    product         VARCHAR(20),
    status          VARCHAR(16)   NOT NULL DEFAULT 'active',
                    -- active | reserved | deprecated | available
    issued_at       TIMESTAMP     NOT NULL DEFAULT now(),
    region_id       INT
) DISTRIBUTED BY (alloc_id);

-- ─── BIN Utilization Summary (refreshed) ────────────────────────────────────
CREATE TABLE bin_utilization (
    bin_range_id    INT,
    bin_range       int8range     NOT NULL,
    region_id       INT,
    total_bins      INT           NOT NULL,
    allocated_bins  INT           NOT NULL,
    reserved_bins   INT           NOT NULL DEFAULT 0,
    utilization_pct NUMERIC(5,1)  NOT NULL,
    last_refreshed  TIMESTAMP     NOT NULL DEFAULT now()
) DISTRIBUTED BY (bin_range_id);

-- ─── Account KPIs (per-customer behavioural time series) ────────────────────
-- Replaces network_metrics: the signals monitored against risk_profiles.
CREATE TABLE account_kpis (
    kpi_id          BIGSERIAL,
    ts              TIMESTAMP     NOT NULL,
    customer_id     INT           NOT NULL,
    region_id       INT,
    txn_velocity    NUMERIC(8,2)  NOT NULL,     -- txns/hour
    avg_ticket      NUMERIC(12,2) NOT NULL,
    decline_rate_pct NUMERIC(5,2) NOT NULL,
    fraud_bps       NUMERIC(8,2),               -- fraud basis points
    chargeback_rate_pct NUMERIC(5,2)
) WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (kpi_id)
PARTITION BY RANGE (ts) (
    START ('2025-12-01'::timestamp) INCLUSIVE
    END   ('2026-12-01'::timestamp) EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra
);

-- ─── Fraud Cases (investigation lifecycle) ──────────────────────────────────
CREATE TABLE fraud_cases (
    case_id         SERIAL,
    ts              TIMESTAMP     NOT NULL DEFAULT now(),
    account_id      BIGINT        NOT NULL,
    card_bin        BIGINT,
    fraud_category  VARCHAR(32)   NOT NULL,
    severity        VARCHAR(10)   NOT NULL,     -- critical | high | medium | low
    feed_id         INT,
    matched_rule    VARCHAR(64),
    narrative       TEXT,
    status          VARCHAR(16)   NOT NULL DEFAULT 'open',
                    -- open | investigating | confirmed | closed | sar_filed
    region_id       INT
) DISTRIBUTED BY (case_id);


-- ============================================================================
-- VIEWS  (Ready-to-use analytics layers)
-- ============================================================================

-- ─── Transaction Summary (hourly) ───────────────────────────────────────────
CREATE VIEW v_txn_hourly AS
SELECT
    date_trunc('hour', ts)                  AS hour,
    r.region_code,
    COUNT(*)                                AS txn_count,
    SUM(amount)                             AS total_amount,
    COUNT(DISTINCT account_id)              AS unique_accounts,
    COUNT(DISTINCT merchant_id)             AS unique_merchants,
    COUNT(*) FILTER (WHERE auth_response <> '00') AS declines
FROM transactions t
LEFT JOIN regions r ON t.region_id = r.region_id
GROUP BY 1, 2;

-- ─── Spend Anomaly Detection (per-account hourly spend > 3 sigma) ───────────
CREATE VIEW v_txn_anomalies AS
WITH hourly AS (
    SELECT
        date_trunc('hour', ts)  AS hour,
        account_id,
        SUM(amount)             AS total_amount,
        COUNT(*)                AS txn_count
    FROM transactions
    WHERE ts > '2026-06-24'::timestamp
    GROUP BY 1, 2
),
stats AS (
    SELECT
        account_id,
        AVG(total_amount)                          AS avg_amount,
        STDDEV_POP(total_amount)                   AS std_amount
    FROM hourly
    GROUP BY account_id
)
SELECT
    h.hour,
    h.account_id,
    h.total_amount,
    h.txn_count,
    s.avg_amount,
    ROUND((h.total_amount - s.avg_amount) / NULLIF(s.std_amount, 0), 2) AS z_score
FROM hourly h
JOIN stats s USING (account_id)
WHERE (h.total_amount - s.avg_amount) / NULLIF(s.std_amount, 0) > 3
ORDER BY z_score DESC;

-- ─── Cross-source Alert Correlation (case + auth + device) ──────────────────
CREATE VIEW v_correlated_alerts AS
SELECT
    n.ts                       AS case_ts,
    n.account_id,
    n.severity                 AS case_severity,
    n.narrative                AS case_narrative,
    a.decision                 AS auth_decision,
    a.amount                   AS auth_amount,
    a.merchant_country         AS auth_country,
    d.event_type               AS device_event,
    d.result                   AS device_result
FROM case_narratives n
JOIN auth_decisions a
    ON n.account_id = a.account_id
    AND a.ts BETWEEN n.ts - interval '5 minutes'
                 AND n.ts + interval '5 minutes'
LEFT JOIN device_events d
    ON n.account_id = d.account_id
    AND d.ts BETWEEN n.ts - interval '10 minutes'
                  AND n.ts + interval '10 minutes'
WHERE n.severity <= 3;

-- ─── BIN Utilization Dashboard ──────────────────────────────────────────────
CREATE VIEW v_bin_utilization AS
SELECT
    b.bin_range,
    (upper(b.bin_range) - lower(b.bin_range))  AS range_size,
    br.issuer_name,
    br.scheme,
    r.region_code,
    b.total_bins,
    b.allocated_bins,
    b.reserved_bins,
    b.utilization_pct,
    CASE
        WHEN b.utilization_pct >= 90 THEN 'critical'
        WHEN b.utilization_pct >= 70 THEN 'warning'
        ELSE 'healthy'
    END                                        AS health_status,
    b.last_refreshed
FROM bin_utilization b
JOIN bin_ranges br ON b.bin_range_id = br.bin_range_id
JOIN regions r ON br.region_id = r.region_id;

-- ─── BIN Range Overlap Detection (native range overlap operator) ────────────
CREATE VIEW v_bin_overlaps AS
SELECT
    a.bin_range_id  AS range_a_id,
    a.bin_range     AS range_a,
    b.bin_range_id  AS range_b_id,
    b.bin_range     AS range_b,
    a.issuer_name   AS issuer_a,
    b.issuer_name   AS issuer_b
FROM bin_ranges a
JOIN bin_ranges b ON a.bin_range_id < b.bin_range_id
WHERE a.bin_range && b.bin_range            -- native range overlap operator!
  AND NOT (a.bin_range @> b.bin_range)      -- exclude parent-child
  AND NOT (b.bin_range @> a.bin_range);

-- ─── Limit / Threshold Status (KPIs vs risk profile) ────────────────────────
CREATE VIEW v_limit_status AS
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    r.region_code,
    rp.max_txn_velocity,
    rp.max_decline_rate,
    rp.expected_avg_ticket,
    AVG(k.txn_velocity)                   AS avg_velocity,
    AVG(k.decline_rate_pct)               AS avg_decline_rate,
    AVG(k.avg_ticket)                     AS avg_ticket,
    AVG(k.fraud_bps)                      AS avg_fraud_bps,
    CASE
        WHEN AVG(k.txn_velocity) > rp.max_txn_velocity
          OR AVG(k.decline_rate_pct) > rp.max_decline_rate
        THEN 'BREACH'
        WHEN AVG(k.txn_velocity) > rp.max_txn_velocity * 0.8
          OR AVG(k.decline_rate_pct) > rp.max_decline_rate * 0.8
        THEN 'AT RISK'
        ELSE 'HEALTHY'
    END                                   AS limit_status
FROM customers c
JOIN risk_profiles rp ON c.customer_id = rp.customer_id
    AND rp.effective_to IS NULL
JOIN regions r ON c.region_id = r.region_id
JOIN account_kpis k ON c.customer_id = k.customer_id
    AND k.ts > '2026-06-30'::timestamp
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- ─── Watchlist Matches (live) — THE native containment killer feature ───────
CREATE VIEW v_watchlist_matches AS
SELECT
    t.ts,
    t.account_id,
    t.card_bin,
    t.amount,
    t.merchant_country,
    w.feed_name,
    w.category          AS fraud_category,
    w.confidence,
    cr.country_name     AS merchant_country_name,
    cr.risk_score       AS country_risk
FROM transactions t
JOIN fraud_watchlists w
    ON t.card_bin <@ w.bin_range        -- ⚡ NATIVE int8range containment
    AND w.active = TRUE
LEFT JOIN country_risk cr
    ON t.merchant_country = cr.country_code
WHERE t.ts > '2026-06-30'::timestamp
  AND w.confidence >= 70;


-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Count of BINs covered by an int8range band
CREATE OR REPLACE FUNCTION bin_range_size(r int8range) RETURNS BIGINT AS $$
    SELECT (upper(r) - lower(r))::bigint;
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- Composite fraud-risk score (0-100) from amount / decline rate / country risk
CREATE OR REPLACE FUNCTION fraud_risk_score(
    amount NUMERIC,
    decline_rate NUMERIC,
    country_risk NUMERIC
) RETURNS INT AS $$
    SELECT GREATEST(0, LEAST(100,
        ROUND(
              LEAST(amount / 100.0, 40)
            + (decline_rate * 3.0)
            + (country_risk * 0.4)
        )::INT
    ));
$$ LANGUAGE SQL IMMUTABLE STRICT;


-- ============================================================================
-- DONE — Schema ready
-- ============================================================================
-- Next: Run 02_seed_reference.sql to populate reference data
-- ============================================================================
