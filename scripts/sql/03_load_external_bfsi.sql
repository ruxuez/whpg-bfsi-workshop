-- ═══════════════════════════════════════════════════════════════════════════════
-- 03_load_external_bfsi.sql
-- Meridian Retail Bank — bulk-load the BFSI fact tables from gpfdist CSVs
-- produced by data_generator_updated.py.  This is the external-table alternative
-- to the in-database seed 03_seed_traffic_with_personas.sql.
--
--   1.  python3 data_generator_updated.py --output-dir /data/csv --scale 1.0   # ~50M rows, June 2026
--   2.  gpfdist -d /data/csv -p 8081 &
--   3.  psql -d bank -f 03_load_external_bfsi.sql
--
-- The CSVs carry no header row and the column order matches 01_schema.sql minus
-- each table's auto-generated serial PK (txn_id, device_evt_id, decision_id,
-- note_id, wire_id, kpi_id), so the INSERTs let those default.
-- Replace GPFDIST_HOST with the host running gpfdist.
-- ═══════════════════════════════════════════════════════════════════════════════

SET search_path TO bfsi_demo, public;

-- Note: gpfdist must be running on the cdw container at port 8081
-- Start with: docker exec -d cdw gpfdist -d /data/csv -p 8081

-- ── transactions ────────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS ext_transactions;
CREATE READABLE EXTERNAL TABLE ext_transactions (
    ts TIMESTAMP, account_id BIGINT, card_bin BIGINT, pan_last4 CHAR(4),
    amount NUMERIC(18,2), currency CHAR(3), mcc INT, merchant_id BIGINT,
    merchant_name VARCHAR(120), merchant_country CHAR(2), channel VARCHAR(16),
    txn_type VARCHAR(16), auth_response CHAR(1), beneficiary_account BIGINT,
    beneficiary_country CHAR(2), iso_msg JSONB, region_id INT
) LOCATION ('gpfdist://cdw:8081/transactions.csv')
  FORMAT 'CSV' (DELIMITER ',' QUOTE '"' NULL '');

INSERT INTO transactions
    (ts, account_id, card_bin, pan_last4, amount, currency, mcc, merchant_id,
     merchant_name, merchant_country, channel, txn_type, auth_response,
     beneficiary_account, beneficiary_country, iso_msg, region_id)
SELECT ts, account_id, card_bin, pan_last4, amount, currency, mcc, merchant_id,
       merchant_name, merchant_country, channel, txn_type, auth_response,
       beneficiary_account, beneficiary_country, iso_msg, region_id
FROM ext_transactions;

-- ── device_events ───────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS ext_device_events;
CREATE READABLE EXTERNAL TABLE ext_device_events (
    ts TIMESTAMP, account_id BIGINT, device_fingerprint VARCHAR(64),
    ip_country CHAR(2), channel VARCHAR(16), event_type VARCHAR(24),
    result VARCHAR(16), region_id INT
) LOCATION ('gpfdist://cdw:8081/device_events.csv')
  FORMAT 'CSV' (DELIMITER ',' QUOTE '"' NULL '');

INSERT INTO device_events
    (ts, account_id, device_fingerprint, ip_country, channel, event_type, result, region_id)
SELECT ts, account_id, device_fingerprint, ip_country, channel, event_type, result, region_id
FROM ext_device_events;

-- ── auth_decisions ──────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS ext_auth_decisions;
CREATE READABLE EXTERNAL TABLE ext_auth_decisions (
    ts TIMESTAMP, account_id BIGINT, card_bin BIGINT, mcc INT,
    amount NUMERIC(18,2), decision VARCHAR(12), rule_id INT, channel VARCHAR(16),
    merchant_country CHAR(2), region_id INT
) LOCATION ('gpfdist://cdw:8081/auth_decisions.csv')
  FORMAT 'CSV' (DELIMITER ',' QUOTE '"' NULL '');

INSERT INTO auth_decisions
    (ts, account_id, card_bin, mcc, amount, decision, rule_id, channel, merchant_country, region_id)
SELECT ts, account_id, card_bin, mcc, amount, decision, rule_id, channel, merchant_country, region_id
FROM ext_auth_decisions;

-- ── case_narratives ─────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS ext_case_narratives;
CREATE READABLE EXTERNAL TABLE ext_case_narratives (
    ts TIMESTAMP, account_id BIGINT, card_bin BIGINT, analyst VARCHAR(40),
    queue VARCHAR(24), severity SMALLINT, narrative TEXT, region_id INT
) LOCATION ('gpfdist://cdw:8081/case_narratives.csv')
  FORMAT 'CSV' (DELIMITER ',' QUOTE '"' NULL '');

INSERT INTO case_narratives
    (ts, account_id, card_bin, analyst, queue, severity, narrative, region_id)
SELECT ts, account_id, card_bin, analyst, queue, severity, narrative, region_id
FROM ext_case_narratives;

-- ── wire_events ─────────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS ext_wire_events;
CREATE READABLE EXTERNAL TABLE ext_wire_events (
    ts TIMESTAMP, ordering_account BIGINT, beneficiary_bic VARCHAR(16),
    beneficiary_country CHAR(2), event_type VARCHAR(16), rail VARCHAR(8),
    amount NUMERIC(18,2), region_id INT
) LOCATION ('gpfdist://cdw:8081/wire_events.csv')
  FORMAT 'CSV' (DELIMITER ',' QUOTE '"' NULL '');

INSERT INTO wire_events
    (ts, ordering_account, beneficiary_bic, beneficiary_country, event_type, rail, amount, region_id)
SELECT ts, ordering_account, beneficiary_bic, beneficiary_country, event_type, rail, amount, region_id
FROM ext_wire_events;

-- ── account_kpis ────────────────────────────────────────────────────────────
DROP EXTERNAL TABLE IF EXISTS ext_account_kpis;
CREATE READABLE EXTERNAL TABLE ext_account_kpis (
    ts TIMESTAMP, customer_id INT, region_id INT, txn_velocity NUMERIC(10,1),
    avg_ticket NUMERIC(12,2), decline_rate_pct NUMERIC(6,2), fraud_bps NUMERIC(8,1),
    chargeback_rate_pct NUMERIC(6,2)
) LOCATION ('gpfdist://cdw:8081/account_kpis.csv')
  FORMAT 'CSV' (DELIMITER ',' QUOTE '"' NULL '');

INSERT INTO account_kpis
    (ts, customer_id, region_id, txn_velocity, avg_ticket, decline_rate_pct, fraud_bps, chargeback_rate_pct)
SELECT ts, customer_id, region_id, txn_velocity, avg_ticket, decline_rate_pct, fraud_bps, chargeback_rate_pct
FROM ext_account_kpis;

-- ANALYZE transactions;  ANALYZE device_events;  ANALYZE auth_decisions;
-- ANALYZE case_narratives;  ANALYZE wire_events;  ANALYZE account_kpis;
