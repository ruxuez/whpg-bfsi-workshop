-- ============================================================================
-- Meridian Retail Bank × EDB WarehousePG — Demo Queries (Labs 1 & forensics)
-- ============================================================================
-- Optimised for single-cluster demo (~13M transactions, ~28 days of data).
-- Each query targets < 5s execution.
--
-- Legend:
--   ⚡ = Native type operator (JSONB @> / @? or int8range <@) — the differentiator
--   🔗 = Cross-source correlation (SIEM / case-mgmt replacement value)
--   💰 = Direct cost saving vs current stack
-- ============================================================================
SET search_path TO bfsi_demo, public;
\timing on


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ LAB 1 · PART A — ISO 20022 PAYMENT MESSAGES IN JSONB  (THE HERO)         ║
-- ║ The competitor angle: Snowflake/Databricks store messages as VARIANT but  ║
-- ║ cannot GIN-index arbitrary containment — you flatten or full-scan. WHPG    ║
-- ║ indexes the raw message and answers containment in one operator.           ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─── 1A: SEPA Instant payments — JSONB containment (GIN-accelerated) ────────
-- ⚡ ONE operator (@>) vs flattening every nested field on a columnar warehouse
-- "Show me every SEPA Instant payment we settled today"
SELECT
    date_trunc('hour', ts)              AS hour,
    count(*)                            AS sepa_instant_payments,
    round(sum(amount), 2)               AS total_amount
FROM transactions
WHERE iso_msg @> '{"PmtTpInf":{"SvcLvl":{"Cd":"SEPA"},"LclInstrm":{"Cd":"INST"}}}'  -- ⚡
  AND ts > '2026-06-30'::timestamp
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;


-- ─── 1B: High-value payments via SQL/JSON path (@?) ─────────────────────────
-- ⚡ jsonpath predicate inside the message — find anything settling over 9,000
-- "Flag payments above the structuring threshold, straight from the message"
SELECT
    account_id,
    iso_msg #>> '{IntrBkSttlmAmt,Ccy}'         AS ccy,
    (iso_msg #>> '{IntrBkSttlmAmt,value}')::numeric AS settled_value,
    iso_msg #>> '{Cdtr,CtryOfRes}'             AS creditor_country,
    beneficiary_account
FROM transactions
WHERE iso_msg @? '$.IntrBkSttlmAmt ? (@.value > 9000)'   -- ⚡ jsonpath predicate
  AND ts > '2026-06-30'::timestamp
ORDER BY settled_value DESC
LIMIT 25;


-- ─── 1C: Spend by creditor (beneficiary) bank BIC — #>> path extraction ─────
-- ⚡ Reach straight into the nested CdtrAgt.FinInstnId.BICFI path
SELECT
    iso_msg #>> '{CdtrAgt,FinInstnId,BICFI}'   AS creditor_bic,
    count(*)                                    AS payments,
    round(sum(amount), 2)                       AS total_amount
FROM transactions
WHERE ts > '2026-06-30'::timestamp
  AND iso_msg ? 'CdtrAgt'                       -- only messages carrying a CdtrAgt
GROUP BY 1
ORDER BY total_amount DESC NULLS LAST
LIMIT 15;


-- ─── 1D: Cross-border payments — debtor country ≠ creditor country ──────────
-- ⚡ Two path extractions compared in one pass (data-residency / AML signal)
SELECT
    iso_msg #>> '{Dbtr,CtryOfRes}'  AS debtor_ctry,
    iso_msg #>> '{Cdtr,CtryOfRes}'  AS creditor_ctry,
    count(*)                        AS payments,
    round(sum(amount), 2)           AS total_amount
FROM transactions
WHERE ts > '2026-06-30'::timestamp
  AND (iso_msg #>> '{Dbtr,CtryOfRes}') IS DISTINCT FROM (iso_msg #>> '{Cdtr,CtryOfRes}')
  AND iso_msg ? 'Cdtr'
GROUP BY 1, 2
ORDER BY total_amount DESC
LIMIT 20;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ LAB 1 · PART B — NATIVE int8range BIN / ACCOUNT CONTAINMENT             ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─── 1E: Compromised-BIN watchlist match — native <@ join ───────────────────
-- ⚡ ONE operator (card_bin <@ bin_range). On Databricks this is ip-style range
--    math: a UDF + BETWEEN scan. Here it is a GiST-indexed containment join.
-- "Match every authorisation against the compromised-BIN feeds in one query"
SELECT
    w.feed_name,
    w.category,
    w.confidence,
    count(*)                AS hit_count,
    count(DISTINCT t.account_id) AS accounts,
    round(sum(t.amount), 2) AS total_amount,
    min(t.ts)               AS first_seen,
    max(t.ts)               AS last_seen
FROM transactions t
JOIN fraud_watchlists w
    ON t.card_bin <@ w.bin_range          -- ⚡ THE killer feature
WHERE t.ts > '2026-06-30'::timestamp
  AND w.active
  AND w.confidence >= 80
GROUP BY 1, 2, 3
ORDER BY hit_count DESC;


-- ─── 1F: Identify the owning portfolio by account-range containment ─────────
-- ⚡ account_id <@ account_range maps each transaction to its portfolio
SELECT
    c.customer_name,
    c.segment,
    c.kyc_risk,
    count(*)                AS txns,
    round(sum(t.amount), 2) AS total_amount,
    count(*) FILTER (WHERE t.auth_response <> '00') AS declines
FROM transactions t
JOIN customers c ON t.account_id <@ c.account_range   -- ⚡ portfolio attribution
WHERE t.ts > '2026-06-30'::timestamp
GROUP BY 1, 2, 3
ORDER BY total_amount DESC
LIMIT 15;


-- ─── 1G: Spend anomaly — per-account hourly z-score > 3 ─────────────────────
WITH hourly AS (
    SELECT date_trunc('hour', ts) AS hour, account_id,
           sum(amount) AS total_amount, count(*) AS txn_count
    FROM transactions
    WHERE ts > '2026-06-30'::timestamp
    GROUP BY 1, 2
),
stats AS (
    SELECT account_id, avg(total_amount) AS avg_amt, stddev(total_amount) AS sd_amt
    FROM hourly GROUP BY 1 HAVING stddev(total_amount) > 0
)
SELECT h.hour, h.account_id, h.total_amount, h.txn_count,
       round(s.avg_amt::numeric, 0) AS avg_amt,
       round(((h.total_amount - s.avg_amt) / s.sd_amt)::numeric, 2) AS z_score
FROM hourly h JOIN stats s ON h.account_id = s.account_id
WHERE (h.total_amount - s.avg_amt) / s.sd_amt > 3
ORDER BY z_score DESC
LIMIT 20;


-- ─── 1H: Card-testing detection (breadth + micro-amounts) ───────────────────
-- "Find accounts hitting >50 distinct merchants with sub-$5 authorisations"
SELECT
    account_id,
    count(DISTINCT merchant_id) AS merchants_hit,
    count(DISTINCT mcc)         AS mccs_hit,
    count(*)                    AS auths,
    count(*) FILTER (WHERE auth_response <> '00') AS declines,
    round(avg(amount), 2)       AS avg_amount
FROM transactions
WHERE ts > '2026-06-30'::timestamp
  AND channel = 'ECOM'
  AND amount < 5
GROUP BY account_id
HAVING count(DISTINCT merchant_id) > 50
ORDER BY merchants_hit DESC
LIMIT 20;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ CROSS-SOURCE CORRELATION  (case + auth + device)                        ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─── 2A: For every critical case note, find matching auth + device activity ─
-- 🔗 Single SQL query correlates 3 sources — replaces SIEM correlation rules
SELECT
    n.ts              AS case_time,
    n.account_id,
    n.queue,
    left(n.narrative, 80) AS case_note,
    a.decision        AS auth_decision,
    a.amount          AS auth_amount,
    d.event_type      AS device_event,
    d.result          AS device_result
FROM case_narratives n
JOIN auth_decisions a
    ON n.account_id = a.account_id
    AND a.ts BETWEEN n.ts - interval '5 minutes' AND n.ts + interval '5 minutes'
LEFT JOIN device_events d
    ON n.account_id = d.account_id
    AND d.ts BETWEEN n.ts - interval '10 minutes' AND n.ts + interval '10 minutes'
WHERE n.severity <= 2
  AND n.ts > '2026-06-30'::timestamp
ORDER BY n.ts DESC
LIMIT 30;


-- ─── 2B: Event volume dashboard — all sources ───────────────────────────────
-- 💰 One platform for transactions + cases + auth + device + wires
SELECT 'transactions' AS source, count(*) AS events,
       pg_size_pretty(pg_total_relation_size('transactions')) AS storage
       FROM transactions WHERE ts > '2026-06-30'::timestamp
UNION ALL
SELECT 'case_narratives', count(*), pg_size_pretty(pg_total_relation_size('case_narratives'))
       FROM case_narratives WHERE ts > '2026-06-30'::timestamp
UNION ALL
SELECT 'auth_decisions', count(*), pg_size_pretty(pg_total_relation_size('auth_decisions'))
       FROM auth_decisions WHERE ts > '2026-06-30'::timestamp
UNION ALL
SELECT 'device_events', count(*), pg_size_pretty(pg_total_relation_size('device_events'))
       FROM device_events WHERE ts > '2026-06-30'::timestamp
ORDER BY events DESC;


-- ─── 2C: Risky device action followed by a decline ──────────────────────────
-- "Find accounts adding a payee / new device from a high-risk country, then declined"
SELECT
    d.account_id,
    d.event_type,
    d.ip_country,
    count(DISTINCT d.device_evt_id) AS risky_events,
    count(DISTINCT a.decision_id)   AS declines,
    max(d.ts) AS last_device_event,
    max(a.ts) AS last_decline
FROM device_events d
JOIN auth_decisions a
    ON d.account_id = a.account_id
    AND a.decision IN ('DECLINE', 'BLOCK')
    AND a.ts BETWEEN d.ts - interval '30 minutes' AND d.ts + interval '30 minutes'
WHERE d.event_type IN ('payee_add', 'new_device', 'pwd_reset')
  AND d.ip_country IN ('RU', 'NG', 'PA', 'IR', 'KP')
  AND d.ts > '2026-06-30'::timestamp
GROUP BY 1, 2, 3
ORDER BY risky_events DESC
LIMIT 20;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ BIN INVENTORY & DATA QUALITY  (int8range utilization + overlap)         ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─── 3A: Overlapping BIN ranges — native && operator ────────────────────────
-- ⚡ Catch mis-configured / colliding issuing programs (data-quality control)
SELECT * FROM v_bin_overlaps;

-- ─── 3B: BIN range sizing ───────────────────────────────────────────────────
SELECT issuer_name, scheme, bin_range,
       bin_range_size(bin_range) AS bins_in_range
FROM bin_ranges
ORDER BY bins_in_range DESC
LIMIT 15;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ LIMIT / THRESHOLD MONITORING  (KPIs vs risk profile)                    ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─── 4A: Portfolios breaching velocity / decline thresholds ─────────────────
SELECT customer_name, segment, region_code, limit_status,
       round(avg_velocity, 1)     AS avg_velocity,
       round(avg_decline_rate, 2) AS avg_decline_rate,
       round(avg_fraud_bps, 1)    AS avg_fraud_bps
FROM v_limit_status
WHERE limit_status IN ('BREACH', 'AT RISK')
ORDER BY limit_status, avg_fraud_bps DESC;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ AML / COMPLIANCE & FORENSICS                                            ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─── 5A: Live watchlist matches with country-risk enrichment ────────────────
-- ⚡ Native int8range containment + AML geo enrichment in one pass
SELECT
    t.account_id,
    w.feed_name,
    w.category,
    w.confidence,
    cr.country_name AS merchant_country,
    cr.risk_score   AS country_risk,
    cr.fatf_status,
    count(*)                AS txns,
    round(sum(t.amount), 2) AS total_amount,
    min(t.ts) AS first_seen,
    max(t.ts) AS last_seen
FROM transactions t
JOIN fraud_watchlists w ON t.card_bin <@ w.bin_range AND w.active AND w.confidence >= 80
LEFT JOIN country_risk cr ON t.merchant_country = cr.country_code
WHERE t.ts > '2026-06-30'::timestamp
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY txns DESC
LIMIT 20;


-- ─── 5B: Cross-border flow to high-risk corridors (AML) ─────────────────────
SELECT
    cr.country_name AS beneficiary_country,
    cr.fatf_status,
    cr.risk_score,
    count(DISTINCT t.account_id) AS ordering_accounts,
    count(*)                     AS transfers,
    round(sum(t.amount), 2)      AS total_amount
FROM transactions t
JOIN country_risk cr ON t.beneficiary_country = cr.country_code
WHERE t.txn_type = 'transfer'
  AND t.ts > '2026-06-30'::timestamp
  AND (cr.fatf_status <> 'compliant' OR cr.is_sanctioned)
GROUP BY 1, 2, 3
ORDER BY total_amount DESC;


-- ─── 5C: Sanctions screening — payments to sanctioned beneficiary accounts ──
SELECT
    t.account_id,
    t.beneficiary_account,
    w.feed_name,
    w.category,
    count(*)                AS payments,
    round(sum(t.amount), 2) AS total_amount
FROM transactions t
JOIN fraud_watchlists w
    ON t.beneficiary_account = w.single_account
   AND w.category IN ('sanctioned', 'mule')
   AND w.active
WHERE t.ts > now() - interval '30 days'
GROUP BY 1, 2, 3, 4
ORDER BY total_amount DESC
LIMIT 25;


-- ─── 5D: Forensic trace — everything about one account across all sources ───
-- 🔗 "Give me EVERYTHING about this account across ALL sources"
SELECT * FROM (
    (SELECT 'transaction' AS source, ts,
            txn_type || ' ' || amount::text || ' ' || coalesce(merchant_country,'') AS detail,
            channel AS extra
        FROM transactions WHERE account_id = 105900001 AND ts > now() - interval '28 days'
        ORDER BY ts DESC LIMIT 20)
    UNION ALL
    (SELECT 'auth', ts, decision || ' ' || coalesce(amount::text,''), coalesce(channel,'')
        FROM auth_decisions WHERE account_id = 105900001 AND ts > now() - interval '28 days'
        ORDER BY ts DESC LIMIT 20)
    UNION ALL
    (SELECT 'device', ts, event_type || ' (' || result || ')', coalesce(ip_country,'')
        FROM device_events WHERE account_id = 105900001 AND ts > now() - interval '28 days'
        ORDER BY ts DESC LIMIT 20)
    UNION ALL
    (SELECT 'case', ts, left(narrative, 80), coalesce(queue,'')
        FROM case_narratives WHERE account_id = 105900001 AND ts > now() - interval '28 days'
        ORDER BY ts DESC LIMIT 20)
) forensic
ORDER BY ts DESC
LIMIT 50;


-- ─── 5E: Fraud case summary ─────────────────────────────────────────────────
SELECT fraud_category, severity, status,
       count(*)                AS case_count,
       count(DISTINCT account_id) AS accounts,
       min(ts) AS earliest, max(ts) AS latest
FROM fraud_cases
WHERE ts > now() - interval '30 days'
GROUP BY 1, 2, 3
ORDER BY
    CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
    case_count DESC;


-- ============================================================================
-- ⭐ BONUS: the "wow" query — everything native, one statement
-- ============================================================================
-- "For each region: top fraud category, highest-risk portfolio, hottest BIN band"
WITH watch AS (
    SELECT bin_range, category FROM fraud_watchlists WHERE active AND confidence >= 70 AND bin_range IS NOT NULL
),
fraud_summary AS (
    SELECT r.region_code, w.category, count(*) AS hits,
           row_number() OVER (PARTITION BY r.region_code ORDER BY count(*) DESC) AS rn
    FROM transactions t
    JOIN watch w ON t.card_bin <@ w.bin_range
    JOIN regions r ON t.region_id = r.region_id
    WHERE t.ts > '2026-06-30'::timestamp
    GROUP BY 1, 2
),
risk_portfolios AS (
    SELECT r.region_code, c.customer_name, avg(k.fraud_bps) AS avg_bps,
           row_number() OVER (PARTITION BY r.region_code ORDER BY avg(k.fraud_bps) DESC) AS rn
    FROM account_kpis k
    JOIN customers c ON k.customer_id = c.customer_id
    JOIN regions r ON c.region_id = r.region_id
    WHERE k.ts > '2026-06-30'::timestamp
    GROUP BY 1, 2
),
hot_bins AS (
    SELECT r.region_code, t.card_bin,
           count(*) FILTER (WHERE t.card_bin <@ ANY(SELECT bin_range FROM watch)) AS fraud_txns,
           row_number() OVER (PARTITION BY r.region_code ORDER BY
               count(*) FILTER (WHERE t.card_bin <@ ANY(SELECT bin_range FROM watch)) DESC) AS rn
    FROM transactions t
    JOIN regions r ON t.region_id = r.region_id
    WHERE t.ts > '2026-06-30'::timestamp
    GROUP BY 1, 2
)
SELECT fs.region_code, fs.category AS top_fraud, fs.hits AS fraud_hits,
       rp.customer_name AS highest_risk_portfolio, round(rp.avg_bps, 1) AS their_fraud_bps,
       hb.card_bin AS hottest_bin, hb.fraud_txns
FROM fraud_summary fs
JOIN risk_portfolios rp ON fs.region_code = rp.region_code AND rp.rn = 1
JOIN hot_bins hb ON fs.region_code = hb.region_code AND hb.rn = 1
WHERE fs.rn = 1
ORDER BY fs.hits DESC;

\timing off
