-- ═══════════════════════════════════════════════════════════════════════════════
-- Lab 3: Hybrid Forensic Discovery — pgvector + MADlib on Persona-Based Data
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- SCENARIO
-- ─────────────────────────────────────────────────────────────────────────────
-- The dataset was generated with four behavioral personas embedded in it:
--   • Normal        (~70% of traffic) — baseline business flows
--   • Recon         (~12%) — port-scan: high unique_ports, tiny bytes
--   • Exfiltration  ( ~8%) — few unique_dsts, MASSIVE bytes
--   • C2 Beaconing  (~10%) — periodic tiny flows, very low byte variance
--
-- INVESTIGATION WORKFLOW
-- ─────────────────────────────────────────────────────────────────────────────
-- Step A  BEHAVIORAL FLAGGING  — MADlib K-Means groups IPs by (bytes, ports,
--         entropy). Students identify the Exfiltration cluster (high bytes,
--         low dst-entropy).
--
-- Step B  SEMANTIC DEEP DIVE   — Pick an anomalous IP → pgvector finds syslogs
--         about "unusual data movement" using cosine similarity.
--
-- Step C  THE AHA! MOMENT      — LIKE '%exfil%' finds nothing.
--         Vector search finds "Data sync to cloud", "Archive exported",
--         "Backup completed" — intent-based retrieval, not keyword matching.
--
-- Run order:
--   01_schema.sql  →  02_seed_reference.sql  →  03_load_external.sql
--   →  06_lab3_ai_analytics.sql  →  07_kmeans_fallback.sql
-- ═══════════════════════════════════════════════════════════════════════════════

SET search_path TO netvista_demo, public;

-- ═══════════════════════════════════════════════════════════════════════════════
-- CLEANUP — idempotent re-run
-- ═══════════════════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS netvista_demo.kmeans_assignments;
DROP TABLE IF EXISTS netvista_demo.netflow_features_norm;
DROP TABLE IF EXISTS netvista_demo.netflow_features;
DROP TABLE IF EXISTS netvista_demo.syslog_embeddings;
DROP INDEX  IF EXISTS netvista_demo.idx_syslog_embedding_hnsw;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PART A: pgvector — Semantic Search on Syslog Events
-- ═══════════════════════════════════════════════════════════════════════════════
-- Goal: Store semantic embeddings for syslog messages so an analyst can search
-- for "unusual data movement" and find Exfil logs even without knowing exact
-- keywords like "rclone" or "SFTP upload".
--
-- In production: use sentence-transformers (all-MiniLM-L6-v2) to generate real
-- 384-dim embeddings via Python, then INSERT into this table.
-- For the workshop: 32-dim feature vectors derived from message characteristics
-- — enough to demonstrate the cosine similarity concept clearly.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE netvista_demo.syslog_embeddings (
    event_id     BIGINT,
    src_ip       INET,
    hostname     TEXT,
    program      TEXT,
    message      TEXT,
    severity     INT,
    persona      TEXT,          -- 'normal' | 'recon' | 'exfil' | 'c2'  (for lab verification)
    embedding    vector(32)     -- 32-dim feature vector
) DISTRIBUTED BY (event_id);

-- ─── Build persona-aware feature vectors ────────────────────────────────────
-- Dimension assignments:
--  [0]  normalized severity
--  [1]  is sshd
--  [2]  is firewalld / iptables / snort  (network security tools)
--  [3]  is kernel
--  [4]  is haproxy / kubelet / systemd
--  [5]  is backup-svc / rsync / rclone / openvpn  (data movement tools)
--  [6]  is cron / beacon / svchost  (scheduling / C2 beaconing tools)
--  [7]  is audit
--  [8]  message: scan / probe / flood keywords       → Recon signal
--  [9]  message: port scan / nmap / RST              → Recon signal
--  [10] message: large outbound / transfer / export  → Exfil signal
--  [11] message: encrypted tunnel / sync / archive   → Exfil signal
--  [12] message: heartbeat / keep-alive / beacon     → C2 signal
--  [13] message: polling / check-in / watchdog       → C2 signal
--  [14] message: connection refused / ICMP           → Recon signal
--  [15] message: credential / passwd / harvest       → Recon/Exfil
--  [16] message: SYN / RST / flood                   → Recon signal
--  [17] message: backup / export / tar / zip         → Exfil signal
--  [18] message: upload / POST / payload             → Exfil signal
--  [19] message: interval / seq= / jitter            → C2 signal
--  [20] severity <= 2  (emerg / alert)
--  [21] severity == 3  (error)
--  [22] severity == 4  (warning)
--  [23] hostname like 'ids-'  (IDS sensor)
--  [24] hostname like 'srv-'  (server)
--  [25] hostname like 'host-' (endpoint)
--  [26-31] reserved / noise padding

INSERT INTO netvista_demo.syslog_embeddings 
    (event_id, src_ip, hostname, program, message, severity, persona, embedding)
SELECT 
    event_id, src_ip, hostname_alias, program_alias, message, severity, persona, embedding
FROM (
    SELECT 
        event_id, 
        src_ip, 
        COALESCE(hostname, 'unknown') AS hostname_alias, 
        COALESCE(program, 'unknown') AS program_alias, 
        LEFT(message, 300) AS message, 
        severity,
        CASE 
            WHEN program IN ('rsync','rclone','backup-svc','openvpn','curl','audit','netfilter') 
              OR message ILIKE '%outbound%' OR message ILIKE '%Archive%' OR message ILIKE '%sync%' 
              OR message ILIKE '%backup%' OR message ILIKE '%upload%' 
            THEN 'exfil'
            WHEN program IN ('beacon','svchost','cron') 
              OR message ILIKE '%heartbeat%' OR message ILIKE '%keep-alive%' 
              OR message ILIKE '%polling%' OR message ILIKE '%C2%' 
              OR message ILIKE '%beacon%'
            THEN 'c2'
            WHEN program IN ('snort','firewalld','iptables') 
              OR message ILIKE '%scan%' OR message ILIKE '%probe%' 
              OR message ILIKE '%flood%' OR message ILIKE '%nmap%'
            THEN 'recon'
            ELSE 'normal'
        END AS persona,
        ARRAY[
            severity::float / 7.0,
            CASE WHEN program = 'sshd' THEN 1.0 ELSE 0.0 END,
            CASE WHEN program IN ('firewalld','iptables','snort') THEN 1.0 ELSE 0.0 END,
            CASE WHEN program = 'kernel' THEN 1.0 ELSE 0.0 END,
            CASE WHEN program IN ('haproxy','kubelet','systemd','ntpd') THEN 1.0 ELSE 0.0 END,
            CASE WHEN program IN ('rsync','rclone','backup-svc','openvpn','curl','sftp') THEN 1.0 ELSE 0.0 END,
            CASE WHEN program IN ('cron','beacon','svchost') THEN 1.0 ELSE 0.0 END,
            CASE WHEN program = 'audit' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%scan%' OR message ILIKE '%probe%' OR message ILIKE '%flood%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%nmap%' OR message ILIKE '%port scan%' OR message ILIKE '%RST flag%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%outbound transfer%' OR message ILIKE '%MB in%' OR message ILIKE '%export%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%encrypted tunnel%' OR message ILIKE '%sync to cloud%' OR message ILIKE '%Archive%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%heartbeat%' OR message ILIKE '%keep-alive%' OR message ILIKE '%beacon%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%polling%' OR message ILIKE '%check-in%' OR message ILIKE '%watchdog%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%Connection refused%' OR message ILIKE '%ICMP%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%passwd%' OR message ILIKE '%credential%' OR message ILIKE '%harvest%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%SYN%' OR message ILIKE '%RST%' OR message ILIKE '%flood%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%backup%' OR message ILIKE '%tar.gz%' OR message ILIKE '%.zip%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%upload%' OR message ILIKE '%POST%' OR message ILIKE '%payload%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN message ILIKE '%interval%' OR message ILIKE '%seq=%' OR message ILIKE '%jitter%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN severity <= 2 THEN 1.0 ELSE 0.0 END,
            CASE WHEN severity = 3 THEN 1.0 ELSE 0.0 END,
            CASE WHEN severity = 4 THEN 1.0 ELSE 0.0 END,
            CASE WHEN hostname LIKE 'ids-%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN hostname LIKE 'srv-%' THEN 1.0 ELSE 0.0 END,
            CASE WHEN hostname LIKE 'host-%' THEN 1.0 ELSE 0.0 END,
            random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05
        ]::vector(32) AS embedding
    FROM netvista_demo.syslog_events
    WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
) sub
WHERE persona != 'normal' OR (persona = 'normal' AND event_id % 10 = 0)
LIMIT 200000;  -- 200K events for the workshop

-- HNSW index for fast ANN search (uncomment if pgvector >= 0.5 installed)
-- CREATE INDEX idx_syslog_embedding_hnsw
-- ON netvista_demo.syslog_embeddings
-- USING hnsw (embedding vector_cosine_ops)
-- WITH (m = 16, ef_construction = 64);

ANALYZE netvista_demo.syslog_embeddings;

DO $$ BEGIN
    RAISE NOTICE 'syslog_embeddings built — % rows', (SELECT COUNT(*) FROM netvista_demo.syslog_embeddings);
    RAISE NOTICE 'Persona breakdown:';
END $$;

SELECT persona, COUNT(*) AS event_count
FROM netvista_demo.syslog_embeddings
GROUP BY 1 ORDER BY 2 DESC;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PART B: MADlib — Netflow Feature Engineering & Behavioral Clustering
-- ═══════════════════════════════════════════════════════════════════════════════

-- Step 1: Build per-IP hourly behavioral profiles
-- These SIX dimensions are what K-Means will cluster on (designed for persona separation):
--   flow_count   : activity volume          → Recon HIGH, Exfil LOW
--   unique_ports : breadth of port access   → Recon EXTREME (100-500), Normal (5-20)
--   total_bytes  : transfer volume          → Exfil EXTREME (100M+), Recon TINY
--   dst_entropy  : destination diversity    → Recon HIGH (0.8+), Exfil LOW (0.05)
--   port_spread  : port diversity ratio     → Recon HIGH, Exfil/C2 LOW
--   byte_cv      : coefficient of variation → C2 VERY LOW (<0.3), Normal MODERATE

CREATE TABLE netvista_demo.netflow_features AS
SELECT
    date_trunc('hour', ts) AS hour,
    src_ip,
    COUNT(*)                                        AS flow_count,
    COUNT(DISTINCT dst_ip)                          AS unique_dsts,
    COUNT(DISTINCT dst_port)                        AS unique_ports,
    SUM(bytes)                                      AS total_bytes,
    AVG(bytes)                                      AS avg_bytes,
    STDDEV_SAMP(bytes)                              AS stddev_bytes,
    MAX(bytes)                                      AS max_bytes,
    SUM(packets)                                    AS total_packets,
    -- dst_entropy: high = many distinct destinations (Recon)
    --              low  = very few destinations (Exfil, C2)
    ROUND(COUNT(DISTINCT dst_ip)::numeric
          / NULLIF(COUNT(*), 0), 4)                 AS dst_entropy,
    -- port_spread: high = many distinct ports (Recon fingerprint)
    ROUND(COUNT(DISTINCT dst_port)::numeric
          / NULLIF(COUNT(*), 0), 4)                 AS port_spread,
    -- byte_cv: coefficient of variation of bytes
    --   C2 beaconing → very LOW (constant payload < 0.3)
    --   Normal        → moderate (0.5-2.0)
    --   Exfil/Recon   → high variance
    ROUND(STDDEV_SAMP(bytes) / NULLIF(AVG(bytes), 0), 4) AS byte_cv
FROM netvista_demo.netflow_logs
WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
GROUP BY 1, 2
HAVING COUNT(*) >= 5
DISTRIBUTED BY (src_ip);

ANALYZE netvista_demo.netflow_features;

DO $$ BEGIN
    RAISE NOTICE 'netflow_features built — % IP-hour profiles',
        (SELECT COUNT(*) FROM netvista_demo.netflow_features);
END $$;

-- Step 2: Baseline stats — students see what "normal" looks like
-- Run this in the lab to understand the feature distribution before clustering.
--
-- SELECT
--     COUNT(*)                                    AS total_profiles,
--     ROUND(AVG(flow_count),    1)                AS avg_flows,
--     ROUND(AVG(unique_dsts),   1)                AS avg_dsts,
--     ROUND(AVG(unique_ports),  1)                AS avg_ports,
--     ROUND(AVG(total_bytes)::numeric / 1e6, 2)  AS avg_bytes_mb,
--     ROUND(AVG(dst_entropy)::numeric, 4)         AS avg_dst_entropy,
--     ROUND(AVG(port_spread)::numeric, 4)         AS avg_port_spread,
--     ROUND(AVG(byte_cv)::numeric, 4)             AS avg_byte_cv
-- FROM netvista_demo.netflow_features;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PART C: AI Factory — "Hybrid Forensic Discovery"
-- ═══════════════════════════════════════════════════════════════════════════════
-- This is the MAIN TEACHING QUERY for Lab 3.
-- It wires together all three steps of the investigation:
--
--   Step A  K-Means clusters → find Exfiltration cluster (high bytes, low entropy)
--   Step B  Pick anomalous IPs from that cluster
--   Step C  Use pgvector cosine similarity to find related syslogs
--           searching semantically for "unusual data movement"

-- ── C1: Cluster-Guided Vector Search  ────────────────────────────────────────
-- The analyst asks: "What logs match the INTENT of 'unusual data movement'?"
-- This uses the EXFIL cluster (cluster_id = 2 from kmeans_fallback) as input.

WITH
-- Step A: Get anomalous IPs from the Exfil cluster (or fallback: high bytes + low entropy)
exfil_candidates AS (
    SELECT
        f.src_ip,
        SUM(f.total_bytes)              AS total_bytes,
        ROUND(AVG(f.dst_entropy)::numeric, 4)   AS avg_dst_entropy,
        ROUND(AVG(f.unique_ports)::numeric, 1)  AS avg_unique_ports,
        a.cluster_id
    FROM netvista_demo.netflow_features f
    -- Join to K-Means assignments (from 07_kmeans_fallback.sql)
    JOIN netvista_demo.kmeans_assignments a ON a.src_ip = f.src_ip
    -- Target the cluster that has LOW entropy + HIGH bytes (= Exfil pattern)
    -- In the SQL fallback this is typically cluster_id 2 or 3
    WHERE a.cluster_id >= 2
    GROUP BY f.src_ip, a.cluster_id
    -- Extra guard: minimum 100MB total transfers
    HAVING SUM(f.total_bytes) > 100_000_000
    ORDER BY total_bytes DESC
    LIMIT 15
),

-- Step B: Build a "query vector" representing the concept of data exfiltration.
-- This is what the analyst would search for: "unusual data movement, outbound transfer"
-- We manually encode its feature profile into the same 32-dim space.
query_vector AS (
    SELECT ARRAY[
        0.43,   -- severity ~3 normalized
        0.0,    -- not sshd
        0.0,    -- not firewall tool
        0.0,    -- not kernel
        0.0,    -- not infra tool
        1.0,    -- IS a data movement tool (rsync/rclone/backup)
        0.0,    -- not C2 tool
        0.0,    -- not audit
        0.0, 0.0,  -- not recon signals
        1.0,    -- outbound transfer / export signal
        1.0,    -- encrypted tunnel / archive signal
        0.0, 0.0,  -- not C2 signals
        0.0,    -- not ICMP
        0.0,    -- not credential
        0.0,    -- not TCP abuse
        1.0,    -- backup / tar / zip keyword
        1.0,    -- upload / POST keyword
        0.0,    -- not interval (C2)
        0.0,    -- not emerg/alert
        1.0,    -- IS error-level
        0.0,    -- not warning
        0.0,    -- not IDS host
        1.0,    -- IS server host
        0.0,    -- not endpoint
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0  -- padding
    ]::vector(32) AS vec
),

-- Step C: Vector similarity search — "Show me logs LIKE this threat profile"
similar_syslogs AS (
    SELECT
        se.event_id,
        se.hostname,
        se.program,
        LEFT(se.message, 120)           AS message,
        se.severity,
        se.persona,                      -- reveal the ground truth
        ROUND((1 - (se.embedding <=> qv.vec))::numeric, 4) AS similarity
    FROM netvista_demo.syslog_embeddings se
    CROSS JOIN query_vector qv
    ORDER BY se.embedding <=> qv.vec
    LIMIT 50
)

SELECT
    ec.src_ip::text         AS flagged_ip,
    ec.cluster_id,
    ROUND(ec.total_bytes::numeric / 1e9, 2)    AS total_bytes_gb,
    ec.avg_dst_entropy,
    sl.hostname,
    sl.program,
    sl.message              AS semantic_match,
    sl.similarity,
    sl.persona              AS ground_truth_persona   -- confirms we found Exfil logs
FROM exfil_candidates ec
CROSS JOIN similar_syslogs sl
WHERE sl.similarity > 0.70
ORDER BY ec.total_bytes DESC, sl.similarity DESC
LIMIT 40;


-- ── C2: The "Aha!" Contrast — LIKE vs Vector Search ────────────────────────
-- LIKE search: the naive approach an analyst might try first.
-- This deliberately FAILS to find Exfil logs because they don't say "exfil".
--
-- SELECT LEFT(message, 120) AS message, program, severity
-- FROM netvista_demo.syslog_embeddings
-- WHERE message ILIKE '%exfiltration%'
--    OR message ILIKE '%data theft%'
-- LIMIT 20;
--
-- → Result: 0 rows (or near 0).
-- The logs say "Data sync to cloud", "Archive exported", "Backup completed"
-- — none contain the word "exfiltration".

-- ── C3: Z-Score Anomaly Confirmation ────────────────────────────────────────
-- After vector search identifies the threat, confirm with statistical flagging.
--
-- WITH stats AS (
--     SELECT
--         AVG(unique_ports) AS mu_p, STDDEV_SAMP(unique_ports) AS sd_p,
--         AVG(total_bytes)  AS mu_b, STDDEV_SAMP(total_bytes)  AS sd_b,
--         AVG(dst_entropy)  AS mu_e, STDDEV_SAMP(dst_entropy)  AS sd_e,
--         AVG(byte_cv)      AS mu_cv,STDDEV_SAMP(byte_cv)      AS sd_cv
--     FROM netvista_demo.netflow_features
-- ),
-- scored AS (
--     SELECT f.src_ip::text,
--         ROUND(ABS(f.unique_ports - s.mu_p) / NULLIF(s.sd_p, 0), 2) AS z_ports,
--         ROUND(ABS(f.total_bytes  - s.mu_b) / NULLIF(s.sd_b, 0), 2) AS z_bytes,
--         ROUND(ABS(f.dst_entropy  - s.mu_e) / NULLIF(s.sd_e, 0), 2) AS z_entropy,
--         ROUND(ABS(f.byte_cv      - s.mu_cv)/ NULLIF(s.sd_cv,0), 2) AS z_byte_cv
--     FROM netvista_demo.netflow_features f, stats s
-- )
-- SELECT src_ip,
--     CASE WHEN z_ports   > 5 THEN 'RECON'
--          WHEN z_bytes   > 5 THEN 'EXFIL'
--          WHEN z_byte_cv < 0.5 AND z_bytes > 2 THEN 'C2'
--          ELSE 'SUSPECT'
--     END AS inferred_persona,
--     z_ports, z_bytes, z_entropy, z_byte_cv
-- FROM scored
-- WHERE z_ports > 3 OR z_bytes > 3 OR z_byte_cv < 0.5
-- ORDER BY z_bytes DESC LIMIT 20;


-- ═══════════════════════════════════════════════════════════════════════════════
-- CLEANUP (optional)
-- ═══════════════════════════════════════════════════════════════════════════════
-- DROP TABLE IF EXISTS netvista_demo.syslog_embeddings;
-- DROP TABLE IF EXISTS netvista_demo.netflow_features;
-- DROP TABLE IF EXISTS netvista_demo.netflow_features_norm;
-- DROP TABLE IF EXISTS netvista_demo.kmeans_assignments;
