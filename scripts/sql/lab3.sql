-- =============================================================================
-- Lab 3: AI Analytics & The AI Factory
-- Complete SQL script for pgvector + MADlib demonstration
-- =============================================================================
SET search_path TO netvista_demo, public;

-- =============================================================================
-- CLEANUP - Remove any existing tables
-- =============================================================================
DROP TABLE IF EXISTS netvista_demo.kmeans_assignments CASCADE;
DROP TABLE IF EXISTS netvista_demo.netflow_features_norm CASCADE;
DROP TABLE IF EXISTS netvista_demo.netflow_features_agg CASCADE;
DROP TABLE IF EXISTS netvista_demo.netflow_features CASCADE;
DROP TABLE IF EXISTS netvista_demo.syslog_embeddings CASCADE;

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================================
-- STEP 1: pgvector - Semantic Search on Syslog Events
-- =============================================================================
-- Purpose: Build 32-dimensional feature vectors representing syslog characteristics
-- for semantic similarity search (finding threats by meaning, not keywords)

-- Create embeddings table
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
LIMIT 200000;

ANALYZE netvista_demo.syslog_embeddings;

-- Verify embeddings loaded
SELECT persona, COUNT(*) AS event_count
FROM netvista_demo.syslog_embeddings
GROUP BY 1 ORDER BY 2 DESC;

-- =============================================================================
-- STEP 2: MADlib - Behavioral Feature Extraction from Netflow
-- =============================================================================
-- Purpose: Calculate 6 behavioral metrics per IP per hour:
--   • flow_count, unique_ports: activity volume
--   • total_bytes, byte_cv: data transfer patterns
--   • dst_entropy, port_spread: target diversity

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

-- =============================================================================
-- STEP 3: Add Diverse Syslog Samples
-- =============================================================================
-- Purpose: Manually insert 20+ diverse threat logs to demonstrate semantic search
-- variety (RECON, EXFIL, C2 patterns with different phrasings)

INSERT INTO netvista_demo.syslog_embeddings
    (event_id, src_ip, hostname, program, message, severity, persona, embedding)
VALUES
    -- RECON messages with diverse phrasings
    (900001, '10.10.10.15', 'srv-acme-01', 'firewalld', 'Port scan detected: 10.10.10.15 hit 4523 unique ports in 60s', 2, 'recon',
     ARRAY[0.2, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.02, 0.03, 0.01, 0.04, 0.02, 0.01]::vector(32)),

    (900002, '10.10.10.12', 'ids-us-e-02', 'firewalld', 'REJECT TCP from 10.10.10.12:52341 to 10.20.1.50:22 (Connection refused)', 2, 'recon',
     ARRAY[0.2, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.01, 0.02, 0.03, 0.02, 0.01, 0.02]::vector(32)),

    (900003, '10.10.10.18', 'ids-us-e-01', 'snort', 'SCAN SYN FIN detected from 10.10.10.18 — possible nmap', 2, 'recon',
     ARRAY[0.2, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.03, 0.01, 0.02, 0.01, 0.03, 0.02]::vector(32)),

    (900004, '10.10.10.20', 'srv-acme-03', 'kernel', 'TCP: SYN retransmission flood from 10.10.10.20', 1, 'recon',
     ARRAY[0.1, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.02, 0.01, 0.03, 0.02, 0.01, 0.03]::vector(32)),

    (900005, '10.10.10.14', 'srv-acme-02', 'iptables', 'REJECT IN=eth0 SRC=10.10.10.14 DST=10.20.1.100 PROTO=TCP — RST flag', 3, 'recon',
     ARRAY[0.4, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.8, 0.0, 0.0, 0.0, 0.0, 0.5, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.01, 0.02, 0.01, 0.03, 0.02, 0.01]::vector(32)),

    (900006, '10.10.10.22', 'ids-us-e-03', 'firewalld', 'Connection reset by peer: 10.10.10.22 — likely port probe', 2, 'recon',
     ARRAY[0.2, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.5, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.8, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.02, 0.03, 0.02, 0.01, 0.02, 0.03]::vector(32)),

    (900007, '10.10.10.11', 'srv-acme-05', 'sshd', 'Invalid user admin from 10.10.10.11 port 41234', 3, 'recon',
     ARRAY[0.4, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.03, 0.01, 0.02, 0.03, 0.01, 0.02]::vector(32)),

    (900008, '10.10.10.19', 'ids-us-e-02', 'snort', 'ICMP Unreachable from 10.10.10.19: host unreachable (TTL exceeded)', 2, 'recon',
     ARRAY[0.2, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.5, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.01, 0.03, 0.02, 0.01, 0.03, 0.02]::vector(32)),

    -- EXFIL messages with diverse phrasings
    (900010, '10.20.1.205', 'srv-acme-04', 'rsync', 'Large outbound transfer: 10.20.1.205 → 103.224.82.15: 284MB in 145s', 1, 'exfil',
     ARRAY[0.1, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.5, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.02, 0.01, 0.03, 0.02, 0.01, 0.02]::vector(32)),

    (900011, '10.20.1.203', 'srv-acme-06', 'backup-svc', 'Archive exported: /opt/data/customer_db_full.tar.gz (250MB)', 1, 'exfil',
     ARRAY[0.1, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.8, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.5, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.01, 0.02, 0.01, 0.03, 0.02, 0.01]::vector(32)),

    (900012, '10.20.1.207', 'srv-acme-02', 'rclone', 'Data sync to cloud: 3.2GB transferred to remote endpoint 103.224.82.15', 3, 'exfil',
     ARRAY[0.4, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.8, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.03, 0.01, 0.02, 0.01, 0.03, 0.02]::vector(32)),

    (900013, '10.20.1.201', 'srv-acme-08', 'openvpn', 'Encrypted tunnel established: 10.20.1.201 → 103.224.82.15:443 (TLS 1.3)', 0, 'exfil',
     ARRAY[0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.02, 0.03, 0.01, 0.02, 0.01, 0.03]::vector(32)),

    (900014, '10.20.1.209', 'srv-acme-03', 'netfilter', 'ALERT: Sustained high-bandwidth flow 10.20.1.209→103.224.82.15: 450MB over 10min', 1, 'exfil',
     ARRAY[0.1, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.5, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.01, 0.02, 0.03, 0.01, 0.02, 0.01]::vector(32)),

    (900015, '10.20.1.202', 'srv-acme-07', 'backup-svc', 'Backup completed: customer_records_8472.tar encrypted and staged', 3, 'exfil',
     ARRAY[0.4, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.5, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.02, 0.01, 0.02, 0.03, 0.01, 0.02]::vector(32)),

    (900016, '10.20.1.208', 'srv-acme-05', 'curl', 'POST https://103.224.82.15/upload?token=7821 — 180MB payload — 200 OK', 4, 'exfil',
     ARRAY[0.5, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.03, 0.02, 0.01, 0.02, 0.03, 0.01]::vector(32)),

    -- C2 messages with diverse phrasings
    (900020, '192.168.10.52', 'host-jp-12', 'beacon', 'C2 beacon: 192.168.10.52 → 91.219.236.222 jitter=45ms interval=300s', 4, 'c2',
     ARRAY[0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.02, 0.01, 0.03, 0.02, 0.01, 0.02]::vector(32)),

    (900021, '192.168.10.55', 'host-jp-15', 'cron', 'Heartbeat detected: agent check-in from 192.168.10.55 — seq=4821', 6, 'c2',
     ARRAY[0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.01, 0.02, 0.01, 0.03, 0.02, 0.01]::vector(32)),

    (900022, '192.168.10.58', 'host-jp-08', 'svchost', 'Polling remote host 91.219.236.222:443 — last seen 287s ago', 5, 'c2',
     ARRAY[0.7, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.03, 0.01, 0.02, 0.01, 0.03, 0.02]::vector(32)),

    (900023, '192.168.10.61', 'host-jp-11', 'curl', 'Keep-alive received: 192.168.10.61 → C2:8080 — next poll in 120s', 6, 'c2',
     ARRAY[0.8, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.02, 0.03, 0.01, 0.02, 0.01, 0.03]::vector(32)),

    (900024, '192.168.10.64', 'host-jp-14', 'beacon', 'DNS beacon query: 8472.update.svc.internal — TTL 60s', 4, 'c2',
     ARRAY[0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.01, 0.02, 0.03, 0.01, 0.02, 0.01]::vector(32)),

    (900025, '192.168.10.67', 'host-jp-17', 'svchost', 'Polling remote host: GET /ping HTTP/1.1 — 204 No Content', 4, 'c2',
     ARRAY[0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.02, 0.01, 0.02, 0.03, 0.01, 0.02]::vector(32));

ANALYZE netvista_demo.syslog_embeddings;

-- Verify diversity
SELECT
    persona,
    COUNT(DISTINCT LEFT(message, 40)) as unique_message_prefixes,
    COUNT(*) as total_logs
FROM netvista_demo.syslog_embeddings
WHERE persona IN ('recon', 'exfil', 'c2')
GROUP BY persona
ORDER BY persona;

-- Show sample diversity
SELECT
    persona,
    program,
    LEFT(message, 70) as sample_message
FROM netvista_demo.syslog_embeddings
WHERE persona IN ('recon', 'exfil', 'c2')
AND event_id >= 900000
ORDER BY persona, event_id;

-- =============================================================================
-- STEP 4: Aggregate IP Behavior Across All Hours
-- =============================================================================
-- Purpose: CRITICAL FIX - Aggregate all hours per IP BEFORE normalization
-- This ensures clustering sees overall IP behavior, not individual hourly snapshots

CREATE TABLE netvista_demo.netflow_features_agg AS
  SELECT
      src_ip,
      SUM(flow_count)                          AS total_flows,
      AVG(unique_dsts)                         AS avg_unique_dsts,
      AVG(unique_ports)                        AS avg_unique_ports,
      SUM(total_bytes)                         AS total_bytes,
      AVG(dst_entropy)                         AS avg_dst_entropy,
      AVG(port_spread)                         AS avg_port_spread,
      AVG(byte_cv)                             AS avg_byte_cv,
      COUNT(*)                                 AS num_hours  -- how many hours this IP was active
  FROM netvista_demo.netflow_features
  GROUP BY src_ip
  DISTRIBUTED BY (src_ip);

  ANALYZE netvista_demo.netflow_features_agg;

-- =============================================================================
-- STEP 5: Normalize Aggregated Features for K-Means
-- =============================================================================
-- Purpose: Z-score normalization (mean=0, stddev=1) so all features have equal weight

  CREATE TABLE netvista_demo.netflow_features_norm AS
    SELECT
        src_ip,
        ARRAY[
            (total_flows     - (SELECT AVG(total_flows)     FROM netvista_demo.netflow_features_agg)) /
                NULLIF((SELECT STDDEV(total_flows)     FROM netvista_demo.netflow_features_agg), 0),
            (avg_unique_dsts - (SELECT AVG(avg_unique_dsts) FROM netvista_demo.netflow_features_agg)) /
                NULLIF((SELECT STDDEV(avg_unique_dsts) FROM netvista_demo.netflow_features_agg), 0),
            (avg_unique_ports- (SELECT AVG(avg_unique_ports)FROM netvista_demo.netflow_features_agg)) /
                NULLIF((SELECT STDDEV(avg_unique_ports)FROM netvista_demo.netflow_features_agg), 0),
            (total_bytes     - (SELECT AVG(total_bytes)     FROM netvista_demo.netflow_features_agg)) /
                NULLIF((SELECT STDDEV(total_bytes)     FROM netvista_demo.netflow_features_agg), 0),
            (avg_dst_entropy - (SELECT AVG(avg_dst_entropy) FROM netvista_demo.netflow_features_agg)) /
                NULLIF((SELECT STDDEV(avg_dst_entropy) FROM netvista_demo.netflow_features_agg), 0),
            (avg_port_spread - (SELECT AVG(avg_port_spread) FROM netvista_demo.netflow_features_agg)) /
                NULLIF((SELECT STDDEV(avg_port_spread) FROM netvista_demo.netflow_features_agg), 0)
        ]::double precision[] AS features
    FROM netvista_demo.netflow_features_agg
    DISTRIBUTED BY (src_ip);

-- =============================================================================
-- STEP 6: Run MADlib K-Means Clustering (k=5)
-- =============================================================================
-- Purpose: Discover 5 behavioral clusters using kmeanspp algorithm
-- Expected: 1 NORMAL cluster + 3-4 threat personas (RECON, EXFIL, C2)

    CREATE TABLE netvista_demo.kmeans_assignments AS
    WITH model AS (
        SELECT centroids
        FROM madlib.kmeanspp(
            'netvista_demo.netflow_features_norm',  -- source table
            'features',                             -- feature column
            5,                                      -- k clusters
            'madlib.dist_norm2',                    -- distance metric
            'madlib.avg',                           -- centroid aggregate
            100,                                    -- max iterations
            0.001::double precision                 -- convergence threshold
        )
    ),
    -- Unpack the 2-D centroid array into one row per cluster
    centroids AS (
        SELECT
            i - 1 AS cluster_id,
            ARRAY[
                m.centroids[i][1],
                m.centroids[i][2],
                m.centroids[i][3],
                m.centroids[i][4],
                m.centroids[i][5],
                m.centroids[i][6]
            ]::double precision[] AS centroid
        FROM model m, generate_series(1, 5) AS i
    ),
    -- For every (IP, centroid) pair compute distance; keep closest
    ranked AS (
        SELECT
            n.src_ip,
            c.cluster_id,
            ROW_NUMBER() OVER (
                PARTITION BY n.src_ip
                ORDER BY madlib.dist_norm2(n.features, c.centroid)
            ) AS rn
        FROM netvista_demo.netflow_features_norm n
        CROSS JOIN centroids c
    )
    SELECT src_ip, cluster_id
    FROM   ranked
    WHERE  rn = 1
    DISTRIBUTED BY (src_ip);

    ANALYZE netvista_demo.kmeans_assignments;

-- =============================================================================
-- STEP 7: Validation Queries
-- =============================================================================

-- 7.1 Cluster Distribution - Verify 5 clusters with reasonable split
SELECT
    cluster_id,
    COUNT(*)                          AS member_count,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM netvista_demo.kmeans_assignments
GROUP BY 1
ORDER BY 1;

-- 7.2 Cluster Profiling - Infer persona from behavioral metrics
SELECT
    a.cluster_id,
    COUNT(*) AS member_count,
    ROUND(AVG(f.total_flows), 1) AS avg_total_flows,
    ROUND(AVG(f.total_bytes)::numeric / 1e6, 2) AS avg_total_bytes_mb,
    ROUND(AVG(f.avg_unique_ports), 1) AS avg_ports,
    ROUND(AVG(f.avg_dst_entropy)::numeric, 4) AS avg_entropy,
    ROUND(AVG(f.avg_byte_cv)::numeric, 4) AS avg_byte_cv,
    CASE
    -- RECON: extreme ports (not just > 100, but > 1000)
    WHEN AVG(f.avg_unique_ports) > 1000 THEN 'RECON (High Ports)'
    
    -- EXFIL: MASSIVE bytes (not 100 MB, but > 10 GB = 10,000 MB)
    WHEN AVG(f.total_bytes) > 10000000000 THEN 'EXFIL (High Bytes)'  -- 10 GB threshold
    
    -- C2: low variance + low entropy (more specific)
    WHEN AVG(f.avg_byte_cv) < 0.4 AND AVG(f.avg_dst_entropy) < 0.5 THEN 'C2 (Beaconing)'
    
    -- NORMAL: everything else
    ELSE 'NORMAL (Baseline)'
END AS inferred_persona
FROM netvista_demo.kmeans_assignments a
JOIN netvista_demo.netflow_features_agg f ON a.src_ip = f.src_ip
GROUP BY 1
ORDER BY member_count DESC;

-- =============================================================================
-- DONE - Lab 3 SQL Setup Complete
-- =============================================================================
-- Next: Run app3.py dashboard to visualize pgvector + MADlib results
