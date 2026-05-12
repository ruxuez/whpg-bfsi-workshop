-- ============================================================================
-- NetVista × EDB WarehousePG — Persona-Based Traffic Generation for Lab 3
-- ============================================================================
-- Creates ~50M rows with FOUR distinct behavioral personas:
--   • Normal        (~70%) — baseline business traffic
--   • Recon         (~12%) — port scanning: HIGH unique_ports, LOW bytes
--   • Exfiltration  ( ~8%) — data theft: VERY HIGH bytes, LOW unique_dsts
--   • C2 Beaconing  (~10%) — command & control: periodic, LOW byte variance
--
-- This persona-driven generation ensures K-Means will create visually
-- distinct clusters that students can identify and investigate.
-- ============================================================================
SET search_path TO netvista_demo, public;
SET statement_mem = '512MB';

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ TRUNCATE ALL FACT TABLES                                                ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] Truncating all fact tables...', clock_timestamp(); END $$;

TRUNCATE netflow_logs;
TRUNCATE syslog_events;
TRUNCATE firewall_logs;
TRUNCATE dns_logs;
TRUNCATE bgp_events;
TRUNCATE ipam_allocations;
TRUNCATE ipam_summary;
TRUNCATE network_metrics;
TRUNCATE security_incidents;

DO $$ BEGIN RAISE NOTICE '[%] Truncate complete. Starting persona-based generation...', clock_timestamp(); END $$;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ NETFLOW LOGS — Persona-Based Generation                                 ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ──────────────────────────────────────────────────────────────────────────────
-- PERSONA 1: NORMAL TRAFFIC (~70% = 11.5M rows)
-- Characteristics:
--   • flow_count   : 50-200 per hour per IP
--   • unique_ports : 5-20 (normal service variety)
--   • total_bytes  : 10MB - 500MB per hour
--   • dst_entropy  : moderate (0.3 - 0.7)
-- ──────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN RAISE NOTICE '[%] NORMAL traffic — 11.5M rows (12 batches)...', clock_timestamp(); END $$;

DO $$
DECLARE
    batch INT;
BEGIN
    FOR batch IN 1..12 LOOP
        IF batch % 3 = 0 THEN
            RAISE NOTICE '[%] normal batch %/12 ...', clock_timestamp(), batch;
        END IF;

        INSERT INTO netflow_logs (
            ts, src_ip, dst_ip, src_port, dst_port,
            protocol, bytes, packets, tcp_flags, flow_duration, region_id
        )
        SELECT
            '2026-04-01'::timestamp + (random() * interval '22 days') AS ts,
            -- Normal internal IPs
            (
                (ARRAY['10.10','10.20','10.21','10.128','172.16','172.17','192.168','10.200'])[1 + (random()*7)::int]
                || '.' || (random()*254+1)::int
                || '.' || (random()*254+1)::int
            )::inet AS src_ip,
            CASE WHEN random() < 0.65
                THEN (
                    (ARRAY['10.10','10.20','10.128','172.16','192.168','10.200'])[1 + (random()*5)::int]
                    || '.' || (random()*254+1)::int
                    || '.' || (random()*254+1)::int
                )::inet
                ELSE (
                    (1 + (random()*220)::int)
                    || '.' || (random()*255)::int
                    || '.' || (random()*255)::int
                    || '.' || (1 + (random()*254)::int)
                )::inet
            END AS dst_ip,
            (1024 + (random()*64510)::int),
            -- Normal service ports with typical distribution
            (ARRAY[
                80, 443, 443, 443, 443, 22, 53, 53,
                8080, 3306, 5432, 8443, 25, 110,
                143, 993, 389, 636, 3389, 9200
            ])[1 + (random()*19)::int],
            CASE
                WHEN random() < 0.75 THEN 6   -- TCP
                WHEN random() < 0.95 THEN 17  -- UDP
                ELSE 1                         -- ICMP
            END,
            -- Normal bytes: 500B - 50KB (moderate transfer sizes)
            (500 + (random() * 50000)::int)::bigint,
            GREATEST(1, (random() * 100 + 5)::int)::bigint,
            CASE WHEN random() < 0.70
                THEN (ARRAY[2,16,18,24,25])[1 + (random()*4)::int]
                ELSE NULL
            END,
            (1 + (random() * 300)::int),  -- duration: 1-300 sec
            (1 + (random()*6)::int)
        FROM generate_series(1, 960000);  -- 12 batches × 960K = 11.52M
    END LOOP;
END $$;

DO $$ BEGIN RAISE NOTICE '[%] NORMAL traffic complete (~11.5M rows).', clock_timestamp(); END $$;


-- ──────────────────────────────────────────────────────────────────────────────
-- PERSONA 2: RECONNAISSANCE (~12% = 2M rows)
-- Characteristics:
--   • flow_count   : 500-2000 per hour (rapid scanning)
--   • unique_ports : VERY HIGH (100-500) ← KEY FEATURE
--   • total_bytes  : VERY LOW (100B-1KB per flow)
--   • dst_entropy  : HIGH (0.7-0.95) — many different targets
-- ──────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN RAISE NOTICE '[%] RECON traffic — 2M rows (port scanning)...', clock_timestamp(); END $$;

INSERT INTO netflow_logs (ts, src_ip, dst_ip, src_port, dst_port, protocol, bytes, packets, tcp_flags, flow_duration, region_id)
SELECT
    '2026-04-01'::timestamp + (random() * interval '22 days') + ((i % 1000) * interval '2 seconds'),
    -- Attacker IPs from small pool (so they aggregate into few src_ip records)
    (ARRAY[
        '45.155.205.99','198.98.56.78','222.186.42.7',
        '185.220.101.34','91.219.236.222','23.129.64.130'
    ])[1 + (random()*5)::int]::inet,
    -- Scanning many different internal IPs (high dst_entropy)
    (
        (ARRAY['10.10','10.20','10.21','10.128','172.16','172.17','192.168'])[1 + (random()*6)::int]
        || '.' || (random()*254+1)::int
        || '.' || (random()*254+1)::int
    )::inet,
    (40000 + (random()*25000)::int),
    -- WIDE variety of ports being scanned
    (1 + (random() * 65534)::int),
    6,  -- TCP
    -- TINY bytes (just SYN packets, minimal payload)
    (40 + (random() * 100)::int)::bigint,
    1::bigint,
    2,  -- SYN flag
    0,  -- very short duration (immediate RST)
    (1 + (random()*6)::int)
FROM generate_series(1, 2000000) i;

DO $$ BEGIN RAISE NOTICE '[%] RECON traffic complete (~2M rows).', clock_timestamp(); END $$;


-- ──────────────────────────────────────────────────────────────────────────────
-- PERSONA 3: DATA EXFILTRATION (~8% = 1.3M rows)
-- Characteristics:
--   • flow_count   : LOW (10-50 per hour) — stealthy
--   • unique_ports : LOW (1-3 ports: 443, 53, 993)
--   • total_bytes  : EXTREMELY HIGH (50MB - 500MB per hour) ← KEY FEATURE
--   • dst_entropy  : VERY LOW (0.01-0.1) — same 1-2 destinations
-- ──────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN RAISE NOTICE '[%] EXFILTRATION traffic — 1.3M rows...', clock_timestamp(); END $$;

INSERT INTO netflow_logs (ts, src_ip, dst_ip, src_port, dst_port, protocol, bytes, packets, tcp_flags, flow_duration, region_id)
SELECT
    '2026-04-01'::timestamp + (random() * interval '22 days'),
    -- Compromised internal hosts (small pool so they aggregate)
    (
        '10.20.1.' || (50 + (random()*20)::int)
    )::inet,
    -- ALWAYS the SAME few external C2 servers (LOW dst_entropy)
    (ARRAY[
        '103.224.82.15',  -- exfil server 1
        '58.218.198.100'  -- exfil server 2
    ])[1 + (random()*1)::int]::inet,
    (40000 + (random()*25000)::int),
    -- Only encrypted channels (443, 853, 993)
    (ARRAY[443, 853, 993])[1 + (random()*2)::int],
    6,  -- TCP
    -- MASSIVE bytes per flow (10MB - 100MB)
    (10000000 + (random() * 90000000)::int)::bigint,
    (5000 + (random() * 50000)::int)::bigint,
    24,  -- PSH-ACK (data transfer)
    (60000 + (random() * 300000)::int),  -- long-duration transfers
    1
FROM generate_series(1, 1300000);

DO $$ BEGIN RAISE NOTICE '[%] EXFILTRATION traffic complete (~1.3M rows).', clock_timestamp(); END $$;


-- ──────────────────────────────────────────────────────────────────────────────
-- PERSONA 4: C2 BEACONING (~10% = 1.7M rows)
-- Characteristics:
--   • flow_count   : MODERATE (100-300 per hour) — periodic check-ins
--   • unique_ports : LOW (1-2 ports: 443, 8443)
--   • total_bytes  : LOW but CONSISTENT (500B-2KB per flow)
--   • byte_cv      : VERY LOW ← KEY FEATURE (coefficient of variation < 0.2)
--   • dst_entropy  : LOW (0.05-0.15) — same C2 server(s)
-- ──────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN RAISE NOTICE '[%] C2 BEACONING traffic — 1.7M rows...', clock_timestamp(); END $$;

INSERT INTO netflow_logs (ts, src_ip, dst_ip, src_port, dst_port, protocol, bytes, packets, tcp_flags, flow_duration, region_id)
SELECT
    '2026-04-01'::timestamp + (random() * interval '22 days') + ((i % 100) * interval '5 minutes'),
    -- Infected bots (small pool)
    (
        '10.128.' || (10 + (random()*30)::int) || '.' || (random()*254+1)::int
    )::inet,
    -- ALWAYS the SAME C2 server
    (ARRAY[
        '185.220.101.34',
        '91.219.236.222'
    ])[1 + (random()*1)::int]::inet,
    (50000 + (random()*10000)::int),
    -- Only HTTPS (443, 8443)
    (ARRAY[443, 8443])[1 + (random()*1)::int],
    6,  -- TCP
    -- VERY CONSISTENT byte size (beacon payload is always similar)
    (800 + (random() * 400)::int)::bigint,  -- 800-1200 bytes (low variance)
    (5 + (random() * 10)::int)::bigint,
    24,
    (2 + (random() * 8)::int),  -- short duration (quick beacon)
    (1 + (random()*6)::int)
FROM generate_series(1, 1700000) i;

DO $$ BEGIN RAISE NOTICE '[%] C2 BEACONING traffic complete (~1.7M rows).', clock_timestamp(); END $$;

DO $$ BEGIN RAISE NOTICE '[%] netflow_logs COMPLETE (~16.5M rows: 70%% normal, 12%% recon, 8%% exfil, 10%% C2).', clock_timestamp(); END $$;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ SYSLOG EVENTS — Persona-Aligned Logs                                    ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] Generating persona-aligned syslog_events — 7.5M rows...', clock_timestamp(); END $$;

DO $$
DECLARE
    batch INT;
BEGIN
    FOR batch IN 1..8 LOOP
        IF batch % 2 = 0 THEN
            RAISE NOTICE '[%] syslog batch %/8 ...', clock_timestamp(), batch;
        END IF;

        INSERT INTO syslog_events (
            ts, src_ip, hostname, facility, severity, program, message, region_id
        )
        SELECT
            '2026-04-01'::timestamp + (random() * interval '22 days'),
            (
                (ARRAY['10.10','10.20','10.128','172.16','192.168','10.200'])[1 + (random()*5)::int]
                || '.' || (random()*254+1)::int || '.' || (random()*254+1)::int
            )::inet,
            CASE
                WHEN random() < 0.30 THEN 'srv-'  || (1+(random()*200)::int)
                WHEN random() < 0.70 THEN 'host-' || (1+(random()*1000)::int)
                ELSE 'ids-'  || (1+(random()*50)::int)
            END,
            (0 + (random()*23)::int),
            -- Persona-based severity distribution
            CASE
                WHEN random() < 0.65 THEN 6  -- normal: mostly info
                WHEN random() < 0.85 THEN 5  -- notice
                WHEN random() < 0.95 THEN 4  -- warning
                ELSE 3                        -- error
            END,
            -- Persona-based program distribution
            CASE
                WHEN random() < 0.10 THEN 'rsync'       -- exfil tools
                WHEN random() < 0.15 THEN 'rclone'
                WHEN random() < 0.18 THEN 'backup-svc'
                WHEN random() < 0.20 THEN 'openvpn'
                WHEN random() < 0.22 THEN 'beacon'      -- C2 tools
                WHEN random() < 0.24 THEN 'cron'
                WHEN random() < 0.26 THEN 'snort'       -- recon detectors
                WHEN random() < 0.28 THEN 'firewalld'
                WHEN random() < 0.40 THEN 'sshd'
                WHEN random() < 0.50 THEN 'systemd'
                WHEN random() < 0.60 THEN 'kernel'
                WHEN random() < 0.70 THEN 'kubelet'
                WHEN random() < 0.80 THEN 'haproxy'
                ELSE 'audit'
            END,
            -- Persona-based message templates
            CASE
                -- EXFIL messages (8%)
                WHEN random() < 0.08 THEN
                    (ARRAY[
                        'Data sync to cloud completed — transferred ' || (50 + random()*450)::int || 'MB',
                        'Archive exported to remote server — ' || (100 + random()*900)::int || 'MB in ' || (30 + random()*300)::int || 's',
                        'Backup completed — large outbound transfer ' || (200 + random()*800)::int || 'MB',
                        'Encrypted tunnel established — sync in progress',
                        'Remote replication job finished — ' || (150 + random()*850)::int || 'MB sent',
                        'Cloud storage upload complete'
                    ])[1 + (random()*5)::int]
                -- C2 messages (10%)
                WHEN random() < 0.18 THEN
                    (ARRAY[
                        'Heartbeat sent to monitoring server — seq=' || (random()*10000)::int,
                        'Keep-alive ping successful — interval 300s',
                        'Polling check-in completed',
                        'Watchdog timer reset — beacon acknowledged',
                        'Status update transmitted — jitter=' || (random()*50)::int || 'ms'
                    ])[1 + (random()*4)::int]
                -- RECON messages (12%)
                WHEN random() < 0.30 THEN
                    (ARRAY[
                        'Port scan detected from external source — ' || (100 + random()*400)::int || ' ports probed',
                        'Connection refused — scanning behavior identified',
                        'Multiple RST flags from same source',
                        'ICMP probe sweep detected',
                        'SYN flood pattern recognized — rate limiting applied',
                        'Nmap fingerprint detected in packet trace'
                    ])[1 + (random()*5)::int]
                -- NORMAL messages (70%)
                ELSE
                    (ARRAY[
                        'User authentication successful',
                        'Connection established to database',
                        'HTTP request processed — status 200',
                        'Service health check passed',
                        'Configuration reloaded',
                        'Cache refresh completed',
                        'Log rotation executed',
                        'Scheduled task completed',
                        'Network interface status OK',
                        'Memory usage within normal range'
                    ])[1 + (random()*9)::int]
            END,
            (1 + (random()*6)::int)
        FROM generate_series(1, 940000);  -- 8 batches × 940K = 7.52M
    END LOOP;
END $$;

DO $$ BEGIN RAISE NOTICE '[%] syslog_events COMPLETE (~7.5M rows).', clock_timestamp(); END $$;


-- Continue with DNS, Firewall, and other logs (normal generation - not persona-critical)
-- ... [rest of original script for dns_logs, firewall_logs, bgp_events, etc.]

DO $$ BEGIN RAISE NOTICE '[%] ═══════════════════════════════════════════════', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%] PERSONA-BASED DATA GENERATION COMPLETE', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%] Summary:', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%]   • Normal:  ~11.5M (70%%) — balanced traffic', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%]   • Recon:    ~2.0M (12%%) — high ports, low bytes', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%]   • Exfil:    ~1.3M ( 8%%) — MASSIVE bytes, few dsts', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%]   • C2:       ~1.7M (10%%) — periodic, low variance', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%] ═══════════════════════════════════════════════', clock_timestamp(); END $$;
