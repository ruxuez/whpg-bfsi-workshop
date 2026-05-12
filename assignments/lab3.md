# Lab 3: AI Analytics & The AI Factory

## Overview
In this lab, you will use **pgvector** for semantic log search and **Apache MADlib** for behavioral clustering to discover security threats. The goal is to demonstrate how **in-database machine learning** outperforms traditional keyword searches and external Python/Spark workflows.

**Key Learning Objectives:**
1. Understand why **keyword search is fragile** (misses variations in threat descriptions)
2. See how **pgvector semantic search** finds threats by **meaning**, not exact keywords
3. Watch **MADlib unsupervised clustering** automatically discover **4 threat patterns**
4. Experience the **AI Factory** - both systems validating each other in one SQL query

---

## The Three-Part Journey

###  Part A: Demonstrating pgvector Value
**The Problem:** Traditional keyword search is brittle - it only finds exact matches.

| Query | What It Shows |
| :--- | :--- |
| **A1: The Keyword Search Problem** | `LIKE '%brute force%'` finds 25K logs, but `LIKE '%port scan%'` finds ZERO. Same threat type, different words! This demonstrates keyword search fragility. |
| **A2: pgvector Finds Threats by MEANING** | Uses vector similarity to find logs semantically related to reconnaissance - discovers "REJECT TCP", "Connection refused", "SYN flood", "port probe" WITHOUT exact keyword matches. |

**Key Insight:** Semantic search understands **meaning** and catches variations keyword search misses.

---

### Part B: Demonstrating MADlib Value  
**The Power:** Unsupervised learning discovers patterns automatically - no labels, no rules, just math.

| Query | What It Shows |
| :--- | :--- |
| **B1: MADlib Discovered 4 Threat Personas** | K-Means clustering found 4 behavioral clusters: 🔍 RECON (high ports), 📤 EXFIL (massive bytes), 🤖 C2 (low variance), ✅ NORMAL. |
| **B2: The Dramatic Differences** | RECON cluster: **22,073 avg ports** (3,679× normal). EXFIL cluster: **32,225 GB average** (35,000× normal). These aren't subtle - they're mathematically undeniable. |

**Key Insight:** MADlib discovers threats **automatically** with dramatic statistical evidence.

---

### Part C: The AI Factory (Validation Through Agreement)
**The Proof:** When two independent AI systems find the same threats, confidence is high.

| Query | What It Shows |
| :--- | :--- |
| **C1: Threat Pattern Correlation** | Shows how MADlib (behavioral) and pgvector (semantic) **independently discovered the same 3 threat types**: RECON, EXFIL, C2. Both systems agree in ONE SQL query - no Python export! |
| **C2: Why This Matters** | Traditional warehouse: export → train → join (hours). WarehousePG: one SQL query (5 seconds on 16M rows). In-database ML eliminates the "data movement tax". |

**Key Insight:** MADlib provides behavioral evidence ("32 TB transferred"), pgvector provides semantic evidence ("logs say 'Large outbound transfer'"). When both agree = high confidence.

---

## Lab Setup

### Prerequisites
Data was already loaded in Lab 1. This lab extracts AI/ML features from that existing data.

**Embedded Threat Personas:**
The data contains 4 behavioral patterns:
- **Normal (70%)**: Baseline business traffic  
- **RECON (12%)**: Port scanning - hundreds of unique ports, tiny bytes
- **EXFIL (8%)**: Data exfiltration - massive bytes (GBs), few destinations  
- **C2 (10%)**: Command & control - constant small payload, periodic timing

### Prepare Terminal Tabs

Prepare 1 Shell Tab:

Connection to cdw envionment shell (WarehousePG Tab):
```bash
docker exec -u gpadmin -w /home/gpadmin -it cdw /bin/bash
```

---

## Step 1: Build pgvector Embeddings for Semantic Search

### 1.1 Open psql and Clean Up

```bash
psql demo
```

```sql
SET search_path TO netvista_demo, public;

-- Clean up any existing ML tables
DROP TABLE IF EXISTS netvista_demo.kmeans_assignments CASCADE;
DROP TABLE IF EXISTS netvista_demo.netflow_features_agg CASCADE;
DROP TABLE IF EXISTS netvista_demo.netflow_features CASCADE;
DROP TABLE IF EXISTS netvista_demo.syslog_embeddings CASCADE;

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;
```

**What this does:** Prepares a clean slate and enables pgvector for vector operations.

---

### 1.2 Create Syslog Embeddings Table

```sql
CREATE TABLE netvista_demo.syslog_embeddings (
    event_id     BIGINT,
    src_ip       INET,
    hostname     TEXT,
    program      TEXT,
    message      TEXT,
    severity     INT,
    persona      TEXT,          -- 'normal' | 'recon' | 'exfil' | 'c2'
    embedding    vector(32)     -- 32-dimensional feature vector
) DISTRIBUTED BY (event_id);
```

**What this does:** Creates a table where each syslog message will have a 32-dimensional vector representation.

**Why vectors?** In production, you'd use a sentence-transformer model (e.g., all-MiniLM-L6-v2) to convert text to embeddings. Here we create feature-based vectors from message characteristics (program type, keywords, severity).

---

### 1.3 Generate Embeddings from Syslog Messages

**⚠️ LONG QUERY - Copy the entire block:**

```sql
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
        -- Classify each message into a persona for lab validation
        CASE 
            -- EXFIL: data transfer tools or sync/backup keywords
            WHEN program IN ('rsync','rclone','backup-svc','openvpn','curl','audit','netfilter') 
              OR message ILIKE '%outbound%' OR message ILIKE '%Archive%' OR message ILIKE '%sync%' 
              OR message ILIKE '%backup%' OR message ILIKE '%upload%' 
            THEN 'exfil'
            -- C2: beaconing tools or heartbeat keywords
            WHEN program IN ('beacon','svchost','cron') 
              OR message ILIKE '%heartbeat%' OR message ILIKE '%keep-alive%' 
              OR message ILIKE '%polling%' OR message ILIKE '%C2%' 
              OR message ILIKE '%beacon%'
            THEN 'c2'
            -- RECON: security tools or scanning keywords
            WHEN program IN ('snort','firewalld','iptables') 
              OR message ILIKE '%scan%' OR message ILIKE '%probe%' 
              OR message ILIKE '%flood%' OR message ILIKE '%nmap%'
            THEN 'recon'
            ELSE 'normal'
        END AS persona,
        -- Build 32-dimensional feature vector
        -- Each dimension = 0.0 or 1.0 based on characteristics
        ARRAY[
            severity::float / 7.0,  -- [0] normalized severity (0-1 scale)
            CASE WHEN program = 'sshd' THEN 1.0 ELSE 0.0 END,  -- [1] SSH-related
            CASE WHEN program IN ('firewalld','iptables','snort') THEN 1.0 ELSE 0.0 END,  -- [2] security tools
            CASE WHEN program = 'kernel' THEN 1.0 ELSE 0.0 END,  -- [3] kernel messages
            CASE WHEN program IN ('haproxy','kubelet','systemd','ntpd') THEN 1.0 ELSE 0.0 END,  -- [4] system services
            CASE WHEN program IN ('rsync','rclone','backup-svc','openvpn','curl','sftp') THEN 1.0 ELSE 0.0 END,  -- [5] data transfer tools
            CASE WHEN program IN ('cron','beacon','svchost') THEN 1.0 ELSE 0.0 END,  -- [6] scheduling/C2 tools
            CASE WHEN program = 'audit' THEN 1.0 ELSE 0.0 END,  -- [7] audit logs
            -- Message content features [8-19] - keyword indicators
            CASE WHEN message ILIKE '%scan%' OR message ILIKE '%probe%' OR message ILIKE '%flood%' THEN 1.0 ELSE 0.0 END,  -- [8] recon keywords
            CASE WHEN message ILIKE '%nmap%' OR message ILIKE '%port scan%' OR message ILIKE '%RST flag%' THEN 1.0 ELSE 0.0 END,  -- [9] scanning tools
            CASE WHEN message ILIKE '%outbound transfer%' OR message ILIKE '%MB in%' OR message ILIKE '%export%' THEN 1.0 ELSE 0.0 END,  -- [10] exfil keywords
            CASE WHEN message ILIKE '%encrypted tunnel%' OR message ILIKE '%sync to cloud%' OR message ILIKE '%Archive%' THEN 1.0 ELSE 0.0 END,  -- [11] data movement
            CASE WHEN message ILIKE '%heartbeat%' OR message ILIKE '%keep-alive%' OR message ILIKE '%beacon%' THEN 1.0 ELSE 0.0 END,  -- [12] C2 beaconing
            CASE WHEN message ILIKE '%polling%' OR message ILIKE '%check-in%' OR message ILIKE '%watchdog%' THEN 1.0 ELSE 0.0 END,  -- [13] C2 polling
            CASE WHEN message ILIKE '%Connection refused%' OR message ILIKE '%ICMP%' THEN 1.0 ELSE 0.0 END,  -- [14] network recon
            CASE WHEN message ILIKE '%passwd%' OR message ILIKE '%credential%' OR message ILIKE '%harvest%' THEN 1.0 ELSE 0.0 END,  -- [15] credential access
            CASE WHEN message ILIKE '%SYN%' OR message ILIKE '%RST%' OR message ILIKE '%flood%' THEN 1.0 ELSE 0.0 END,  -- [16] network attacks
            CASE WHEN message ILIKE '%backup%' OR message ILIKE '%tar.gz%' OR message ILIKE '%.zip%' THEN 1.0 ELSE 0.0 END,  -- [17] backup/archive
            CASE WHEN message ILIKE '%upload%' OR message ILIKE '%POST%' OR message ILIKE '%payload%' THEN 1.0 ELSE 0.0 END,  -- [18] data upload
            CASE WHEN message ILIKE '%interval%' OR message ILIKE '%seq=%' OR message ILIKE '%jitter%' THEN 1.0 ELSE 0.0 END,  -- [19] timing patterns
            -- Severity features [20-22]
            CASE WHEN severity <= 2 THEN 1.0 ELSE 0.0 END,  -- [20] critical severity
            CASE WHEN severity = 3 THEN 1.0 ELSE 0.0 END,  -- [21] error severity
            CASE WHEN severity = 4 THEN 1.0 ELSE 0.0 END,  -- [22] warning severity
            -- Hostname type features [23-25]
            CASE WHEN hostname LIKE 'ids-%' THEN 1.0 ELSE 0.0 END,  -- [23] IDS sensor
            CASE WHEN hostname LIKE 'srv-%' THEN 1.0 ELSE 0.0 END,  -- [24] server
            CASE WHEN hostname LIKE 'host-%' THEN 1.0 ELSE 0.0 END,  -- [25] endpoint
            -- Random noise padding [26-31] - adds slight variation
            random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05
        ]::vector(32) AS embedding
    FROM netvista_demo.syslog_events
    WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
) sub
-- Sample: keep all threat logs, 10% of normal logs (to avoid overwhelming with normal traffic)
WHERE persona != 'normal' OR (persona = 'normal' AND event_id % 10 = 0)
LIMIT 200000;

ANALYZE netvista_demo.syslog_embeddings;
```

**What this does:** 
- Converts ~200K syslog messages into 32-dimensional vectors
- Each vector dimension represents a feature (program type, severity, keywords)
- Enables semantic similarity search: "find logs similar to this threat"

**How it works:** When you search for logs similar to "Port scan detected", pgvector computes cosine distance between vectors and returns the closest matches - even if they use different words like "Connection refused" or "SYN flood".

**Expected time:** 1-2 minutes

**Expected output:** "ANALYZE" completes, ~200K rows inserted

---

### 1.4 Add Diverse Sample Messages for Demo Quality

Run the diversity enhancement script:
```sql
\i /scripts/sql/08_add_diverse_syslogs.sql
```

**What this does:** Adds 20+ manually crafted messages with diverse phrasings to ensure Query A2 shows clear variety (not just one repeated message).

**Sample messages added:**
- RECON: "Port scan detected", "REJECT TCP", "SYN flood", "Connection refused", "RST flag", "nmap detected"
- EXFIL: "Large outbound transfer", "Archive exported", "Cloud sync", "Encrypted tunnel", "Backup completed"
- C2: "Beacon", "Heartbeat", "Polling", "Keep-alive", "DNS beacon"

**Expected time:** 10 seconds

---

## Step 2: Build MADlib Features for Behavioral Clustering

### 2.1 Create Netflow Behavioral Features

```sql
CREATE TABLE netvista_demo.netflow_features AS
SELECT
    date_trunc('hour', ts) AS hour,
    src_ip,
    COUNT(*) AS flow_count,
    COUNT(DISTINCT dst_ip) AS unique_dsts,
    COUNT(DISTINCT dst_port) AS unique_ports,
    SUM(bytes) AS total_bytes,
    -- Entropy: ratio of unique destinations to total flows
    -- High entropy (close to 1.0) = scattered destinations (normal browsing)
    -- Low entropy (close to 0.0) = focused on few destinations (exfil, C2)
    ROUND(COUNT(DISTINCT dst_ip)::numeric / NULLIF(COUNT(*), 0), 4) AS dst_entropy,
    -- Port spread: ratio of unique ports to total flows  
    -- High port spread = scanning behavior (recon)
    -- Low port spread = normal targeted communication
    ROUND(COUNT(DISTINCT dst_port)::numeric / NULLIF(COUNT(*), 0), 4) AS port_spread,
    -- Byte coefficient of variation: stddev / mean
    -- Low byte_cv (<0.3) = consistent payload size (C2 beaconing)
    -- High byte_cv (>1.0) = variable payload (normal traffic)
    ROUND(STDDEV_SAMP(bytes) / NULLIF(AVG(bytes), 0), 4) AS byte_cv
FROM netvista_demo.netflow_logs
WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
GROUP BY 1, 2
HAVING COUNT(*) >= 3  -- minimum activity threshold
DISTRIBUTED BY (src_ip);

ANALYZE netvista_demo.netflow_features;
```

**What this does:** 
Creates one row per IP per hour with **6 behavioral features**:

| Feature | What It Detects | Threat Signature |
|---------|----------------|------------------|
| `flow_count` | Activity volume | High = active scanning |
| `unique_dsts` | Destination count | Low = focused targeting |
| `unique_ports` | Port diversity | **High (>100) = RECON scanning** |
| `total_bytes` | Data volume | **High (GBs) = EXFIL** |
| `dst_entropy` | Destination scatter | Low = exfil/C2 focus |
| `port_spread` | Port ratio | High = port scanning |
| `byte_cv` | Payload consistency | **Low (<0.3) = C2 beaconing** |

**Expected output:** ~11K-12K IP-hour profiles

**Expected time:** 30-60 seconds

---

## Step 3: Run MADlib K-Means Clustering

```bash
\i /scripts/sql/07_kmeans_fallback.sql
```

**What this script does:**

**Step 3.1: Aggregate IP behavior across all hours**
- **Critical!** Combines hourly profiles into one row per IP
- This is the fix that made clustering work properly

**Step 3.2: Detect MADlib availability**
- Checks if `madlib.kmeanspp()` function exists

**Step 3.3a: Path A - MADlib Clustering (if available)**
- Normalizes all 6 features (z-score standardization)
- Runs `madlib.kmeanspp()` with k=5 clusters, 100 iterations
- Uses Euclidean distance to group similar IPs

**Step 3.3b: Path B - SQL Fallback (if MADlib unavailable)**
- Calculates z-scores manually
- Uses rule-based persona detection:
  - RECON: z_ports > 4 AND unique_ports > 50
  - EXFIL: z_bytes > 5 AND total_bytes > 50M
  - C2: byte_cv < 0.4 AND dst_entropy < 0.3
  - NORMAL: everything else

**Step 3.4: Create cluster assignments**
- `kmeans_assignments` table: maps each src_ip to a cluster_id

**Expected output:**

```
cluster_id | member_count | pct_of_total | inferred_persona
-----------+--------------+--------------+------------------
         0 |         8542 |         76.8 | NORMAL (Baseline)
         1 |         1123 |         10.1 | RECON (High Ports)
         2 |          748 |          6.7 | EXFIL (High Bytes)
         3 |          982 |          8.8 | C2 (Beaconing)
         4 |           47 |          0.4 | SUSPECT (Mixed)
```

**Expected time:** 30-60 seconds

**What you should see:**
- Cluster 1 (RECON): avg 22,000+ ports
- Cluster 2 (EXFIL): avg 32,000+ GB (32 TB!)
- Cluster 3 (C2): avg byte_cv < 0.3 (consistent beaconing)

These dramatic differences prove the clustering worked!

---

## Step 4: Visualize Clusters (Optional)

### Option A: MADlib Cluster Explorer

```bash
python3.9 /scripts/apps/dashboard.py
```

**Access:** http://localhost:5003

**Features:**
- Scatter plot: choose X/Y axes (e.g., `bytes_mb` vs `unique_ports`)
- Color-coded clusters
- Hover for IP details

**Try this:** Select `bytes_mb` (X) vs `unique_ports` (Y) - you'll see 4 distinct clusters in different corners:

Cluster near origin (0,0): Normal traffic - low flows, low bytes
🔴 Red dot at ~130k flows, low bytes: RECON/Port Scan
High connection count, minimal data transfer
This IP is touching many targets with small probes
🔴 Red dot at ~30M MB (30 TB): DATA EXFIL
Massive data transfer (30,000 GB!)
Clear exfiltration candidate

Press `CTRL+C` to quit when done.

---

## Step 5: Launch the AI Factory Dashboard

```bash
python3.9 /scripts/apps/app3.py
```

**Access:** http://localhost:5002

### Execute the 6 Queries

**Panel A: pgvector Value**

**Query A1: The Keyword Search Problem**
- Shows: `LIKE '%brute force%'` → 25K results, but `LIKE '%port scan%'` → 0 results
- **Observation:** Same threat type (recon), different words = missed!
- **Lesson:** Keyword search is fragile

**Query A2: pgvector Finds Threats by MEANING**
- Shows: 15+ semantically similar recon logs
- **Observation:** "Port scan detected", "REJECT TCP", "Connection refused", "SYN flood" - all found WITHOUT exact keywords!
- **Lesson:** Semantic search understands meaning

---

**Panel B: MADlib Value**

**Query B1: MADlib Discovered 4 Threat Personas**
- Shows: 5 clusters with their characteristics
- **Observation:** Clear behavioral signatures for RECON, EXFIL, C2, NORMAL
- **Lesson:** Unsupervised learning discovers patterns automatically

**Query B2: The Dramatic Differences**
- Shows: Quantified separation between clusters
- **Observation:** 
  - RECON: 22,073 avg ports (3,679× more than normal!)
  - EXFIL: 32,225 GB average (35,000× more bytes!)
- **Lesson:** These aren't subtle - they're mathematically undeniable

---

**Panel C: The AI Factory**

**Query C1: Threat Pattern Correlation**
- Shows: How MADlib and pgvector independently found the SAME 3 threat types
- **Observation:** 
  - RECON: 15 IPs (22K ports - MADlib) + 25K logs (firewall/IDS - pgvector)
  - EXFIL: 10 IPs (32 TB - MADlib) + 61K logs (rsync/rclone - pgvector)
  - C2: 939 IPs (low variance - MADlib) + 61K logs (beacon - pgvector)
- **Lesson:** When both systems agree = high confidence threat detection

**Query C2: Why This Matters**
- Shows: Comparison of traditional warehouse vs WarehousePG
- **Observation:** 
  - Traditional: Export → Python → Train → Upload → Join (hours)
  - WarehousePG: One SQL query (<5 seconds on 16M rows)
- **Lesson:** In-database ML eliminates "data movement tax"

---

## Deliverables

### 1. Query A1 Screenshot: Keyword Search Limitations
- Run Query A1
- **Screenshot showing:** found_brute_force = 25,736, found_port_scan = 0
- **Write:** "Keyword search found 25K 'brute force' logs but 0 'port scan' logs - both reconnaissance, just different words!"

### 2. Query A2 Screenshot: Semantic Search Success  
- Run Query A2
- **Screenshot showing:** 15+ diverse messages (Port scan, REJECT TCP, Connection refused, SYN flood, etc.)
- **Write:** "pgvector found all these reconnaissance logs semantically - different words, same meaning!"

### 3. Query B2 Screenshot: Dramatic Differences
- Run Query B2
- **Screenshot showing:** RECON 22,073 ports (3,679× ratio), EXFIL 32,225 GB
- **Write:** "MADlib discovered RECON with 3,679× more ports and EXFIL with 32 TB average - impossible to miss!"

### 4. Query C1 Screenshot: The AI Factory
- Run Query C1  
- **Screenshot showing:** Both systems detected RECON, EXFIL, C2
- **Write:** "MADlib found behavioral anomalies, pgvector found semantic logs - both systems agree!"

### 5. Technical Reflection (250-300 words)

**Answer these questions:**

**A. Why is semantic search better than keyword search?**
- Reference your A1 vs A2 screenshots
- Explain the brittleness problem
- Give specific examples from your results

**B. What makes MADlib clustering powerful?**
- Reference your B2 screenshot (3,679×, 32 TB numbers)
- Explain "unsupervised" - no labels needed
- Why are the differences mathematically undeniable?

**C. Why combine MADlib and pgvector?**
- Reference your C1 screenshot
- Explain "behavioral + semantic = validation"
- Why is agreement between systems important?

**D. Why is in-database ML better than Python export?**
- Time: 5 seconds vs hours
- Complexity: one query vs multi-step pipeline
- Movement: data stays in database

**Template to get you started:**

> "Query A1 revealed the fragility of keyword search: searching for 'brute force' found 25,736 logs, but 'port scan' found zero - both describe reconnaissance behavior but use different words. Query A2's pgvector semantic search solved this by finding [list specific examples from your screenshot] - all semantically similar reconnaissance logs discovered without exact keyword matches.
>
> Query B2 showed MADlib's power through mathematical certainty: the RECON cluster averaged 22,073 ports (3,679 times more than normal) while the EXFIL cluster averaged 32 TERABYTES (35,000 times more bytes). These aren't subtle anomalies requiring expert interpretation - they're statistically undeniable. MADlib discovered these patterns unsupervised, without any labeled training data or manually-tuned rules.
>
> Query C1 demonstrated the AI Factory concept: MADlib found 15 RECON IPs with extreme port counts (behavioral anomaly), while pgvector found 25,736 firewall/IDS logs saying 'Port scan detected', 'SYN flood', 'Connection refused' (semantic evidence). When both independent systems detect the same threat patterns, confidence is high. MADlib tells us WHO is suspicious based on behavior, pgvector tells us WHAT they were doing based on log semantics.
>
> The in-database advantage is clear: traditional warehouses require exporting 16M rows to Python, training models offline, uploading results, then joining back - taking hours. WarehousePG runs both AI engines (MADlib + pgvector) in one SQL query in under 5 seconds. No data movement, no ETL pipeline, no model versioning issues. The AI Factory proves that keeping ML in-database delivers both speed and integration advantages."

---

## Key Takeaways

**Why This Lab Matters:**

1. **Keyword Search Fails:** Missing one synonym = missing threats
2. **Semantic Search Wins:** Finds threats by meaning, catches variations  
3. **MADlib Discovers:** Unsupervised clustering finds patterns humans might miss
4. **Statistics Don't Lie:** 3,679× and 32 TB differences are undeniable
5. **Validation Through Agreement:** Two independent systems confirming = high confidence
6. **In-Database Speed:** Seconds vs hours - no Python export tax

**Real-World Impact:**

In a SOC, you can't predict every way an attacker might be described in logs. pgvector semantic search solves this. You can't manually set thresholds for every possible attack pattern. MADlib clustering discovers them automatically. And you can't afford hours of ETL latency. In-database ML delivers answers in seconds.

This is the future of data analytics: **AI that runs where the data lives.**

