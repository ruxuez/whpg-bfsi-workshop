-- =============================================================================
-- 07_kmeans_fixed.sql
-- CORRECTED VERSION: Aggregates per src_ip BEFORE normalization
-- =============================================================================

SET search_path TO netvista_demo, public;

-- Drop existing tables
DROP TABLE IF EXISTS netvista_demo.kmeans_assignments;
DROP TABLE IF EXISTS netvista_demo.netflow_features_norm;
DROP TABLE IF EXISTS netvista_demo.netflow_features_agg;

DO $$
BEGIN

  -- ══════════════════════════════════════════════════════════════════════════
  -- STEP 1: Aggregate ALL hours per src_ip FIRST
  -- This is critical! We need ONE row per IP with its aggregate behavior
  -- ══════════════════════════════════════════════════════════════════════════

  RAISE NOTICE 'Step 1: Aggregating features per src_ip (all hours combined)...';

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

  RAISE NOTICE 'Aggregated % unique IPs', (SELECT COUNT(*) FROM netvista_demo.netflow_features_agg);

  -- ══════════════════════════════════════════════════════════════════════════
  -- STEP 2: Check if MADlib is available
  -- ══════════════════════════════════════════════════════════════════════════

  IF EXISTS (
      SELECT 1
      FROM   pg_proc     p
      JOIN   pg_namespace n ON n.oid = p.pronamespace
      WHERE  n.nspname = 'madlib'
      AND    p.proname = 'kmeanspp'
  ) THEN

    -- ════════════════════════════════════════════════════════════════════════
    -- PATH A: MADlib available → real kmeanspp
    -- ════════════════════════════════════════════════════════════════════════

    RAISE NOTICE 'MADlib detected — normalizing aggregated features...';

    -- Normalize the SIX behavioral features using the aggregated data
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

    RAISE NOTICE 'Running madlib.kmeanspp (k=5, max_iter=100) on % rows...',
        (SELECT COUNT(*) FROM netvista_demo.netflow_features_norm);

    -- Run K-Means and assign clusters
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

    RAISE NOTICE 'MADlib kmeanspp complete — kmeans_assignments populated';

  -- ══════════════════════════════════════════════════════════════════════════
  -- PATH B: MADlib absent → pure-SQL persona detection
  -- ══════════════════════════════════════════════════════════════════════════
  ELSE

    RAISE NOTICE 'MADlib not found — using SQL rule-based persona detection';

    CREATE TABLE netvista_demo.kmeans_assignments AS
    WITH stats AS (
        -- Global mean / stddev for each behavioral feature
        SELECT
            AVG(total_flows)      AS mu_f,  STDDEV_SAMP(total_flows)      AS sd_f,
            AVG(total_bytes)      AS mu_b,  STDDEV_SAMP(total_bytes)      AS sd_b,
            AVG(avg_unique_dsts)  AS mu_d,  STDDEV_SAMP(avg_unique_dsts)  AS sd_d,
            AVG(avg_unique_ports) AS mu_p,  STDDEV_SAMP(avg_unique_ports) AS sd_p,
            AVG(avg_dst_entropy)  AS mu_e,  STDDEV_SAMP(avg_dst_entropy)  AS sd_e,
            AVG(avg_port_spread)  AS mu_s,  STDDEV_SAMP(avg_port_spread)  AS sd_s,
            AVG(avg_byte_cv)      AS mu_cv, STDDEV_SAMP(avg_byte_cv)      AS sd_cv
        FROM netvista_demo.netflow_features_agg
    ),
    scored AS (
        -- Persona-aware scoring
        SELECT
            f.src_ip,
            -- Z-scores for each feature
            ABS(f.total_flows      - s.mu_f)  / NULLIF(s.sd_f, 0)  AS z_flow,
            ABS(f.total_bytes      - s.mu_b)  / NULLIF(s.sd_b, 0)  AS z_bytes,
            ABS(f.avg_unique_ports - s.mu_p)  / NULLIF(s.sd_p, 0)  AS z_ports,
            ABS(f.avg_dst_entropy  - s.mu_e)  / NULLIF(s.sd_e, 0)  AS z_entropy,
            ABS(f.avg_port_spread  - s.mu_s)  / NULLIF(s.sd_s, 0)  AS z_spread,
            ABS(f.avg_byte_cv      - s.mu_cv) / NULLIF(s.sd_cv, 0) AS z_cv,
            -- Composite anomaly score (weighted)
            (   ABS(f.total_flows      - s.mu_f)  / NULLIF(s.sd_f, 0)
            + ABS(f.total_bytes      - s.mu_b)  / NULLIF(s.sd_b, 0) * 2.0
            + ABS(f.avg_unique_ports - s.mu_p)  / NULLIF(s.sd_p, 0) * 2.0
            + ABS(f.avg_dst_entropy  - s.mu_e)  / NULLIF(s.sd_e, 0)
            + ABS(f.avg_port_spread  - s.mu_s)  / NULLIF(s.sd_s, 0)
            + ABS(f.avg_byte_cv      - s.mu_cv) / NULLIF(s.sd_cv, 0)
            ) AS anomaly_score,
            -- Raw features for persona detection
            f.avg_unique_ports, f.total_bytes, f.avg_dst_entropy, f.avg_byte_cv
        FROM netvista_demo.netflow_features_agg f, stats s
    )
    -- Persona-based clustering
    SELECT
        src_ip,
        CASE
            -- RECON: high ports (z > 4) + low bytes
            WHEN z_ports > 4 AND z_bytes < 2 AND avg_unique_ports > 50 THEN 1
            -- EXFIL: extreme bytes (z > 5) + low entropy
            WHEN z_bytes > 5 AND avg_dst_entropy < 0.2 AND total_bytes > 50000000 THEN 2
            -- C2: low byte_cv (constant payload) + low entropy
            WHEN avg_byte_cv < 0.4 AND avg_dst_entropy < 0.3 AND z_flow BETWEEN 0.5 AND 3 THEN 3
            -- High anomaly but doesn't fit patterns
            WHEN anomaly_score > 8 THEN 4
            -- NORMAL: everything else
            ELSE 0
        END AS cluster_id
    FROM scored
    DISTRIBUTED BY (src_ip);

    RAISE NOTICE 'SQL fallback clustering complete — kmeans_assignments populated';

  END IF;

END$$;

-- Refresh planner stats
ANALYZE netvista_demo.kmeans_assignments;

-- Show cluster distribution
RAISE NOTICE 'Cluster distribution:';
SELECT
    cluster_id,
    COUNT(*)                          AS member_count,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM netvista_demo.kmeans_assignments
GROUP BY 1
ORDER BY 1;

-- Show cluster characteristics
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
