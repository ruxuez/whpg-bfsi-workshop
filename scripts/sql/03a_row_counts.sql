-- Row counts for the six BFSI fact tables. Run it before and after the gpfdist
-- load (03_load_external_bfsi.sql) to see the data arrive.
SET search_path TO bfsi_demo, public;
\timing on
SELECT 'transactions'    AS table_name, COUNT(*) AS row_count FROM transactions
UNION ALL SELECT 'auth_decisions',  COUNT(*) FROM auth_decisions
UNION ALL SELECT 'device_events',   COUNT(*) FROM device_events
UNION ALL SELECT 'case_narratives', COUNT(*) FROM case_narratives
UNION ALL SELECT 'wire_events',     COUNT(*) FROM wire_events
UNION ALL SELECT 'account_kpis',    COUNT(*) FROM account_kpis
ORDER BY row_count DESC;
