-- ============================================================================
-- Meridian Retail Bank × EDB WarehousePG — Reference Data Seed
-- ============================================================================
SET search_path TO bfsi_demo, public;

TRUNCATE regions;
TRUNCATE bin_ranges;
TRUNCATE customers;
TRUNCATE risk_profiles;
TRUNCATE fraud_watchlists;
TRUNCATE country_risk;

-- ─── Regions (booking entities) ─────────────────────────────────────────────
INSERT INTO regions (region_code, region_name, timezone) VALUES
    ('US-EAST',  'US East (New York)',        'America/New_York'),
    ('US-WEST',  'US West (San Francisco)',   'America/Los_Angeles'),
    ('EU-WEST',  'EU West (Frankfurt)',       'Europe/Berlin'),
    ('EU-EAST',  'EU East (Warsaw)',          'Europe/Warsaw'),
    ('APAC-JP',  'APAC Japan (Tokyo)',        'Asia/Tokyo'),
    ('APAC-SG',  'APAC Singapore',            'Asia/Singapore'),
    ('LATAM',    'Latin America (São Paulo)', 'America/Sao_Paulo');

-- ─── BIN Ranges (card scheme master, hierarchical int8range) ────────────────
-- 8-digit BINs. Parent supernets + issuer child ranges. The last two rows
-- overlap on purpose to demonstrate native range-overlap detection.
INSERT INTO bin_ranges (bin_range, scheme, issuer_name, product_type, country_code, region_id, parent_range) VALUES
    -- Visa supernet + children
    (int8range(40000000, 49999999, '[]'), 'VISA',       'Visa Supernet',                'credit',     'US', 1, NULL),
    (int8range(41010000, 41019999, '[]'), 'VISA',       'Meridian Visa Classic',        'credit',     'US', 1, int8range(40000000,49999999,'[]')),
    (int8range(42020000, 42029999, '[]'), 'VISA',       'Meridian Visa Debit',          'debit',      'US', 1, int8range(40000000,49999999,'[]')),
    (int8range(45050000, 45059999, '[]'), 'VISA',       'Meridian Visa Signature',      'credit',     'EU', 3, int8range(40000000,49999999,'[]')),
    -- Mastercard supernet + children
    (int8range(51000000, 55999999, '[]'), 'MASTERCARD', 'Mastercard Supernet',          'credit',     'US', 1, NULL),
    (int8range(52020000, 52029999, '[]'), 'MASTERCARD', 'Meridian World Mastercard',    'credit',     'US', 2, int8range(51000000,55999999,'[]')),
    (int8range(53030000, 53039999, '[]'), 'MASTERCARD', 'Meridian Mastercard Prepaid',  'prepaid',    'EU', 3, int8range(51000000,55999999,'[]')),
    -- Amex
    (int8range(37000000, 37999999, '[]'), 'AMEX',       'Meridian Amex Platinum',       'commercial', 'US', 1, NULL),
    -- Discover
    (int8range(60110000, 60119999, '[]'), 'DISCOVER',   'Meridian Discover',            'credit',     'US', 2, NULL),
    -- APAC issuing
    (int8range(43030000, 43039999, '[]'), 'VISA',       'Meridian Visa APAC',           'credit',     'JP', 5, int8range(40000000,49999999,'[]')),
    -- Two deliberately OVERLAPPING ranges (data-quality demo via v_bin_overlaps)
    (int8range(58000000, 58005000, '[]'), 'MASTERCARD', 'Legacy Co-Brand A',            'credit',     'US', 1, NULL),
    (int8range(58004000, 58009000, '[]'), 'MASTERCARD', 'Legacy Co-Brand B',            'credit',     'US', 1, NULL);

-- ─── Customers (portfolios; account_range = contiguous account-id band) ─────
INSERT INTO customers (customer_name, region_id, segment, kyc_risk, account_range) VALUES
    ('Meridian Retail US-East',     1, 'retail',  'low',    int8range(100000000, 100999999, '[]')),
    ('Meridian Retail US-West',     2, 'retail',  'low',    int8range(101000000, 101999999, '[]')),
    ('Meridian Private Wealth',     1, 'private', 'medium', int8range(102000000, 102099999, '[]')),
    ('Meridian SME Banking',        1, 'sme',     'medium', int8range(103000000, 103999999, '[]')),
    ('Meridian Retail EU-West',     3, 'retail',  'low',    int8range(104000000, 104999999, '[]')),
    ('Meridian Retail EU-East',     4, 'retail',  'medium', int8range(105000000, 105999999, '[]')),
    ('Meridian Retail APAC-JP',     5, 'retail',  'low',    int8range(106000000, 106999999, '[]')),
    ('Meridian Retail APAC-SG',     6, 'retail',  'low',    int8range(107000000, 107999999, '[]')),
    ('Meridian Retail LATAM',       7, 'retail',  'high',   int8range(108000000, 108999999, '[]')),
    ('Meridian Prepaid Programs',   2, 'retail',  'high',   int8range(109000000, 109999999, '[]'));

-- ─── Risk Profiles (expected-behaviour thresholds per portfolio) ────────────
INSERT INTO risk_profiles (customer_id, max_daily_amount, max_txn_velocity, max_decline_rate, expected_avg_ticket, effective_from)
SELECT
    customer_id,
    CASE segment WHEN 'private' THEN 250000.00 WHEN 'sme' THEN 100000.00 ELSE 10000.00 END,
    CASE segment WHEN 'sme' THEN 200 WHEN 'private' THEN 80 ELSE 60 END,
    CASE kyc_risk WHEN 'high' THEN 8.00 WHEN 'medium' THEN 5.00 ELSE 3.00 END,
    CASE segment WHEN 'private' THEN 1200.00 WHEN 'sme' THEN 450.00 ELSE 75.00 END,
    onboarded_at
FROM customers;

-- ─── Fraud Watchlists ───────────────────────────────────────────────────────
-- Compromised-BIN feeds arrive as BIN *ranges* (int8range) -> native <@ join.
-- single_account rows target specific mule / sanctioned accounts.
INSERT INTO fraud_watchlists (feed_name, bin_range, single_account, category, confidence, country_code, first_seen, last_seen) VALUES
    -- Compromised BIN bands (subsets of issuer ranges -> match seeded personas)
    ('Visa CAMS',    int8range(41010100, 41010199, '[]'), NULL, 'compromised_bin', 96, 'US', '2026-06-30 12:00:00'::timestamp-interval '10 days', '2026-06-30 12:00:00'::timestamp-interval '4 hours'),
    ('MC ADC',       int8range(52020200, 52020299, '[]'), NULL, 'compromised_bin', 94, 'US', '2026-06-30 12:00:00'::timestamp-interval '8 days',  '2026-06-30 12:00:00'::timestamp-interval '2 hours'),
    ('Visa CAMS',    int8range(45050500, 45050599, '[]'), NULL, 'compromised_bin', 88, 'EU', '2026-06-30 12:00:00'::timestamp-interval '20 days', '2026-06-30 12:00:00'::timestamp-interval '1 day'),
    ('Internal SOC', int8range(53030300, 53030399, '[]'), NULL, 'compromised_bin', 80, 'EU', '2026-06-30 12:00:00'::timestamp-interval '5 days',  '2026-06-30 12:00:00'::timestamp-interval '6 hours'),
    -- Known mule accounts (structuring beneficiaries)
    ('Internal AML', NULL, 108500001, 'mule',        92, 'BR', '2026-06-30 12:00:00'::timestamp-interval '15 days', '2026-06-30 12:00:00'::timestamp-interval '3 hours'),
    ('Internal AML', NULL, 108500002, 'mule',        90, 'NG', '2026-06-30 12:00:00'::timestamp-interval '12 days', '2026-06-30 12:00:00'::timestamp-interval '5 hours'),
    ('Internal AML', NULL, 105500050, 'mule',        85, 'RU', '2026-06-30 12:00:00'::timestamp-interval '9 days',  '2026-06-30 12:00:00'::timestamp-interval '2 hours'),
    -- OFAC sanctioned account
    ('OFAC SDN',     NULL, 999000001, 'sanctioned',  99, 'IR', '2026-06-30 12:00:00'::timestamp-interval '60 days', '2026-06-30 12:00:00'::timestamp-interval '1 hour'),
    ('OFAC SDN',     NULL, 999000002, 'sanctioned',  99, 'KP', '2026-06-30 12:00:00'::timestamp-interval '45 days', '2026-06-30 12:00:00'::timestamp-interval '1 hour'),
    -- High-risk corridor (whole prepaid program flagged for velocity abuse)
    ('Internal SOC', int8range(53030000, 53039999, '[]'), NULL, 'velocity_abuse', 75, 'EU', '2026-06-30 12:00:00'::timestamp-interval '7 days', '2026-06-30 12:00:00'::timestamp-interval '30 minutes');

-- ─── Country Risk (AML geo enrichment) ──────────────────────────────────────
INSERT INTO country_risk (country_code, country_name, region, risk_score, fatf_status, is_sanctioned, latitude, longitude) VALUES
    ('US', 'United States', 'NA',     5,  'compliant', FALSE,  38.95070, -77.44720),
    ('GB', 'United Kingdom','EU',     8,  'compliant', FALSE,  51.50740,  -0.12780),
    ('DE', 'Germany',       'EU',     7,  'compliant', FALSE,  50.11550,   8.68420),
    ('FR', 'France',        'EU',     9,  'compliant', FALSE,  48.85660,   2.35220),
    ('JP', 'Japan',         'APAC',   6,  'compliant', FALSE,  35.68950, 139.69170),
    ('SG', 'Singapore',     'APAC',   6,  'compliant', FALSE,   1.35210, 103.81980),
    ('BR', 'Brazil',        'LATAM',  35, 'compliant', FALSE, -23.55050, -46.63330),
    ('NG', 'Nigeria',       'AFRICA', 78, 'grey',      FALSE,   9.07650,   7.39860),
    ('RU', 'Russia',        'EU',     85, 'black',     FALSE,  55.75580,  37.61730),
    ('IR', 'Iran',          'MENA',   98, 'black',     TRUE,   35.68920,  51.38900),
    ('KP', 'North Korea',   'APAC',   99, 'black',     TRUE,   39.03920, 125.76250),
    ('PA', 'Panama',        'LATAM',  62, 'grey',      FALSE,   8.98230, -79.51980),
    ('CY', 'Cyprus',        'EU',     55, 'grey',      FALSE,  35.12640,  33.42990),
    ('AE', 'UAE',           'MENA',   40, 'compliant', FALSE,  24.45390,  54.37730);

-- ============================================================================
-- DONE — Reference data loaded
-- ============================================================================
