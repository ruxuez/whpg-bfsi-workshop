-- ============================================================================
-- Meridian Retail Bank × EDB WarehousePG — Persona-Based Transaction Generation
-- ============================================================================
-- Creates ~13M transactions (in-DB path) with FOUR personas across June 2026, plus
-- aligned case narratives. Each transaction carries an ISO 20022 iso_msg JSONB
-- so the SAME table powers Lab 1 (JSONB/GIN) and Lab 3 (clustering).
--
--   • NORMAL        (~62%) — baseline retail spend
--   • CARD-TESTING  (~15%) — micro-auths across MANY merchants  (≈ RECON)
--   • BUST-OUT      (~10%) — huge spend, FEW merchants          (≈ EXFIL)
--   • STRUCTURING   (~13%) — near-identical sub-threshold wires  (≈ C2)
--
-- Persona feature signatures (what K-Means separates in Lab 3):
--   CARD-TESTING : distinct_merchants HIGH, amount TINY, merchant_entropy HIGH
--   BUST-OUT     : total_amount EXTREME, distinct_merchants LOW, entropy LOW
--   STRUCTURING  : amount_cv VERY LOW (near-identical wires), entropy LOW
--
-- Tunable scale: edit the generate_series counts below. Defaults run in a few
-- minutes on a small WHPG cluster. Data is seeded across JUNE 2026
-- (2026-06-01 .. 2026-07-01) so the fixed June demo windows always return rows.
-- ============================================================================
SET search_path TO bfsi_demo, public;
SET statement_mem = '512MB';

DO $$ BEGIN RAISE NOTICE '[%] Truncating fact tables...', clock_timestamp(); END $$;
TRUNCATE transactions;
TRUNCATE case_narratives;
TRUNCATE auth_decisions;
TRUNCATE device_events;
TRUNCATE wire_events;
TRUNCATE account_kpis;
TRUNCATE fraud_cases;

-- Helper expressions reused below:
--   ts window: 2026-06-01 .. 2026-07-01 (June 2026)
--   merchant names / BICs / countries chosen per persona

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ PERSONA 1: NORMAL RETAIL SPEND (~8M rows, 8 batches)                     ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] NORMAL spend — 8M rows...', clock_timestamp(); END $$;

DO $$
DECLARE batch INT;
BEGIN
  FOR batch IN 1..8 LOOP
    IF batch % 2 = 0 THEN RAISE NOTICE '[%] normal batch %/8 ...', clock_timestamp(), batch; END IF;
    INSERT INTO transactions
        (ts, account_id, card_bin, pan_last4, amount, currency, mcc, merchant_id, merchant_name,
         merchant_country, channel, txn_type, auth_response, beneficiary_account, beneficiary_country,
         iso_msg, region_id)
    SELECT
        ('2026-06-01'::timestamp + random() * interval '30 days'),
        -- legit account ids spread across portfolio bands
        (100000000 + (random() * 49999)::bigint),   -- 50k normal accounts (not 9M singletons)
        -- legit BINs (avoid compromised sub-bands)
        (ARRAY[41015000,42025000,45055000,52025000,53035000,37005000,60115000,43035000])[1+(random()*7)::int]::bigint,
        lpad((random()*9999)::int::text, 4, '0'),
        ROUND((3 + random() * 800)::numeric, 2),                      -- moderate ticket
        (ARRAY['USD','USD','USD','EUR','GBP','JPY'])[1+(random()*5)::int],
        (ARRAY[5411,5812,5814,5541,5912,5732,4111,5999,5651,7011,4814,5944])[1+(random()*11)::int],
        (1 + (random()*50000)::bigint),                               -- broad merchant pool
        (ARRAY['Walmart','Starbucks','Shell','Amazon','Tesco','Uber','Carrefour','Apple Store','IKEA','Netflix'])[1+(random()*9)::int],
        (ARRAY['US','US','US','GB','DE','FR','JP','SG'])[1+(random()*7)::int],
        (ARRAY['POS','POS','ECOM','ECOM','ATM'])[1+(random()*4)::int],
        'purchase',
        CASE WHEN random() < 0.97 THEN '00' ELSE (ARRAY['05','51','14'])[1+(random()*2)::int] END,
        NULL, NULL,
        jsonb_build_object(
            'GrpHdr', jsonb_build_object('MsgId','MSG'||nextval('transactions_txn_id_seq')::text,'SttlmInf',jsonb_build_object('SttlmMtd','CLRG')),
            'PmtId',  jsonb_build_object('EndToEndId','E2E'),
            'PmtTpInf', jsonb_build_object(
                 'SvcLvl', jsonb_build_object('Cd', (ARRAY['CARD','CARD','SEPA','NURG'])[1+(random()*3)::int]),
                 'LclInstrm', jsonb_build_object('Cd', (ARRAY['POS','ECOM','ATM'])[1+(random()*2)::int])),
            'IntrBkSttlmAmt', jsonb_build_object('Ccy','USD','value', ROUND((3 + random()*800)::numeric,2)),
            'Dbtr', jsonb_build_object('Nm','Meridian Cardholder','CtryOfRes','US'),
            'Cdtr', jsonb_build_object('Nm','Retail Merchant','CtryOfRes', (ARRAY['US','GB','DE','FR'])[1+(random()*3)::int]),
            'CdtrAgt', jsonb_build_object('FinInstnId', jsonb_build_object('BICFI', (ARRAY['CHASUS33','BARCGB22','DEUTDEFF','BNPAFRPP'])[1+(random()*3)::int])),
            'RmtInf', jsonb_build_object('Ustrd','Card purchase')
        ),
        (1 + (random()*6)::int)
    FROM generate_series(1, 1000000);
  END LOOP;
END $$;

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ PERSONA 2: CARD-TESTING (~2M rows)  ≈ RECON                              ║
-- ║   small pool of compromised accounts, micro-auths across MANY merchants ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] CARD-TESTING — 2M rows...', clock_timestamp(); END $$;

INSERT INTO transactions
    (ts, account_id, card_bin, pan_last4, amount, currency, mcc, merchant_id, merchant_name,
     merchant_country, channel, txn_type, auth_response, beneficiary_account, beneficiary_country, iso_msg, region_id)
SELECT
    ('2026-06-01'::timestamp + random() * interval '30 days') + ((i % 1000) * interval '3 seconds'),
    -- tiny account pool so they aggregate into few accounts
    (100900001 + (random()*39)::bigint),
    -- COMPROMISED Visa CAMS BIN band (matches fraud_watchlists)
    (41010100 + (random()*99)::bigint),
    lpad((random()*9999)::int::text, 4, '0'),
    ROUND((0.40 + random() * 3.60)::numeric, 2),                      -- TINY auth probes
    'USD',
    (ARRAY[5999,5816,4816,5967,7995,5969,5734])[1+(random()*6)::int], -- high-risk MCCs, wide spread
    (1 + (random()*200000)::bigint),                                  -- VERY MANY distinct merchants
    'TEST-MERCHANT',
    (ARRAY['US','GB','PA','CY'])[1+(random()*3)::int],
    'ECOM',
    'purchase',
    CASE WHEN random() < 0.55 THEN (ARRAY['05','14','51','54'])[1+(random()*3)::int] ELSE '00' END, -- many declines
    NULL, NULL,
    jsonb_build_object(
        'GrpHdr', jsonb_build_object('MsgId','MSG'||nextval('transactions_txn_id_seq')::text,'SttlmInf',jsonb_build_object('SttlmMtd','CLRG')),
        'PmtTpInf', jsonb_build_object('SvcLvl', jsonb_build_object('Cd','CARD'),'LclInstrm', jsonb_build_object('Cd','ECOM')),
        'IntrBkSttlmAmt', jsonb_build_object('Ccy','USD','value', ROUND((0.40+random()*3.60)::numeric,2)),
        'Cdtr', jsonb_build_object('Nm','TEST-MERCHANT','CtryOfRes','US'),
        'RmtInf', jsonb_build_object('Ustrd','AUTH ONLY')
    ),
    (1 + (random()*6)::int)
FROM generate_series(1, 2000000) i;

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ PERSONA 3: BUST-OUT (~1.3M rows)  ≈ EXFIL                                ║
-- ║   small pool of accounts, HUGE spend over FEW merchants before default   ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] BUST-OUT — 1.3M rows...', clock_timestamp(); END $$;

INSERT INTO transactions
    (ts, account_id, card_bin, pan_last4, amount, currency, mcc, merchant_id, merchant_name,
     merchant_country, channel, txn_type, auth_response, beneficiary_account, beneficiary_country, iso_msg, region_id)
SELECT
    ('2026-06-01'::timestamp + random() * interval '30 days'),
    (101900001 + (random()*29)::bigint),                              -- tiny account pool
    (52020200 + (random()*99)::bigint),                               -- COMPROMISED MC ADC band
    lpad((random()*9999)::int::text, 4, '0'),
    ROUND((800 + random() * 4200)::numeric, 2),                       -- HUGE tickets
    'USD',
    (ARRAY[5944,5732,5651,5311])[1+(random()*3)::int],                -- few categories (jewelry/electronics)
    (700001 + (random()*8)::bigint),                                  -- ONLY ~9 merchants (low entropy)
    'LUXURY-GOODS',
    'US',
    (ARRAY['POS','ECOM'])[1+(random()*1)::int],
    'purchase',
    '00',
    NULL, NULL,
    jsonb_build_object(
        'GrpHdr', jsonb_build_object('MsgId','MSG'||nextval('transactions_txn_id_seq')::text,'SttlmInf',jsonb_build_object('SttlmMtd','CLRG')),
        'PmtTpInf', jsonb_build_object('SvcLvl', jsonb_build_object('Cd','CARD'),'LclInstrm', jsonb_build_object('Cd','POS')),
        'IntrBkSttlmAmt', jsonb_build_object('Ccy','USD','value', ROUND((800+random()*4200)::numeric,2)),
        'Cdtr', jsonb_build_object('Nm','LUXURY-GOODS','CtryOfRes','US'),
        'RmtInf', jsonb_build_object('Ustrd','High-value purchase')
    ),
    (1 + (random()*6)::int)
FROM generate_series(1, 1300000) i;

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ PERSONA 4: STRUCTURING (~1.7M rows)  ≈ C2 BEACONING                      ║
-- ║   regular near-identical sub-threshold wires to mule accounts            ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] STRUCTURING — 1.7M rows...', clock_timestamp(); END $$;

INSERT INTO transactions
    (ts, account_id, card_bin, pan_last4, amount, currency, mcc, merchant_id, merchant_name,
     merchant_country, channel, txn_type, auth_response, beneficiary_account, beneficiary_country, iso_msg, region_id)
SELECT
    ('2026-06-01'::timestamp + random() * interval '30 days') + ((i % 96) * interval '15 minutes'),  -- periodic cadence
    (105900001 + (random()*39)::bigint),
    (53030300 + (random()*99)::bigint),                               -- prepaid program band (velocity_abuse)
    lpad((random()*9999)::int::text, 4, '0'),
    ROUND((9000 + random() * 900)::numeric, 2),                       -- TIGHT band -> very low amount_cv
    'USD',
    6012,                                                              -- financial institution / transfer
    NULL,
    'WIRE-TRANSFER',
    (ARRAY['BR','NG','RU','PA','CY'])[1+(random()*4)::int],            -- high-risk corridors
    (ARRAY['WIRE','INST','P2P'])[1+(random()*2)::int],
    'transfer',
    '00',
    (ARRAY[108500001,108500002,105500050])[1+(random()*2)::int]::bigint, -- KNOWN MULE accounts
    (ARRAY['BR','NG','RU'])[1+(random()*2)::int],
    jsonb_build_object(
        'GrpHdr', jsonb_build_object('MsgId','MSG'||nextval('transactions_txn_id_seq')::text,'SttlmInf',jsonb_build_object('SttlmMtd','INGA')),
        'PmtTpInf', jsonb_build_object('SvcLvl', jsonb_build_object('Cd','SEPA'),'LclInstrm', jsonb_build_object('Cd','INST'),'CtgyPurp', jsonb_build_object('Cd','CASH')),
        'IntrBkSttlmAmt', jsonb_build_object('Ccy','USD','value', ROUND((9000+random()*900)::numeric,2)),
        'Dbtr', jsonb_build_object('Nm','Meridian Cardholder','CtryOfRes','US'),
        'Cdtr', jsonb_build_object('Nm','Beneficiary Co','CtryOfRes', (ARRAY['BR','NG','RU'])[1+(random()*2)::int]),
        'CdtrAgt', jsonb_build_object('FinInstnId', jsonb_build_object('BICFI', (ARRAY['BRASBRRJ','FBNINGLA','SABRRUMM'])[1+(random()*2)::int])),
        'RmtInf', jsonb_build_object('Ustrd','Consulting fee')
    ),
    (1 + (random()*6)::int)
FROM generate_series(1, 1700000) i;

ANALYZE transactions;
DO $$ BEGIN RAISE NOTICE '[%] transactions COMPLETE (~13M rows).', clock_timestamp(); END $$;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ CASE NARRATIVES — persona-aligned free text (drives Lab 3 semantic search)║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] Generating case_narratives — ~1.6M rows...', clock_timestamp(); END $$;

DO $$
DECLARE batch INT;
BEGIN
  FOR batch IN 1..4 LOOP
    INSERT INTO case_narratives (ts, account_id, card_bin, analyst, queue, severity, narrative, region_id)
    SELECT
        ('2026-06-01'::timestamp + random() * interval '30 days'),
        -- account_id TIED to persona, so cluster-flagged accounts have matching notes
        CASE persona
            WHEN 'bust_out'     THEN 101900001 + (random()*29)::bigint
            WHEN 'structuring'  THEN 105900001 + (random()*39)::bigint
            WHEN 'card_testing' THEN 100900001 + (random()*39)::bigint
            ELSE                     100000000 + (random()*49999)::bigint
        END,
        CASE persona
            WHEN 'bust_out'     THEN 52020200 + (random()*99)::bigint
            WHEN 'structuring'  THEN 53030300 + (random()*99)::bigint
            WHEN 'card_testing' THEN 41010100 + (random()*99)::bigint
            ELSE                     (ARRAY[41015000,52025000,53035000])[1+(random()*2)::int]::bigint
        END,
        (ARRAY['j.ng','a.silva','m.kovac','t.chen','r.adeyemi'])[1+(random()*4)::int],
        CASE persona
            WHEN 'structuring'  THEN 'aml-tm'
            WHEN 'bust_out'     THEN 'cards-fraud'
            WHEN 'card_testing' THEN 'cards-fraud'
            ELSE                     (ARRAY['disputes','sanctions','cards-fraud'])[1+(random()*2)::int]
        END,
        CASE WHEN random()<0.10 THEN 1 WHEN random()<0.30 THEN 3 WHEN random()<0.6 THEN 4 ELSE 5 END,
        CASE persona
            -- BUST-OUT narratives — never literally say "bust-out"
            WHEN 'bust_out' THEN (ARRAY[
                'Account drew down full credit line across a handful of merchants in 48h',
                'Sudden spike in high-value purchases at jewellery and electronics outlets',
                'Customer maxed limit then payment returned NSF — balance written off',
                'Rapid utilisation to 100% followed by silence and missed minimum payment',
                'Large-ticket spend concentrated at two merchants right after limit increase'
            ])[1+(random()*4)::int]
            -- STRUCTURING narratives — never say "structuring"
            WHEN 'structuring' THEN (ARRAY[
                'Repeated outbound transfers just under reporting threshold to overseas payee',
                'Series of near-identical 9k wires to the same beneficiary every few hours',
                'Round-tripping funds through prepaid program to high-risk corridor',
                'Consistent sub-10k remittances, beneficiary on internal watch',
                'Layered transfers of similar value to Brazil and Nigeria accounts'
            ])[1+(random()*4)::int]
            -- CARD-TESTING narratives — never say "card testing"
            WHEN 'card_testing' THEN (ARRAY[
                'Burst of sub-dollar authorisations across hundreds of unrelated merchants',
                'High decline rate on micro-amount e-commerce attempts from one card',
                'Rapid-fire small approvals then a single large purchase attempt',
                'Many 1.00 verification charges hitting distinct MIDs in minutes',
                'Velocity of tiny online auths far exceeds cardholder baseline'
            ])[1+(random()*4)::int]
            -- NORMAL narratives
            ELSE (ARRAY[
                'Routine purchase verified by cardholder, no action',
                'Dispute opened for duplicate charge, merchant credited',
                'Travel notification logged, foreign POS expected',
                'Address verification passed, order released',
                'Chargeback represented with compelling evidence',
                'KYC refresh completed, documents on file',
                'Standing order executed as scheduled'
            ])[1+(random()*6)::int]
        END,
        (1 + (random()*6)::int)
    FROM (
        SELECT CASE
                 WHEN r < 0.10 THEN 'bust_out'
                 WHEN r < 0.23 THEN 'structuring'
                 WHEN r < 0.38 THEN 'card_testing'
                 ELSE 'normal'
               END AS persona
        FROM (SELECT random() AS r FROM generate_series(1, 400000)) g
    ) s;
  END LOOP;
END $$;

ANALYZE case_narratives;
DO $$ BEGIN RAISE NOTICE '[%] case_narratives COMPLETE (~1.6M rows).', clock_timestamp(); END $$;


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ ACCOUNT KPIs — per-customer behavioural series (limit/threshold UC)      ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] Generating account_kpis...', clock_timestamp(); END $$;

INSERT INTO account_kpis (ts, customer_id, region_id, txn_velocity, avg_ticket, decline_rate_pct, fraud_bps, chargeback_rate_pct)
SELECT
    '2026-06-30 23:00:00'::timestamp - (g * interval '1 hour'),
    c.customer_id,
    c.region_id,
    ROUND((20 + random()*220)::numeric, 2),
    ROUND((50 + random()*1500)::numeric, 2),
    ROUND((random()*12)::numeric, 2),
    ROUND((random()*60)::numeric, 2),
    ROUND((random()*3)::numeric, 2)
FROM customers c, generate_series(0, 47) g;

ANALYZE account_kpis;

-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║ AUTH DECISIONS + DEVICE EVENTS — for cross-source correlation            ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
DO $$ BEGIN RAISE NOTICE '[%] Generating auth_decisions + device_events...', clock_timestamp(); END $$;

INSERT INTO auth_decisions (ts, account_id, card_bin, mcc, amount, decision, rule_id, channel, merchant_country, region_id)
SELECT
    ('2026-06-01'::timestamp + random() * interval '30 days'),
    (ARRAY[100900001,101900001,105900001,100000500])[1+(random()*3)::int]::bigint + (random()*30)::bigint,
    (ARRAY[41010150,52020250,53030350,41015000])[1+(random()*3)::int]::bigint,
    (ARRAY[5999,5944,6012,5411])[1+(random()*3)::int],
    ROUND((random()*5000)::numeric,2),
    (ARRAY['APPROVE','DECLINE','DECLINE','STEP_UP','BLOCK'])[1+(random()*4)::int],
    (100+(random()*50)::int),
    (ARRAY['ECOM','POS','WIRE'])[1+(random()*2)::int],
    (ARRAY['US','BR','NG','PA'])[1+(random()*3)::int],
    (1+(random()*6)::int)
FROM generate_series(1, 500000);

INSERT INTO device_events (ts, account_id, device_fingerprint, ip_country, channel, event_type, result, region_id)
SELECT
    ('2026-06-01'::timestamp + random() * interval '30 days'),
    (ARRAY[100900001,101900001,105900001,100000500])[1+(random()*3)::int]::bigint + (random()*30)::bigint,
    md5(random()::text),
    (ARRAY['US','RU','NG','BR','PA'])[1+(random()*4)::int],
    (ARRAY['ECOM','MOBILE','WEB'])[1+(random()*2)::int],
    (ARRAY['login','payee_add','pwd_reset','new_device','login'])[1+(random()*4)::int],
    (ARRAY['OK','OK','FAIL','FLAG'])[1+(random()*3)::int],
    (1+(random()*6)::int)
FROM generate_series(1, 400000);

ANALYZE auth_decisions;
ANALYZE device_events;

DO $$ BEGIN RAISE NOTICE '[%] ═══════════════════════════════════════════════', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%] PERSONA-BASED GENERATION COMPLETE', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%]   NORMAL ~8.0M | CARD-TESTING ~2.0M | BUST-OUT ~1.3M | STRUCTURING ~1.7M', clock_timestamp(); END $$;
DO $$ BEGIN RAISE NOTICE '[%] ═══════════════════════════════════════════════', clock_timestamp(); END $$;
