-- ═══════════════════════════════════════════════════════════════════════════════
-- Lab 3 (enrichment): Curated Diverse Case Narratives + Embeddings
-- ═══════════════════════════════════════════════════════════════════════════════
-- The persona generator (03) uses a handful of templated sentences per persona.
-- This file adds hand-written, varied analyst narratives so semantic search has
-- realistic linguistic diversity — paraphrases that share INTENT but not words.
-- None contain the literal terms "structuring", "card testing", or "bust-out",
-- which is exactly what makes vector search outperform keyword search.
--
-- Run AFTER 06_lab3_ai_analytics.sql (it appends to case_embeddings using the
-- same 32-dim feature layout, so the new vectors live in the same space).
-- ═══════════════════════════════════════════════════════════════════════════════
SET search_path TO bfsi_demo, public;

BEGIN;  -- keep the ON COMMIT DROP temp table alive across the inserts below

-- Curated narratives with their ground-truth persona + a representative account.
CREATE TEMP TABLE _curated (account_id BIGINT, queue TEXT, severity INT, persona TEXT, narrative TEXT) ON COMMIT DROP;

INSERT INTO _curated (account_id, queue, severity, persona, narrative) VALUES
-- ── STRUCTURING (sub-threshold layering / smurfing) ──────────────────────────
(105900003,'aml-tm',2,'structuring','Customer sends a steady stream of payments each landing a few hundred below the mandatory reporting line'),
(105900007,'aml-tm',2,'structuring','Beneficiary in a grey-list jurisdiction receives matching remittances spaced evenly through the day'),
(105900011,'aml-tm',1,'structuring','Funds enter and leave within minutes, hopping through the prepaid wallet before heading offshore'),
(105900015,'aml-tm',2,'structuring','Dozens of similar-sized outbound wires, none individually notable, same overseas counterparty'),
(105900019,'aml-tm',2,'structuring','Repeated remittances of comparable value to a payee already under enhanced monitoring'),
(105900023,'aml-tm',1,'structuring','Layered movement of value across linked accounts to obscure the ultimate destination'),
(105900027,'sanctions',1,'structuring','Outbound consulting-fee transfers to a counterparty in a high-risk corridor, amounts curiously uniform'),
(105900031,'aml-tm',2,'structuring','Pattern of just-below-limit disbursements suggests deliberate avoidance of the declaration threshold'),
-- ── BUST-OUT (run-up then walk-away) ─────────────────────────────────────────
(101900002,'cards-fraud',1,'bust_out','Newly raised limit consumed almost entirely within two days at a narrow set of merchants'),
(101900005,'cards-fraud',1,'bust_out','Balance ramped to the ceiling on luxury goods, then the account went dark with no repayment'),
(101900009,'cards-fraud',2,'bust_out','Aggressive spend-up on resaleable electronics immediately preceding a returned payment'),
(101900013,'cards-fraud',1,'bust_out','Account behaved perfectly for months then drained the full line in a single weekend'),
(101900017,'cards-fraud',2,'bust_out','Concentrated high-ticket purchases at two outlets, repayment bounced, written off as loss'),
(101900021,'cards-fraud',1,'bust_out','Sudden burst of big-value transactions exhausting available credit before charge-off'),
(101900024,'disputes',2,'bust_out','Cardholder maxed the account on jewellery, then unreachable; merchant goods non-recoverable'),
-- ── CARD-TESTING (validation probing) ────────────────────────────────────────
(100900004,'cards-fraud',1,'card_testing','Flood of trivial online charges fanning out to a vast list of unrelated storefronts'),
(100900008,'cards-fraud',2,'card_testing','Repeated penny authorisations checking which card numbers still authorise'),
(100900012,'cards-fraud',1,'card_testing','Rapid stream of tiny e-commerce attempts with most being rejected by the issuer'),
(100900016,'cards-fraud',2,'card_testing','Hundreds of distinct merchant IDs touched in minutes with sub-dollar amounts'),
(100900020,'cards-fraud',1,'card_testing','Automated probing pattern: micro-charges at machine speed across many sellers'),
(100900025,'cards-fraud',2,'card_testing','High volume of low-value verification hits far above this cardholder''s normal behaviour'),
(100900029,'cards-fraud',1,'card_testing','Burst of small declines followed by one larger successful purchase attempt'),
-- ── NORMAL (legitimate) ──────────────────────────────────────────────────────
(100012345,'disputes',4,'normal','Cardholder confirmed the recurring subscription charge was expected, case closed'),
(100023456,'cards-fraud',5,'normal','Foreign point-of-sale activity matched a logged travel notification, no concern'),
(100034567,'aml-tm',4,'normal','Salary credit and scheduled mortgage debit consistent with historical pattern');

-- Persist into the real case_narratives table (gets sequence note_ids)
INSERT INTO case_narratives (ts, account_id, card_bin, analyst, queue, severity, narrative, region_id)
SELECT ('2026-06-10'::timestamp + random()*interval '20 days'), account_id, NULL, 'workshop-curated',
       queue, severity, narrative, (1+(random()*6)::int)
FROM _curated;

-- Embed ONLY the curated rows into case_embeddings, using the SAME 32-dim layout
-- as Lab 3 Step 1.3 so cosine distance is comparable across all notes.
INSERT INTO case_embeddings (note_id, account_id, card_bin, analyst, queue, severity, narrative, persona, embedding)
SELECT
    n.note_id, n.account_id, n.card_bin, n.analyst, n.queue, n.severity, left(n.narrative,300), c.persona,
    ARRAY[
      n.severity::float/5.0,
      CASE WHEN n.queue='cards-fraud' THEN 1 ELSE 0 END,
      CASE WHEN n.queue='aml-tm' THEN 1 ELSE 0 END,
      CASE WHEN n.queue='sanctions' THEN 1 ELSE 0 END,
      CASE WHEN n.queue='disputes' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%transfer%' OR n.narrative ILIKE '%wire%' OR n.narrative ILIKE '%remittance%' OR n.narrative ILIKE '%outbound%' OR n.narrative ILIKE '%disbursement%' OR n.narrative ILIKE '%payment%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%threshold%' OR n.narrative ILIKE '%below%' OR n.narrative ILIKE '%just-below%' OR n.narrative ILIKE '%reporting line%' OR n.narrative ILIKE '%declaration%' OR n.narrative ILIKE '%limit%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%layer%' OR n.narrative ILIKE '%hopping%' OR n.narrative ILIKE '%offshore%' OR n.narrative ILIKE '%corridor%' OR n.narrative ILIKE '%obscure%' OR n.narrative ILIKE '%overseas%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%limit%' OR n.narrative ILIKE '%maxed%' OR n.narrative ILIKE '%drained%' OR n.narrative ILIKE '%ceiling%' OR n.narrative ILIKE '%consumed%' OR n.narrative ILIKE '%ramped%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%luxury%' OR n.narrative ILIKE '%high-ticket%' OR n.narrative ILIKE '%jewellery%' OR n.narrative ILIKE '%electronics%' OR n.narrative ILIKE '%big-value%' OR n.narrative ILIKE '%high-value%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%returned%' OR n.narrative ILIKE '%written off%' OR n.narrative ILIKE '%charge-off%' OR n.narrative ILIKE '%bounced%' OR n.narrative ILIKE '%non-recoverable%' OR n.narrative ILIKE '%loss%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%trivial%' OR n.narrative ILIKE '%penny%' OR n.narrative ILIKE '%tiny%' OR n.narrative ILIKE '%sub-dollar%' OR n.narrative ILIKE '%micro%' OR n.narrative ILIKE '%small%' OR n.narrative ILIKE '%low-value%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%declin%' OR n.narrative ILIKE '%reject%' OR n.narrative ILIKE '%verification%' OR n.narrative ILIKE '%authoris%' OR n.narrative ILIKE '%charge%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%merchant%' OR n.narrative ILIKE '%storefront%' OR n.narrative ILIKE '%seller%' OR n.narrative ILIKE '%MID%' OR n.narrative ILIKE '%probing%' OR n.narrative ILIKE '%volume%' OR n.narrative ILIKE '%machine speed%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%beneficiary%' OR n.narrative ILIKE '%payee%' OR n.narrative ILIKE '%counterparty%' OR n.narrative ILIKE '%monitoring%' OR n.narrative ILIKE '%watch%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%high-risk%' OR n.narrative ILIKE '%grey-list%' OR n.narrative ILIKE '%jurisdiction%' OR n.narrative ILIKE '%corridor%' OR n.narrative ILIKE '%offshore%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%dispute%' OR n.narrative ILIKE '%chargeback%' OR n.narrative ILIKE '%subscription%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%kyc%' OR n.narrative ILIKE '%salary%' OR n.narrative ILIKE '%mortgage%' OR n.narrative ILIKE '%scheduled%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%confirmed%' OR n.narrative ILIKE '%expected%' OR n.narrative ILIKE '%no concern%' OR n.narrative ILIKE '%consistent%' OR n.narrative ILIKE '%closed%' THEN 1 ELSE 0 END,
      CASE WHEN n.narrative ILIKE '%scheduled%' OR n.narrative ILIKE '%recurring%' OR n.narrative ILIKE '%standing%' THEN 1 ELSE 0 END,
      CASE WHEN n.severity <= 1 THEN 1 ELSE 0 END,
      CASE WHEN n.severity = 3 THEN 1 ELSE 0 END,
      CASE WHEN n.severity = 4 THEN 1 ELSE 0 END,
      CASE WHEN n.queue='disputes' THEN 1 ELSE 0 END,
      random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05
    ]::vector(32)
FROM case_narratives n
JOIN _curated c ON c.narrative = n.narrative
WHERE n.analyst = 'workshop-curated';

ANALYZE case_embeddings;

COMMIT;  -- _curated is dropped here; curated rows are now persisted

-- Quick check: curated rows landed and are searchable
SELECT persona, count(*) AS curated_notes
FROM case_embeddings
WHERE note_id IN (SELECT note_id FROM case_narratives WHERE analyst='workshop-curated')
GROUP BY 1 ORDER BY 2 DESC;
