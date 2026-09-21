-- =============================================================================
-- Firmwide reference data (Delta) — used to CORRELATE streaming claims against
-- book-of-business exposure/premium in the gold layer (e.g. loss ratios).
-- Run in a Databricks SQL editor or notebook. Change the catalog if you renamed it.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS allianz_hackathon;
CREATE SCHEMA  IF NOT EXISTS allianz_hackathon.reference;

CREATE OR REPLACE TABLE allianz_hackathon.reference.firmwide_exposure (
    lob                   STRING  NOT NULL,
    region                STRING  NOT NULL,
    gross_written_premium DECIMAL(18,2),
    total_exposure        DECIMAL(18,2),
    policy_count          BIGINT,
    avg_premium           DECIMAL(12,2),
    target_loss_ratio     DECIMAL(5,4),   -- planning assumption per LOB/region
    as_of_date            DATE
)
COMMENT 'Firmwide book-of-business: premium & exposure by line of business and region';

-- Synthetic but internally-consistent firmwide book: every LOB x region combo,
-- premium scaled by an LOB base and a region multiplier so gold loss ratios vary.
INSERT OVERWRITE allianz_hackathon.reference.firmwide_exposure
WITH lob(lob, base_premium, target_lr) AS (
  VALUES ('Property', 90000000, 0.62),
         ('Motor',    120000000, 0.71),
         ('Liability', 60000000, 0.55),
         ('Marine',    35000000, 0.58),
         ('Health',    80000000, 0.78),
         ('Life',      70000000, 0.45)
),
region(region, mult) AS (
  VALUES ('Germany', 1.35), ('France', 1.15), ('Italy', 0.95), ('UK', 1.25),
         ('Spain', 0.85), ('Switzerland', 0.80), ('Austria', 0.60), ('Benelux', 0.90)
)
SELECT
  l.lob,
  r.region,
  CAST(l.base_premium * r.mult AS DECIMAL(18,2))                              AS gross_written_premium,
  CAST(l.base_premium * r.mult * (3 + rand()*2) AS DECIMAL(18,2))            AS total_exposure,
  CAST(round(l.base_premium * r.mult / (1500 + rand()*3000)) AS BIGINT)      AS policy_count,
  CAST(1500 + rand()*3000 AS DECIMAL(12,2))                                  AS avg_premium,
  CAST(l.target_lr AS DECIMAL(5,4))                                          AS target_loss_ratio,
  current_date()                                                             AS as_of_date
FROM lob l CROSS JOIN region r;

SELECT lob, count(*) AS regions, sum(gross_written_premium) AS gwp
FROM allianz_hackathon.reference.firmwide_exposure GROUP BY lob ORDER BY lob;
