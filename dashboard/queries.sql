-- =============================================================================
-- AI/BI Dashboard queries (over the gold layer). Each block backs one widget.
-- The .lvdash.json dashboard is built from these during setup (see README §7).
-- =============================================================================

-- KPI: total incurred + claim volume (last refresh)
-- widget: counters
SELECT
  sum(incurred_amount)              AS total_incurred,
  sum(claim_txn_count)              AS total_transactions,
  sum(distinct_claims)              AS distinct_claims,
  sum(large_loss_count)             AS large_losses
FROM allianz_hackathon.gold.gold_claims_by_lob_region;

-- Incurred amount by line of business
-- widget: bar chart (x=lob, y=incurred_amount)
SELECT lob, sum(incurred_amount) AS incurred_amount, sum(claim_txn_count) AS txns
FROM allianz_hackathon.gold.gold_claims_by_lob_region
GROUP BY lob ORDER BY incurred_amount DESC;

-- CORRELATION: observed loss ratio vs firmwide target, by LOB
-- widget: grouped bar / table with conditional formatting on vs_target
SELECT
  lob,
  round(sum(incurred_amount) / sum(gross_written_premium), 4) AS observed_loss_ratio,
  round(avg(target_loss_ratio), 4)                            AS target_loss_ratio,
  sum(gross_written_premium)                                  AS gross_written_premium,
  sum(incurred_amount)                                        AS incurred_amount
FROM allianz_hackathon.gold.gold_loss_ratio
GROUP BY lob ORDER BY observed_loss_ratio DESC;

-- Loss ratio heat by region x LOB
-- widget: heatmap / pivot table
SELECT lob, region, observed_loss_ratio, target_loss_ratio, vs_target
FROM allianz_hackathon.gold.gold_loss_ratio
ORDER BY observed_loss_ratio DESC;

-- Streaming trend: daily incurred by LOB
-- widget: line chart (x=event_date, y=incurred_amount, series=lob)
SELECT event_date, lob, incurred_amount, claim_txn_count
FROM allianz_hackathon.gold.gold_claims_daily
ORDER BY event_date, lob;

-- Freshness: newest event landing in silver (proves the 2-min stream is flowing)
-- widget: counter
SELECT
  max(event_ts)                                     AS latest_event,
  timestampdiff(SECOND, max(event_ts), now())       AS seconds_since_latest,
  count(*)                                          AS silver_rows
FROM allianz_hackathon.silver.claims;
