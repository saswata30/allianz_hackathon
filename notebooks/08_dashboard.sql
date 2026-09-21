-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 · AI/BI Dashboard queries (gold layer)
-- MAGIC Run these to validate the gold layer, then use each block as a dataset when you
-- MAGIC build the dashboard: **Dashboards → Create**, add a dataset per query, add widgets,
-- MAGIC publish, and set auto-refresh to ~2 min to watch the stream move.

-- COMMAND ----------

-- MAGIC %md ## KPIs (counters)
-- COMMAND ----------
SELECT
  sum(incurred_amount)  AS total_incurred,
  sum(claim_txn_count)  AS total_transactions,
  sum(distinct_claims)  AS distinct_claims,
  sum(large_loss_count) AS large_losses
FROM allianz_hackathon.gold.gold_claims_by_lob_region;

-- COMMAND ----------
-- MAGIC %md ## Incurred by line of business (bar)
-- COMMAND ----------
SELECT lob, sum(incurred_amount) AS incurred_amount, sum(claim_txn_count) AS txns
FROM allianz_hackathon.gold.gold_claims_by_lob_region
GROUP BY lob ORDER BY incurred_amount DESC;

-- COMMAND ----------
-- MAGIC %md ## CORRELATION — observed loss ratio vs firmwide target, by LOB (bar/table)
-- COMMAND ----------
SELECT
  lob,
  round(sum(incurred_amount) / sum(gross_written_premium), 4) AS observed_loss_ratio,
  round(avg(target_loss_ratio), 4)                            AS target_loss_ratio,
  sum(gross_written_premium)                                  AS gross_written_premium,
  sum(incurred_amount)                                        AS incurred_amount
FROM allianz_hackathon.gold.gold_loss_ratio
GROUP BY lob ORDER BY observed_loss_ratio DESC;

-- COMMAND ----------
-- MAGIC %md ## Loss ratio heat by region × LOB (heatmap / pivot)
-- COMMAND ----------
SELECT lob, region, observed_loss_ratio, target_loss_ratio, vs_target
FROM allianz_hackathon.gold.gold_loss_ratio
ORDER BY observed_loss_ratio DESC;

-- COMMAND ----------
-- MAGIC %md ## Streaming trend — daily incurred by LOB (line)
-- COMMAND ----------
SELECT event_date, lob, incurred_amount, claim_txn_count
FROM allianz_hackathon.gold.gold_claims_daily
ORDER BY event_date, lob;

-- COMMAND ----------
-- MAGIC %md ## Freshness — proves the 2-min stream is flowing (counter)
-- COMMAND ----------
SELECT
  max(event_ts)                               AS latest_event,
  timestampdiff(SECOND, max(event_ts), now()) AS seconds_since_latest,
  count(*)                                    AS silver_rows
FROM allianz_hackathon.silver.claims;
