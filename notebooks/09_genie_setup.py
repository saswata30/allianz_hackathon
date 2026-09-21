# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Genie space — "Allianz Claims Intelligence"
# MAGIC A Genie space lets business users ask natural-language questions over the streaming
# MAGIC claims and the firmwide correlation. Create it in the UI (**Genie → New space**),
# MAGIC then use the tables, instructions, and sample questions below.
# MAGIC
# MAGIC ## Tables to add
# MAGIC | Table | Why |
# MAGIC |---|---|
# MAGIC | `allianz_hackathon.silver.claims` | transaction-grain, clean, quality-checked claims |
# MAGIC | `allianz_hackathon.gold.gold_loss_ratio` | claims vs firmwide premium (loss ratio) |
# MAGIC | `allianz_hackathon.gold.gold_claims_by_lob_region` | pre-aggregated LOB × region |
# MAGIC | `allianz_hackathon.gold.gold_claims_daily` | daily streaming trend |
# MAGIC | `allianz_hackathon.reference.firmwide_exposure` | firmwide book of business |
# MAGIC
# MAGIC ## Instructions (paste into the Genie "Instructions" box)
# MAGIC ```
# MAGIC You are an analyst for Allianz P&C claims. Data streams from a live claims system
# MAGIC every ~2 minutes. "Incurred" means SUM(claim_amount). "Loss ratio" = incurred /
# MAGIC gross_written_premium, compared to target_loss_ratio from the firmwide book. LOB =
# MAGIC line of business (Property, Motor, Liability, Marine, Health, Life). A "large loss"
# MAGIC is claim_amount >= 250,000. Prefer the gold_* tables for aggregates/joins; use
# MAGIC silver.claims for transaction detail. Currencies are ISO (EUR, GBP, CHF).
# MAGIC ```
# MAGIC
# MAGIC ## Sample questions
# MAGIC - What is the total incurred amount by line of business today?
# MAGIC - Which region has the highest loss ratio versus its target?
# MAGIC - Show the daily claim volume trend for Motor over the last week.
# MAGIC - How many large losses (over 250k) have we seen, and in which LOBs?
# MAGIC - Which lines of business are running OVER their target loss ratio right now?
# MAGIC - What's the average reporting lag in days by region?

# COMMAND ----------

# Sanity-check that the tables Genie will use exist and have data.
for t in [
    "allianz_hackathon.silver.claims",
    "allianz_hackathon.gold.gold_loss_ratio",
    "allianz_hackathon.gold.gold_claims_by_lob_region",
    "allianz_hackathon.gold.gold_claims_daily",
    "allianz_hackathon.reference.firmwide_exposure",
]:
    try:
        print(f"{t:60} rows={spark.table(t).count()}")
    except Exception as e:
        print(f"{t:60} MISSING ({str(e).splitlines()[0][:60]})")
