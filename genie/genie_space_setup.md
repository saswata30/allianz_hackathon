# Genie Space — "Allianz Claims Intelligence"

A Genie space lets business users ask natural-language questions over the streaming
claims data and the firmwide correlation. Set it up in the Databricks UI
(**Genie → New space**) or via the `/genie-rooms` skill.

## Tables to add
| Table | Why |
|---|---|
| `allianz_hackathon.silver.claims` | transaction-grain, clean, quality-checked claims |
| `allianz_hackathon.gold.gold_loss_ratio` | claims vs firmwide premium (loss ratio) |
| `allianz_hackathon.gold.gold_claims_by_lob_region` | pre-aggregated LOB × region |
| `allianz_hackathon.gold.gold_claims_daily` | daily streaming trend |
| `allianz_hackathon.reference.firmwide_exposure` | firmwide book of business |

## Space instructions (paste into the Genie "Instructions" box)
```
You are an analyst for Allianz P&C claims. Data streams from a live claims system
every ~2 minutes. "Incurred" means SUM(claim_amount). "Loss ratio" = incurred /
gross_written_premium, and should be compared to target_loss_ratio from the firmwide
book. LOB = line of business (Property, Motor, Liability, Marine, Health, Life).
A "large loss" is claim_amount >= 250,000. Always prefer the gold_* tables for
aggregates and joins; use silver.claims for transaction-level detail. Currency codes
are ISO (EUR, GBP, CHF). When asked about "firmwide" or "book", use firmwide_exposure.
```

## Sample questions (add as example queries)
- "What is the total incurred amount by line of business today?"
- "Which region has the highest loss ratio versus its target?"
- "Show the daily claim volume trend for Motor over the last week."
- "How many large losses (over 250k) have we seen, and in which LOBs?"
- "Which lines of business are running OVER their target loss ratio right now?"
- "What's the average reporting lag in days by region?"

## Curated SQL hints (optional — improves accuracy)
Add these as "SQL expressions"/example answers in the space:
```sql
-- Loss ratio vs target by LOB
SELECT lob,
       round(sum(incurred_amount)/sum(gross_written_premium),4) AS loss_ratio,
       round(avg(target_loss_ratio),4) AS target
FROM allianz_hackathon.gold.gold_loss_ratio GROUP BY lob;
```

## Create via CLI (alternative)
See the `/genie-rooms` skill for programmatic creation. Minimum: create the space,
attach the five tables above, paste the instructions, add the sample questions.
