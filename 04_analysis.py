# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Analysis (Gold -> business answers)
# MAGIC Answers the CEO's question: what did we sell and earn, by channel, country and category?

# COMMAND ----------

import pandas as pd

VOLUME_DIR = "/Volumes/etl_meridian_project/etl_meridianschema/etl_meridianvolume"
gold = pd.read_csv(f"{VOLUME_DIR}/processed/sales_consolidated.csv")

# COMMAND ----------

# One reusable summary: group by any column, sum revenue and profit, add margin %
def profit_summary(by):
    t = gold.groupby(by).agg(
        sales       = ("sale_id", "count"),
        revenue_usd = ("revenue_usd", "sum"),
        profit_usd  = ("profit_usd", "sum"),
    )
    t["margin_pct"] = (t["profit_usd"] / t["revenue_usd"] * 100).round(1)
    return t.round(2).sort_values("profit_usd", ascending=False)

# COMMAND ----------

# Tip: click "+" above each output -> Visualization to turn it into a bar chart
display(profit_summary("channel"))
display(profit_summary("country"))
display(profit_summary("category"))
