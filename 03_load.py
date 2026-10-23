# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Load (Silver -> Gold)
# MAGIC Standardizes store + web sales, combines them, enriches with products/locations/FX, calculates USD revenue and profit, and writes one CSV.

# COMMAND ----------

# ---------- Imports ----------
import glob
import os
from datetime import datetime
import numpy as np                   # np.where for conditional columns
import pandas as pd

# COMMAND ----------

# ---------- Paths (same block in every notebook) ----------
# Note the capital "V" in /Volumes: the path is case-sensitive.
VOLUME_DIR = "/Volumes/etl_meridian_project/etl_meridianschema/etl_meridianvolume"

RAW_DIR    = f"{VOLUME_DIR}/raw"         # landing zone: teams drop their exports here
BRONZE_DIR = f"{VOLUME_DIR}/bronze"      # extracted as-is (all text), Parquet
SILVER_DIR = f"{VOLUME_DIR}/silver"      # cleaned tables, Parquet
OUTPUT_DIR = f"{VOLUME_DIR}/processed"   # final Gold file
LOG_DIR    = f"{VOLUME_DIR}/logs"        # run logs

LOG_FILE = f"{LOG_DIR}/etl_log.txt"

for folder in [SILVER_DIR, OUTPUT_DIR, LOG_DIR]:
    os.makedirs(folder, exist_ok=True)

# COMMAND ----------

# ---------- Logging (same function in every notebook) ----------
def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} | {message}"
    with open(LOG_FILE, "a") as f:     # "a" = append: keeps the history of every run
        f.write(line + "\n")
    print(line)                        # live feedback in the notebook / Job run output

# COMMAND ----------

# ---------- Read Silver: one Parquet file per source ----------
silver = {}
for file in glob.glob(f"{SILVER_DIR}/*.parquet"):
    name = os.path.basename(file).replace(".parquet", "")   # "store_sales.parquet" -> "store_sales"
    silver[name] = pd.read_parquet(file)

for name, df in silver.items():
    print(name, df.shape)

# COMMAND ----------

# ---------- 1. standardize ----------

def standardize_store_sales(df):
    return pd.DataFrame({
        "sale_id":          df["transaction_id"] + "-" + df["line_no"],
        "sale_date":        df["transaction_datetime"].dt.normalize(),
        "location_id":      df["location_id"],
        "product_id":       df["product_id"],
        "quantity":         df["quantity"],
        "unit_price_local": df["unit_price"],
        "discount_rate":    df["discount_pct"] / 100,
        "currency":         df["currency"],
        "status":           np.where(df["quantity"] < 0, "returned", "completed"),
    })


def standardize_web_orders(df):
    return pd.DataFrame({
        "sale_id":          df["order_id"] + "-" + df["product_id"],
        "sale_date":        df["order_ts"].dt.tz_convert(None).dt.normalize(),
        "location_id":      df["storefront"],
        "product_id":       df["product_id"],
        "quantity":         df["qty"],
        "unit_price_local": df["unit_price"],
        "discount_rate":    df["line_discount"],
        "currency":         df["currency"],
        "status":           df["status"],
        "country":          df["customer_country"],   # where the customer is
    })
# COMMAND ----------

# ---------- 2. combine ----------

def combine(store, web):
    sales = pd.concat([store, web], ignore_index=True)
    log_progress(f"Combined: {len(store)} store + {len(web)} web = {len(sales)} sales")
    return sales
# COMMAND ----------

# ---------- 3. enrich ----------

def enrich(sales, products, locations, fx_rates):
    sales = sales.merge(
        locations[["location_id", "channel", "region", "country"]]
            .rename(columns={"country": "location_country"}),
        on="location_id", how="left")
    # Store sales: country = the store's country. Web sales already have the customer's country.
    sales["country"] = sales["country"].fillna(sales["location_country"])

    sales = sales.merge(
        products[["product_id", "name", "category", "unit_cost"]],
        on="product_id", how="left")

    sales = sales.merge(
        fx_rates.rename(columns={"rate_date": "sale_date", "rate_to_usd": "fx_rate"}),
        on=["sale_date", "currency"], how="left")

    unknown = sales["name"].isna().sum()
    sales["name"] = sales["name"].fillna("Unknown product")
    sales["category"] = sales["category"].fillna("Unknown")
    log_progress(f"Enrich: {unknown} sales with a product_id not in the catalog")
    return sales
# COMMAND ----------

# ---------- 4. calculate ----------

def calculate(sales):
    # Business rule: refunded and cancelled orders earn nothing
    counts = sales["status"].isin(["completed", "returned"])

    revenue = sales["quantity"] * sales["unit_price_local"] * (1 - sales["discount_rate"]) * sales["fx_rate"]
    cost    = sales["quantity"] * sales["unit_cost"]

    sales["revenue_usd"] = np.where(counts, revenue, 0).round(2)
    sales["cost_usd"]    = np.where(counts, cost, 0).round(2)
    sales["profit_usd"]  = (sales["revenue_usd"] - sales["cost_usd"]).round(2)
    return sales
# COMMAND ----------

# ---------- 5. final shape ----------

GOLD_COLUMNS = [
    "sale_id", "sale_date", "channel", "region", "country", "location_id",
    "product_id", "name", "category", "status", "quantity",
    "currency", "unit_price_local", "fx_rate",
    "revenue_usd", "cost_usd", "profit_usd",
]

def build_gold(silver):
    store = standardize_store_sales(silver["store_sales"])
    web   = standardize_web_orders(silver["web_orders"])
    sales = combine(store, web)
    sales = enrich(sales, silver["products"], silver["locations"], silver["fx_rates"])
    sales = calculate(sales)
    return sales[GOLD_COLUMNS].rename(columns={"name": "product_name"})

# COMMAND ----------

# ---------- Run and load the Gold file ----------
TARGET_FILE = f"{OUTPUT_DIR}/sales_consolidated.csv"

log_progress("Load phase Started")
gold = build_gold(silver)
gold.to_csv(TARGET_FILE, index=False)     # index=False: no junk index column in the CSV
log_progress(f"Saved {len(gold)} rows to {TARGET_FILE}")
log_progress("Load phase Ended")
log_progress("ETL Job Ended")

display(gold)
