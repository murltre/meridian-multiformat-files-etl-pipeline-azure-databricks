# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Transform (Bronze -> Silver)
# MAGIC Cleans each source with its own function, then saves the cleaned tables to `silver/`.

# COMMAND ----------

# ---------- Imports ----------
import glob
import os
from datetime import datetime
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

# ---------- Read Bronze: one Parquet file per source ----------
bronze = {}
for file in glob.glob(f"{BRONZE_DIR}/*.parquet"):
    name = os.path.basename(file).replace(".parquet", "")   # "store_sales.parquet" -> "store_sales"
    bronze[name] = pd.read_parquet(file)

for name, df in bronze.items():
    print(name, df.shape)

# COMMAND ----------

# ---------- store sales (CSV) ----------

def clean_store_sales(df, locations):
    df = df.copy()

    # Tidy IDs first, so duplicates become truly identical
    df["location_id"] = df["location_id"].str.strip().str.upper()
    df["product_id"]  = df["product_id"].str.strip()

    # POS resends: exact duplicate rows
    df = df.drop_duplicates()

    # Numbers: Berlin writes "94,99"
    df["unit_price"]   = pd.to_numeric(df["unit_price"].str.replace(",", "."), errors="coerce")
    df["quantity"]     = pd.to_numeric(df["quantity"])
    df["discount_pct"] = pd.to_numeric(df["discount_pct"])

    # Dates: normal format first, then Berlin's format fills the gaps
    dt = df["transaction_datetime"]
    df["transaction_datetime"] = (
        pd.to_datetime(dt, format="%Y-%m-%d %H:%M:%S", errors="coerce")
          .fillna(pd.to_datetime(dt, format="%d.%m.%Y %H:%M", errors="coerce"))
    )

    # Missing currency: each store has one currency, so look it up in locations
    store_currency = locations.set_index("location_id")["currency"]
    df["currency"] = df["currency"].fillna(df["location_id"].map(store_currency))

    # Missing payment method: label it
    df["payment_method"] = df["payment_method"].fillna("unknown")

    # No price = can't compute revenue: drop and log
    missing_price = df["unit_price"].isna().sum()
    df = df.dropna(subset=["unit_price"])
    log_progress(f"store_sales: dropped {missing_price} rows with no price")

    return df
# COMMAND ----------

# ---------- web orders (JSON) ----------

COUNTRY_CURRENCY = {"US": "USD", "CA": "CAD", "GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR"}

def clean_web_orders(df):
    df = df.copy()

    # Remove QA test orders
    df = df[~df["customer_customer_id"].str.startswith("TEST")]

    # Event log: a refunded order appears twice (placed, then updated).
    # Keep the LAST event per order item = its latest status. Also removes exact duplicates.
    df = df.drop_duplicates(subset=["order_id", "product_id"], keep="last")

    # Numbers: some qty values were sent as text
    df["qty"]           = pd.to_numeric(df["qty"])
    df["unit_price"]    = pd.to_numeric(df["unit_price"])
    df["line_discount"] = pd.to_numeric(df["line_discount"])

    # Timestamps: most are ISO text, some are epoch milliseconds
    ts = df["order_ts"]
    is_epoch = ts.str.isdigit()
    df["order_ts"] = pd.to_datetime(ts.where(~is_epoch), utc=True, errors="coerce")
    df.loc[is_epoch, "order_ts"] = pd.to_datetime(ts[is_epoch].astype("int64"), unit="ms", utc=True)

    # Missing currency: the customer's country tells us what they paid in
    df["currency"] = df["currency"].fillna(df["customer_country"].map(COUNTRY_CURRENCY))

    return df
# COMMAND ----------

# ---------- products (XML) ----------

def clean_products(df):
    df = df.copy()

    df["product_id"] = df["product_id"].str.strip()

    # Category typed inconsistently: " outdoor ", "OUTDOOR" -> "Outdoor"
    df["category"] = df["category"].str.strip().str.title()

    # Prices: some costs look like "$12.30", some are empty
    df["list_price"] = pd.to_numeric(df["list_price"])
    df["unit_cost"]  = pd.to_numeric(df["unit_cost"].str.replace("$", ""), errors="coerce")

    # Same product exported twice: keep one, log how many
    dupes = df["product_id"].duplicated().sum()
    df = df.drop_duplicates(subset="product_id", keep="last")
    log_progress(f"products: removed {dupes} duplicate product records")

    return df
# COMMAND ----------

# ---------- locations (Excel) ----------

def clean_locations(df):
    df = df.copy()
    df["location_id"]        = df["location_id"].str.strip().str.upper()
    df["store_size_sqm"]     = pd.to_numeric(df["store_size_sqm"])
    df["monthly_rent_local"] = pd.to_numeric(df["monthly_rent_local"])
    df["opening_date"]       = pd.to_datetime(df["opening_date"])
    return df
# COMMAND ----------

# ---------- fx rates (Excel) ----------

def clean_fx_rates(df):
    df = df.copy()

    df["currency"] = df["currency"].str.strip().str.upper()

    # Rates typed as "1,0912" -> 1.0912
    df["rate_to_usd"] = pd.to_numeric(df["rate_to_usd"].str.replace(",", "."), errors="coerce")

    # Dates: real Excel dates, plus some typed as "07/15/2026"
    d = df["rate_date"]
    df["rate_date"] = (
        pd.to_datetime(d, format="%Y-%m-%d %H:%M:%S", errors="coerce")
          .fillna(pd.to_datetime(d, format="%m/%d/%Y", errors="coerce"))
    )

    # Fill weekends and missing days: every day gets the last known rate
    all_days = pd.date_range(df["rate_date"].min(), df["rate_date"].max(), freq="D")
    df = (
        df.dropna(subset=["rate_to_usd"])
          .pivot_table(index="rate_date", columns="currency", values="rate_to_usd")
          .reindex(all_days)
          .ffill()
          .rename_axis("rate_date")
          .reset_index()
          .melt(id_vars="rate_date", var_name="currency", value_name="rate_to_usd")
    )
    return df
# COMMAND ----------

# ---------- transform() ----------

def transform(bronze):
    # Order matters: locations is cleaned first because clean_store_sales uses it to fill missing currencies.
    silver = {}
    silver["locations"]   = clean_locations(bronze["locations"])
    silver["fx_rates"]    = clean_fx_rates(bronze["fx_rates"])
    silver["products"]    = clean_products(bronze["products"])
    silver["store_sales"] = clean_store_sales(bronze["store_sales"], silver["locations"])
    silver["web_orders"]  = clean_web_orders(bronze["web_orders"])

    for name, df in silver.items():
        log_progress(f"Cleaned {name}: {len(bronze[name])} -> {len(df)} rows")
    return silver

# COMMAND ----------

# ---------- Run and save to Silver ----------
log_progress("Transform phase Started")
silver = transform(bronze)
log_progress("Transform phase Ended")

for name, df in silver.items():
    df.to_parquet(f"{SILVER_DIR}/{name}.parquet")   # cleaned tables keep their real types
    log_progress(f"Saved silver: {name}")
