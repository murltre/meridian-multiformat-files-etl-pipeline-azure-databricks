# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Extract (raw files -> Bronze)
# MAGIC Finds each raw file by type, reads it with the matching extractor, and saves the result as Parquet in `bronze/`.

# COMMAND ----------

# ---------- Imports ----------
import glob                          # find files by pattern (*.csv, *.json, ...)
import os                            # build paths, create folders
import json                          # parse the JSON order events
import xml.etree.ElementTree as ET   # parse the XML product catalog
from datetime import datetime        # timestamps for the log
import pandas as pd                  # tables (DataFrames)

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

# Create the folders if missing. exist_ok=True makes this safe to re-run.
for folder in [RAW_DIR, BRONZE_DIR, SILVER_DIR, OUTPUT_DIR, LOG_DIR]:
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

# ---------- One extractor per file type ----------
# Each takes a path and returns a table. Values are read as text (dtype=str) on purpose,
# so dirty values like Berlin's "94,99" survive until the cleaning step.

def extract_csv(path):
    # Store POS sales: already flat
    return pd.read_csv(path, dtype=str)


def extract_json(path):
    # Web orders: JSON lines (one order per line) with a nested "items" list.
    with open(path) as f:
        orders = [json.loads(line) for line in f]

    # Flatten: one row per item, with order-level fields repeated on each row.
    return pd.json_normalize(
        orders,
        record_path="items",
        meta=["order_id", "order_ts", "storefront", "currency", "status",
              ["customer", "customer_id"], ["customer", "country"]],   # nested -> customer_customer_id, customer_country
        sep="_",
    )


def extract_xml(path):
    # Product catalog: walk every <product> tag
    root = ET.parse(path).getroot()
    rows = []
    for p in root.findall("product"):
        rows.append({
            "product_id": p.get("id"),                       # attribute: <product id="P1000">
            "name":       p.findtext("name"),
            "category":   p.findtext("category"),
            "list_price": p.findtext("pricing/list_price"),  # nested tag
            "unit_cost":  p.findtext("pricing/unit_cost"),
        })
    return pd.DataFrame(rows)


def extract_excel(path):
    # Finance workbook: sheet_name=None reads ALL sheets -> {"locations": df, "fx_rates": df}
    return pd.read_excel(path, sheet_name=None, dtype=str)

# COMMAND ----------

# ---------- Extract: glob pattern detects the type, then calls the matching extractor ----------
def extract():
    raw = {}   # dictionary: source name -> DataFrame

    for file in glob.glob(f"{RAW_DIR}/*.csv"):
        raw["store_sales"] = extract_csv(file)
        log_progress(f"Extracted CSV: {os.path.basename(file)}")

    for file in glob.glob(f"{RAW_DIR}/*.json"):
        raw["web_orders"] = extract_json(file)
        log_progress(f"Extracted JSON: {os.path.basename(file)}")

    for file in glob.glob(f"{RAW_DIR}/*.xml"):
        raw["products"] = extract_xml(file)
        log_progress(f"Extracted XML: {os.path.basename(file)}")

    for file in glob.glob(f"{RAW_DIR}/*.xlsx"):
        raw["finance"] = extract_excel(file)     # a dict of 2 sheets
        log_progress(f"Extracted Excel: {os.path.basename(file)}")

    return raw


log_progress("ETL Job Started")
log_progress("Extract phase Started")
raw = extract()
log_progress("Extract phase Ended")

# COMMAND ----------

# ---------- Check: (rows, columns) per source ----------
for name, data in raw.items():
    if isinstance(data, dict):                   # Excel: dict of sheets
        for sheet, df in data.items():
            print(f"{name}/{sheet}: {df.shape}")
    else:
        print(f"{name}: {data.shape}")

# COMMAND ----------

# ---------- Save to Bronze ----------
# Notebooks don't share memory, so the next notebook reads these files.
# .astype("string"): web orders' qty mixes 2 and "3"; Parquet rejects mixed types in one column.
for name, data in raw.items():
    if isinstance(data, dict):                   # Excel: one Parquet file per sheet
        for sheet, df in data.items():
            df.astype("string").to_parquet(f"{BRONZE_DIR}/{sheet}.parquet")
            log_progress(f"Saved bronze: {sheet}")
    else:
        data.astype("string").to_parquet(f"{BRONZE_DIR}/{name}.parquet")
        log_progress(f"Saved bronze: {name}")
