# Meridian Goods – Multi-Format Sales Consolidation ETL

Automated ETL that consolidates **CSV, JSON, XML and Excel** exports from four business teams into one analytics-ready sales file, built on **Databricks** and orchestrated with **Databricks Jobs**.

**Author:** [murltre](https://github.com/murltre)

---

## The business problem

**Meridian Goods** is a fictional retailer headquartered in Austin. It runs 4 stores (Austin, Toronto, Berlin, London) and 2 online shops (North America, Europe). Each team exports data from its own system:

| Team | System | Export | Format |
|---|---|---|---|
| Store Operations | POS terminals | Store sales | **CSV** |
| E-commerce | Online shop platform | Order events (nested) | **JSON lines** |
| Merchandising | Product management system | Product catalog with costs | **XML** |
| Finance | Spreadsheet | Locations and daily FX rates | **Excel** (2 sheets) |

Each week, a finance analyst merged these files by hand to answer the CEO's question: ***"What did we sell, and what did we actually earn, by channel, country and category?"*** The process was slow and error-prone because of mixed currencies, formats and IDs.

## Objectives
- Detect each file's type and extract it with the right reader
- Clean each source's real-world data issues
- Combine store and web sales into **one** table, in **USD**, with **profit**
- Make every run traceable with a log, and schedule the pipeline with Databricks Jobs

## Architecture (Medallion)

```
 raw/  (CSV, JSON, XML, XLSX)
   │  01_extract    detect type → extractor → all values as text
   ▼
 bronze/  (Parquet, 5 tables)
   │  02_transform  one cleaning function per source
   ▼
 silver/  (Parquet, 5 clean tables)
   │  03_load       standardize → combine → enrich → calculate
   ▼
 processed/sales_consolidated.csv   (Gold: 1 table)
   │  04_analysis   profit by channel / country / category
   ▼
 Business answers

 logs/etl_log.txt  ← every step writes here
 Databricks Job: 01 → 02 → 03 → 04 (scheduled)
```

## Technologies
Python · pandas · `glob` · `json` · `xml.etree.ElementTree` · openpyxl (Excel) · pyarrow (Parquet) · Databricks (Unity Catalog Volumes, Notebooks, Jobs)

## Project structure

```
meridian-multiformat-etl/
├── notebooks/                 # Databricks notebooks (source format, importable)
│   ├── 01_extract.py          # raw files → bronze
│   ├── 02_transform.py        # bronze → silver (cleaning)
│   ├── 03_load.py             # silver → gold CSV
│   └── 04_analysis.py         # gold → business summary
├── data/raw/                  # sample data (4 files, Q3 2026)
├── scripts/generate_raw_data.py  # recreates the sample data (seeded, reproducible)
├── docs/
│   ├── WORKFLOW.md            # how the pipeline works + key concepts
│   └── TROUBLESHOOTING.md     # errors we hit, fixes, validation checks
├── REPRODUCE_AND_COMMANDS.md  # step-by-step setup and run guide
└── requirements.txt
```

## Inputs and output

**Inputs** in `data/raw/` are sample data:

| File | Rows |
|---|---|
| `pos_store_sales_2026Q3.csv` | 4,945 sale lines |
| `ecommerce_orders_2026Q3.json` | 2,197 order events → 4,796 item rows |
| `product_catalog.xml` | 203 product records |
| `finance_reference.xlsx` | `locations` (7) + `fx_rates` (264) |

**Output:** `processed/sales_consolidated.csv` has **9,201 rows × 17 columns**:
`sale_id, sale_date, channel, region, country, location_id, product_id, product_name, category, status, quantity, currency, unit_price_local, fx_rate, revenue_usd, cost_usd, profit_usd`

## Key transformations
- **JSON flattening:** nested orders become one row per item (`json_normalize`)
- **Locale fixes:** Berlin's POS writes `94,99` and `01.07.2026`, which become `94.99` and a real date
- **Deduplication:** POS resends, duplicate catalog records, and order events (the latest event wins, so refunds are counted)
- **Missing values filled from other sources:** store currency from `locations`, web currency from the customer's country
- **FX gap-filling:** weekend and missing rates are forward-filled, so every sale can be converted
- **Standardize → combine → enrich:** shared column names, `concat`, then three left joins (location, product, FX on date + currency)
- **Business rules:** refunded and cancelled orders = $0; store returns (negative quantity) reduce revenue

## Results (sample data)

| Question | Answer |
|---|---|
| Store vs. web | Stores earn more revenue ($773K vs. $670K), but **web has the higher margin** (48.8% vs. 45.8%) |
| Top country | **US** ($234K profit), then Germany and the UK, nearly tied |
| Top category | **Electronics** ($196K profit, about 29% of total) |

## How to run
See **[REPRODUCE_AND_COMMANDS.md](REPRODUCE_AND_COMMANDS.md)**. In short: upload `data/raw/` to a Databricks Volume, import the notebooks, set `VOLUME_DIR`, and run `01 → 04`, or let the scheduled Job run them.

## What I learned
- Routing files to format-specific extractors with `glob` patterns
- Flattening nested JSON and parsing XML into DataFrames
- Medallion layers (Bronze/Silver/Gold) and why notebooks hand off data through files
- Cleaning order matters: tidy IDs **before** deduplicating
- Using one source to fill gaps in another (currency, FX)
- Orchestrating dependent notebook tasks with Databricks Jobs

## Limitations and next steps
- Full reload on every run (no incremental loading). If several files share a type, only the last one is kept.
- Business rules (refunds, returns, dropped rows with no price) are assumptions a finance team would confirm.
- Next steps: Delta tables instead of CSV/Parquet, data-quality tests, **Airflow** orchestration, and **Kafka** streaming for web orders.
