# Retail Lakeflow Project: Many Sources, One Table with `append_flow`

A small, complete **Databricks Lakeflow Declarative Pipelines** project that loads orders from several regions (US, EU, APAC) into **one** bronze table using one **Flow** per source, then cleans them in silver and aggregates them in gold.

> Companion project for the YouTube video **"Databricks Flows Explained"**: `<add video link here>`

---

## The idea

Orders arrive as CSV files in different folders, one per region. They all share the same structure and belong in one table.

| Without Flows | With Flows (`append_flow`) |
|---|---|
| One table per region plus an extra `UNION` job | One target table, one flow per region |
| New region = new table + redesign | New region = one more flow |
| More code, more places to break | Shared ingestion logic, independent checkpoints |

**Use a Flow when** several sources feed one table, sources are added over time, or you need to backfill history next to a live stream.
**Don't bother when** you only have one source: a normal streaming table is enough.

## Architecture

```
/Volumes/main/retail_bronze/raw_orders/
        ├── us/    ──► orders_us_flow   ─┐
        ├── eu/    ──► orders_eu_flow   ─┼─► main.retail_bronze.orders_bronze   (streaming table)
        └── apac/  ──► orders_apac_flow ─┘                │
                                                           ▼
                                     main.retail_silver.orders_silver         (streaming table)
                                                           │
                                                           ▼
                                     main.retail_gold.daily_sales             (materialized view)
```

| Layer | Object | What it does |
|---|---|---|
| Bronze | `main.retail_bronze.orders_bronze` | Auto Loader reads CSVs with an explicit schema. Three `@dp.append_flow` flows write into one streaming table. Adds `source_region` and `ingested_at`. |
| Silver | `main.retail_silver.orders_silver` | Drops rows with null `order_id` / `customer_id` or non-positive `quantity` / `amount`. Converts `order_date` to a date. Deduplicates on (`source_region`, `order_id`) with a 30-day watermark. |
| Gold | `main.retail_gold.daily_sales` | Materialized view: `total_sales`, `total_orders`, `total_quantity` per `order_date` and `source_region`. |

## Project structure

```
retail-lakeflow-project/
├── src/
│   ├── 00_setup/create_catalog_schemas_volumes.sql   # catalog, 3 schemas, landing volume
│   ├── 01_bronze/orders_bronze.py                    # Auto Loader + 3 append flows
│   ├── 02_silver/orders_silver.py                    # quality checks + streaming dedup
│   └── 03_gold/daily_sales.py                        # daily sales materialized view
├── sample_data/                                      # tiny example CSVs (us / eu / apac)
├── databricks.yml                                    # Databricks Asset Bundle config (next video)
└── README.md
```

## Prerequisites

- A Databricks workspace with **Unity Catalog** enabled
- Permission to create (or use) the catalog `main`, schemas, and a volume
- Ability to create an **ETL pipeline** (Jobs & Pipelines)

## Quick start

1. **Create the objects.** Run `src/00_setup/create_catalog_schemas_volumes.sql` in a SQL editor. It creates `main`, `retail_bronze`, `retail_silver`, `retail_gold` and the volume `main.retail_bronze.raw_orders`.
2. **Upload sample data.** Create the folders `us`, `eu` and `apac` inside the volume and upload the matching CSV from `sample_data/`:
   ```
   /Volumes/main/retail_bronze/raw_orders/us/orders_us_001.csv
   /Volumes/main/retail_bronze/raw_orders/eu/orders_eu_001.csv
   /Volumes/main/retail_bronze/raw_orders/apac/orders_apac_001.csv
   ```
3. **Create the pipeline.** *Jobs & Pipelines → Create → ETL pipeline*, then add the three files under `src/01_bronze`, `src/02_silver` and `src/03_gold` as source code.
4. **Run it.** Click **Run pipeline** and wait for all three tables to turn green.

## Verify the results

```sql
-- 1) Did all three flows land in one table?
SELECT source_region, COUNT(*) AS rows_loaded
FROM main.retail_bronze.orders_bronze
GROUP BY source_region;

-- 2) Did silver clean the data?
SELECT source_region, COUNT(*) AS clean_rows
FROM main.retail_silver.orders_silver
GROUP BY source_region;

-- 3) Business view
SELECT *
FROM main.retail_gold.daily_sales
ORDER BY order_date, source_region;
```

With the included sample data you should see:

| Check | Expected |
|---|---|
| Bronze rows | US 5, EU 4, APAC 4 (13 total) |
| Silver rows | US 3, EU 3, APAC 3 (9 total) |
| Gold rows | 6 (2 dates × 3 regions) |

Silver drops 4 rows on purpose: one duplicate order (US), one zero quantity (US), one missing `customer_id` (EU) and one negative quantity/amount (APAC).

## Add a new region

1. Create a new folder in the volume, for example `latam/`.
2. Add one more flow in `orders_bronze.py`:
   ```python
   @dp.append_flow(target=TARGET_TABLE, name="orders_latam_flow")
   def orders_latam_flow():
       return read_orders("LATAM")
   ```
3. Run the pipeline. The existing regions keep their own checkpoints, so nothing is reloaded.

## Roadmap

- [ ] Deploy with **Databricks Asset Bundles** (`databricks.yml`)
- [ ] Add tests for the pipeline logic
- [ ] CI/CD

## Author

**Ehsan**, Senior Data Engineer
YouTube: `<channel link>` · LinkedIn: `<profile link>`

## License

`<choose a license, e.g. MIT>`
