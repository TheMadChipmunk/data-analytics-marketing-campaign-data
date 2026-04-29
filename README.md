## Context

In the previous challenge you built the complete Greenweez pipeline. Now you extend it with a new data domain: marketing. The finance team currently tracks revenue and margin in `finance_days`, but can't see whether ad spend is actually driving that revenue.

Your job: integrate data from two advertising platforms (Google Ads and Facebook Ads), build the transformation pipeline for them, and lay the groundwork so the finance team can analyse marketing ROI alongside the existing financial metrics.

## Objective

Extend the Greenweez pipeline with two new marketing data sources and build the models to combine them:

```text
raw_marketing.raw_adwords  ──► stg_adwords  ──┐
                                               ├──► int_marketing_campaigns
raw_marketing.raw_facebook ──► stg_facebook ──┘
```

The new models sit alongside the existing pipeline — you're adding to it, not changing it.

---

## Prerequisites

- Completed Challenge 03 (Greenweez pipeline built: staging → intermediate → mart all running)
- Familiar with `{{ source() }}`, `{{ ref() }}`, and `dbt build`

---

## Section 0: Copy Your Project

> **Working directory convention for this challenge:** `dbt` commands run from **inside** `greenweez_dbt/`. `git` commands run from the **challenge directory** (one level up). Each block below is labelled.

### 0.1 Copy greenweez_dbt from the previous challenge

**📍 In your terminal (challenge directory):**

```bash
# Check the name of your previous challenge directory
ls ..

cp -rP ../../../{{ local_path_to("03-Data-Transformation/10-DBT-Advanced/04-Build-Greenweez-Pipeline") }}/greenweez_dbt .
```

This copies your complete pipeline — all models, sources, tests, and the `dev_database.duckdb` symlink. The symlink points to the shared database at `03-Data-Transformation/dbt-shared/greenweez.duckdb`, so the raw Greenweez data is available immediately. No re-running `make setup` needed.

**📍 In your terminal (into greenweez_dbt/):**

```bash
cd greenweez_dbt
dbt debug
```

You should see `Connection test: OK`.

**📍 In your terminal (challenge directory — `cd ..` first):**

```bash
git add greenweez_dbt/
git commit -m "Copy Greenweez pipeline from previous challenge"
git push origin master
```

---

## Section 1: Load Marketing Data

Two advertising platforms provide daily campaign performance data. Each platform exports to a Parquet file with the same columns:

- `date_date` (date) — Campaign date
- `campaign_key` (varchar) — Internal campaign identifier
- `camPGN_name` (varchar) — Campaign name (inconsistent casing — needs renaming)
- `ads_cost` (varchar) — Daily spend (stored as text — needs casting)
- `impression` (double) — Number of ad views
- `click` (double) — Number of click-throughs

> **Real-world data quality heads-up:** Both platforms export `camPGN_name` with mixed casing, and `ads_cost` is stored as text. Your staging models will fix both — renaming to consistent column names and casting to the right types. `paid_source` has been excluded from the export: you'll add platform context yourself as a literal column in staging.

### 1.1 Download and load the data

**📍 In your terminal (challenge directory):**

```bash
make load-marketing
```

This downloads the two Parquet files and loads them into your DuckDB database as `raw_marketing.raw_gz_adwords` and `raw_marketing.raw_gz_facebook`.

### 1.2 Explore the data

**🗄️ In DBeaver**, connect to `greenweez_dbt/dev_database.duckdb` and run:

```sql
-- Confirm both tables loaded correctly
SELECT 'adwords' AS platform, COUNT(*) AS rows FROM raw_marketing.raw_gz_adwords
UNION ALL
SELECT 'facebook', COUNT(*) FROM raw_marketing.raw_gz_facebook;

-- Inspect the columns
SELECT * FROM raw_marketing.raw_gz_adwords LIMIT 5;
SELECT * FROM raw_marketing.raw_gz_facebook LIMIT 5;
```

Compare the two — the column naming inconsistency you'll need to fix should be visible here.

---

## Section 2: Define Marketing Sources

Your existing `models/staging/sources.yml` already defines the `raw` source. Add the `raw_marketing` source to the same file.

### Source definition brief

Add a second source block for `raw_marketing` with two tables:

- `raw_gz_adwords` — Google Ads campaign performance data
- `raw_gz_facebook` — Facebook Ads campaign performance data

Both under source name `raw_marketing`.

**📝 In VS Code**, open `models/staging/sources.yml` and add the `raw_marketing` source block below the existing `raw` source. Give each table at least a brief description.

<details>
<summary markdown="span">**💡 Hint: source block structure**</summary>

Your existing `raw` source block in `sources.yml` is the pattern. Each `sources:` list item is a separate source group, identified by `name:`. Add a new list item for `raw_marketing` with its `tables:` list.

The `name:` in the source block must match the first argument in `{{ source('raw_marketing', 'raw_gz_adwords') }}`.

</details>

### Checkpoint 1: Marketing Sources

**📍 In your terminal (challenge directory):**

```bash
pytest tests/test_marketing_sources.py -v
```

**Expected:**

- 1 passed

**If tests pass, commit and push:**

**📍 In your terminal (challenge directory):**

```bash
git add greenweez_dbt/models/staging/sources.yml
git commit -m "Add marketing sources"
git push origin master
```

The new source block is registered. Next you'll build a staging model for each platform — the step where you standardise their different column names and types to a common schema.

---

## Section 3: Build Staging Models

One staging model per platform. Staging models are where you fix platform-specific quirks and standardise to a common schema.

### Staging model brief

Both staging models should produce the same output schema:

- `date_date` (date) — pass through
- `campaign_name` (varchar) — rename from `"camPGN_name"` (both tables)
- `ads_cost` (double) — cast from varchar
- `ads_impressions` (double) — rename from `impression`
- `ads_clicks` (double) — rename from `click`
- `platform` (varchar) — add as a literal: `'adwords'` or `'facebook'`

The `platform` column is what makes the UNION ALL in Section 4 useful — you'll be able to filter or group by platform later.

**📝 In VS Code**, create `models/staging/stg_adwords.sql` and `models/staging/stg_facebook.sql`. Use `{{ source('raw_marketing', ...) }}` — no hardcoded table names.

**📍 In your terminal (inside greenweez_dbt/):**

```bash
dbt build --select stg_adwords stg_facebook
```

<details>
<summary markdown="span">**💡 Hint: quoting case-sensitive column names in DuckDB**</summary>

DuckDB is case-sensitive when you use double quotes: `"camPGN_name"` selects the column with that exact casing. Without quotes, DuckDB lowercases identifiers, so `campgn_name` would fail to match.

</details>

### Checkpoint 2: Staging Models

**📍 In your terminal (challenge directory):**

```bash
pytest tests/test_staging_models.py -v
```

**Expected:**

- 1 passed

**If tests pass, commit and push:**

**📍 In your terminal (challenge directory):**

```bash
git add greenweez_dbt/models/staging/stg_adwords.sql greenweez_dbt/models/staging/stg_facebook.sql

git commit -m "Staging models for marketing complete"
git push origin master
```

Both staging models are producing an identical output schema. Now you'll combine them in `int_marketing_campaigns` with a UNION ALL and add the calculated metrics.

---

## Section 4: Combine Platforms

Now that both staging models output identical schemas, combine them into a single intermediate model using UNION ALL. This is the pattern for multi-source consolidation — you extend it to three or four platforms just by adding one more `UNION ALL` block.

### Intermediate model brief

**Create `models/intermediate/int_marketing_campaigns.sql`:**

- Source: `{{ ref('stg_adwords') }}` and `{{ ref('stg_facebook') }}`
- Pattern: UNION ALL — keep all rows from both platforms
- Add two calculated columns:

  - `click_through_rate` = `ads_clicks / ads_impressions` — guard against division by zero when `ads_impressions = 0`
  - `cost_per_click` = `ads_cost / ads_clicks` — guard against division by zero when `ads_clicks = 0`

Use `CASE WHEN ... > 0 THEN ... ELSE 0 END` or `NULLIF` for the division-by-zero guard.

**📍 In your terminal (inside greenweez_dbt/):**

```bash
dbt build --select int_marketing_campaigns
```

Check the lineage: `stg_adwords` and `stg_facebook` should both appear as parents of `int_marketing_campaigns` in the dbt DAG.

**🗄️ In DBeaver**, verify the combined model:

```sql
SELECT platform, COUNT(*) AS rows
FROM main_intermediate.int_marketing_campaigns
GROUP BY platform;
```

Both platforms should appear with similar row counts.

---

## Section 5: Document and Test

Add a `schema.yml` for your new models. The `test_documentation.py` test checks that at least one model has a description — but good documentation means all of them do.

### Documentation brief

**📝 In VS Code**, create `models/staging/schema.yml` documenting `stg_adwords` and `stg_facebook`. For each model, include:

- A model-level `description`
- At least two column-level tests (`not_null` is appropriate for `date_date`, `campaign_name`, `platform`)

**📍 In your terminal (inside greenweez_dbt/):**

```bash
dbt test --select stg_adwords stg_facebook int_marketing_campaigns
```

All tests should pass.

### Checkpoint 3: Documentation

**📍 In your terminal (challenge directory):**

```bash
pytest tests/test_documentation.py -v
```

**Expected:**

- 2 passed

**To run all tests:**

```bash
make
```

**Expected:** 13 passed

**If all tests pass, commit and push:**

**📍 In your terminal (challenge directory):**

```bash
git add greenweez_dbt/models/staging/schema.yml
git commit -m "Marketing documentation and tests complete"
git push origin master
```

---

## 🎉 Challenge Complete

Sources, staging, intermediate, and documentation are all done. The pipeline handles two ad platforms — and adding a third would be one new staging model and one new `UNION ALL` block.

### Key takeaways

- **Staging normalises per-platform quirks, intermediate is platform-agnostic** — by the time data reaches `int_marketing_campaigns`, all sources look identical
- **UNION ALL scales trivially** — adding a third ad platform is one new staging model and one new `UNION ALL` block; nothing else changes
- **`dbt build --select model+`** — the `+` suffix runs a model and everything downstream; use it for targeted refreshes when upstream data changes
