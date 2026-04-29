# Makefile for Marketing Campaign Data

.PHONY: test
test:
	@echo "🧪 Running tests for Marketing Campaign Data"
	@pytest tests/ -v

.PHONY: load-marketing
load-marketing:
	@echo "📦 Downloading marketing campaign data..."
	@mkdir -p greenweez_dbt/data/marketing
	@curl -s -o greenweez_dbt/data/marketing/raw_gz_adwords.parquet \
	  https://wagon-public-datasets.s3.amazonaws.com/data-analytics/03-Data-Transformation/10-DBT-Advanced/greenweez_parquet/raw_gz_adwords.parquet
	@curl -s -o greenweez_dbt/data/marketing/raw_gz_facebook.parquet \
	  https://wagon-public-datasets.s3.amazonaws.com/data-analytics/03-Data-Transformation/10-DBT-Advanced/greenweez_parquet/raw_gz_facebook.parquet
	@cd greenweez_dbt && python3 -c "\
import duckdb; conn = duckdb.connect('dev_database.duckdb'); \
conn.execute('CREATE SCHEMA IF NOT EXISTS raw_marketing'); \
conn.execute(\"CREATE OR REPLACE TABLE raw_marketing.raw_gz_adwords AS SELECT * FROM read_parquet('data/marketing/raw_gz_adwords.parquet')\"); \
conn.execute(\"CREATE OR REPLACE TABLE raw_marketing.raw_gz_facebook AS SELECT * FROM read_parquet('data/marketing/raw_gz_facebook.parquet')\"); \
print('✅ Marketing data loaded into raw_marketing schema')"

.PHONY: test-verbose
test-verbose:
	@pytest tests/ -vv -s

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make load-marketing - Download and load marketing data into DuckDB"
	@echo "  make test           - Run tests for this challenge"
	@echo "  make test-verbose   - Run tests with detailed output"
