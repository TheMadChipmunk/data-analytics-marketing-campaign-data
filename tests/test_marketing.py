"""
Test Marketing Campaign Data

Validates that student has:
- Added new marketing sources (adwords, bing, criteo, facebook)
- Created staging models for each source
- Enriched existing marts with campaign data
- Added proper documentation and tests
"""

import pytest
from pathlib import Path
import yaml


class TestMarketingCampaignData:
    """Test marketing campaign data integration."""

    @pytest.fixture
    def greenweez_dbt_dir(self):
        """Get the greenweez_dbt project directory."""
        project_dir = Path(__file__).parent.parent / "greenweez_dbt"
        assert project_dir.exists(), (
            f"❌ greenweez_dbt/ directory not found in {Path(__file__).parent.parent}\n"
            f"   Did you copy your dbt project from the previous challenge? (Section 0)\n"
            f"   Run: ls .. to find the previous challenge directory, then:\n"
            f"   cp -r ../PREVIOUS-CHALLENGE/greenweez_dbt ."
        )
        return project_dir

    @pytest.fixture
    def sources_files(self, greenweez_dbt_dir):
        """Find all sources files."""
        models_dir = greenweez_dbt_dir / "models"
        if not models_dir.exists():
            return []
        return list(models_dir.rglob("sources.yml"))

    @pytest.fixture
    def staging_models(self, greenweez_dbt_dir):
        """Get all staging models."""
        staging_dir = greenweez_dbt_dir / "models" / "staging"
        if not staging_dir.exists():
            return []
        return list(staging_dir.rglob("*.sql"))

    def test_has_marketing_sources(self, sources_files):
        """Must define marketing source tables (raw_adwords, raw_facebook) in sources.yml."""
        if not sources_files:
            pytest.fail(
                "❌ No sources.yml files found\n"
                "   Create sources.yml to define marketing sources"
            )

        all_tables = []
        for sources_file in sources_files:
            with open(sources_file, 'r') as f:
                content = yaml.safe_load(f)

            if content and 'sources' in content:
                for source in content['sources']:
                    for table in source.get('tables', []):
                        all_tables.append(table.get('name', ''))

        # Check for at least one marketing platform table
        marketing_keywords = ['adwords', 'bing', 'criteo', 'facebook', 'ads', 'campaign']
        has_marketing_source = any(
            any(keyword in table_name.lower() for keyword in marketing_keywords)
            for table_name in all_tables
        )

        assert has_marketing_source, (
            f"❌ No marketing campaign source tables found\n"
            f"   Add a source block with marketing platform tables:\n"
            f"     - name: raw_marketing\n"
            f"       tables:\n"
            f"         - name: raw_adwords\n"
            f"         - name: raw_facebook\n"
            f"   Current source tables: {', '.join(all_tables) or 'none'}"
        )

    def test_has_multiple_marketing_sources(self, sources_files):
        """Should define at least 2 marketing source tables (raw_adwords, raw_facebook)."""
        marketing_tables = []

        for sources_file in sources_files:
            with open(sources_file, 'r') as f:
                content = yaml.safe_load(f)

            if not content or 'sources' not in content:
                continue

            for source in content['sources']:
                for table in source.get('tables', []):
                    table_name = table.get('name', '').lower()
                    if any(keyword in table_name for keyword in ['adwords', 'bing', 'criteo', 'facebook', 'ads', 'campaign']):
                        marketing_tables.append(table_name)

        assert len(marketing_tables) >= 2, (
            "❌ Should define at least 2 marketing platform tables in sources.yml\n"
            "   Found: {} ({})\n".format(
                len(marketing_tables), ', '.join(marketing_tables) if marketing_tables else 'none') +
            "   Add raw_adwords and raw_facebook as tables under the raw_marketing source"
        )

    def test_has_staging_models_for_marketing(self, staging_models):
        """Must create staging models for marketing sources."""
        marketing_models = []

        for model in staging_models:
            model_name = model.stem.lower()
            # Check for marketing-related model names
            if any(keyword in model_name for keyword in ['adwords', 'bing', 'criteo', 'facebook', 'ads', 'campaign']):
                marketing_models.append(model.stem)

        assert len(marketing_models) > 0, (
            "❌ No staging models for marketing data found\n"
            "   Create stg_* models for each marketing source\n"
            "   Example: stg_adwords.sql, stg_facebook.sql"
        )

    def test_marketing_models_use_source_function(self, staging_models):
        """Marketing staging models should use {{ source() }}."""
        marketing_models = [
            m for m in staging_models
            if any(keyword in m.stem.lower() for keyword in ['adwords', 'bing', 'criteo', 'facebook', 'ads', 'campaign'])
        ]

        errors = []
        for model in marketing_models:
            with open(model, 'r') as f:
                content = f.read()

            if '{{ source(' not in content and '{{source(' not in content:
                errors.append(model.stem)

        if errors:
            pytest.fail(
                "❌ Marketing models should use {{ source() }} function\n"
                "   Models missing source() references:\n" +
                "\n".join(["   - {}".format(name) for name in errors])
            )

    def test_has_schema_yml_with_tests(self, greenweez_dbt_dir):
        """Should have schema.yml files with tests."""
        models_dir = greenweez_dbt_dir / "models"
        if not models_dir.exists():
            pytest.skip("No models directory")

        schema_files = list(models_dir.rglob("schema.yml")) + list(models_dir.rglob("*_schema.yml"))

        assert len(schema_files) > 0, (
            "❌ No schema.yml files found\n"
            "   Add documentation and tests for your models"
        )

        total_tests = 0
        for schema_file in schema_files:
            with open(schema_file, 'r') as f:
                content = yaml.safe_load(f)

            if not content:
                continue

            models = content.get('models', [])
            for model in models:
                columns = model.get('columns', [])
                for column in columns:
                    tests = column.get('tests', [])
                    total_tests += len(tests)

        assert total_tests > 0, (
            "❌ No data quality tests found in schema.yml\n"
            "   Add tests to validate your marketing data"
        )


class TestIntermediateModel:
    """Test the int_marketing_campaigns intermediate model."""

    @pytest.fixture
    def greenweez_dbt_dir(self):
        """Get the greenweez_dbt project directory."""
        project_dir = Path(__file__).parent.parent / "greenweez_dbt"
        assert project_dir.exists(), (
            f"❌ greenweez_dbt/ directory not found in {Path(__file__).parent.parent}\n"
            f"   Did you copy your dbt project from the previous challenge? (Section 0)\n"
            f"   Run: ls .. to find the previous challenge directory, then:\n"
            f"   cp -r ../PREVIOUS-CHALLENGE/greenweez_dbt ."
        )
        return project_dir

    @pytest.fixture
    def int_campaigns_file(self, greenweez_dbt_dir):
        """Get int_marketing_campaigns.sql."""
        return greenweez_dbt_dir / "models" / "intermediate" / "int_marketing_campaigns.sql"

    def test_int_marketing_campaigns_exists(self, int_campaigns_file):
        """models/intermediate/int_marketing_campaigns.sql must exist."""
        assert int_campaigns_file.exists(), (
            "❌ models/intermediate/int_marketing_campaigns.sql not found\n"
            "   Create the intermediate model that combines adwords and facebook (Section 4)"
        )

    def test_int_marketing_campaigns_has_union_all(self, int_campaigns_file):
        """int_marketing_campaigns.sql must combine platforms with UNION ALL."""
        if not int_campaigns_file.exists():
            pytest.skip("int_marketing_campaigns.sql doesn't exist yet")
        content = int_campaigns_file.read_text().lower()
        assert "union all" in content, (
            "❌ int_marketing_campaigns.sql must use UNION ALL to combine adwords and facebook\n"
            "   Stack the two staging models: stg_adwords UNION ALL stg_facebook"
        )

    def test_int_marketing_campaigns_has_ctr_metric(self, int_campaigns_file):
        """int_marketing_campaigns.sql must include click_through_rate."""
        if not int_campaigns_file.exists():
            pytest.skip("int_marketing_campaigns.sql doesn't exist yet")
        content = int_campaigns_file.read_text().lower()
        assert "click_through_rate" in content, (
            "❌ int_marketing_campaigns.sql must calculate 'click_through_rate'\n"
            "   CTR = ads_clicks / ads_impressions (guard against division by zero)"
        )

    def test_int_marketing_campaigns_has_cpc_metric(self, int_campaigns_file):
        """int_marketing_campaigns.sql must include cost_per_click."""
        if not int_campaigns_file.exists():
            pytest.skip("int_marketing_campaigns.sql doesn't exist yet")
        content = int_campaigns_file.read_text().lower()
        assert "cost_per_click" in content, (
            "❌ int_marketing_campaigns.sql must calculate 'cost_per_click'\n"
            "   CPC = ads_cost / ads_clicks (guard against division by zero)"
        )
