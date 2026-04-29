"""
Test staging models for Marketing Campaign Data
- Checks creation and correctness of staging models for marketing sources
"""

import pytest
from pathlib import Path

@pytest.fixture
def greenweez_dbt_dir():
    project_dir = Path(__file__).parent.parent / "greenweez_dbt"
    assert project_dir.exists(), (
        "❌ greenweez_dbt/ directory not found. Did you copy your dbt project?"
    )
    return project_dir

class TestStagingModels:
    def test_staging_models_exist(self, greenweez_dbt_dir):
        staging_dir = greenweez_dbt_dir / "models" / "staging"
        marketing_keywords = ["adwords", "bing", "criteo", "facebook", "ads", "campaign"]
        found = []
        if staging_dir.exists():
            for file in staging_dir.glob("*.sql"):
                name = file.stem.lower()
                if any(k in name for k in marketing_keywords):
                    found.append(name)
        assert found, "❌ No marketing staging models found in models/staging/"

    def test_stg_adwords_exists(self, greenweez_dbt_dir):
        stg_adwords = greenweez_dbt_dir / "models" / "staging" / "stg_adwords.sql"
        assert stg_adwords.exists(), (
            "❌ stg_adwords.sql not found in models/staging/\n"
            "   Did you create the AdWords staging model?"
        )

    def test_stg_facebook_exists(self, greenweez_dbt_dir):
        stg_facebook = greenweez_dbt_dir / "models" / "staging" / "stg_facebook.sql"
        assert stg_facebook.exists(), (
            "❌ stg_facebook.sql not found in models/staging/\n"
            "   Did you create the Facebook Ads staging model?"
        )
