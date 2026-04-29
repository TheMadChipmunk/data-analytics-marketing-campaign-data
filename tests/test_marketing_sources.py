"""
Test marketing sources for Marketing Campaign Data
- Checks sources.yml for marketing platforms
"""

import pytest
from pathlib import Path
import yaml

@pytest.fixture
def greenweez_dbt_dir():
    project_dir = Path(__file__).parent.parent / "greenweez_dbt"
    assert project_dir.exists(), (
        "❌ greenweez_dbt/ directory not found. Did you copy your dbt project?"
    )
    return project_dir

class TestMarketingSources:
    def test_marketing_sources_exist(self, greenweez_dbt_dir):
        models_dir = greenweez_dbt_dir / "models"
        sources_files = list(models_dir.rglob("sources.yml"))
        marketing_keywords = ["adwords", "bing", "criteo", "facebook", "ads", "campaign"]
        found = set()
        for file in sources_files:
            with open(file, 'r') as f:
                content = yaml.safe_load(f)
            if content and 'sources' in content:
                for src in content['sources']:
                    for table in src.get('tables', []):
                        table_name = table.get('name', '').lower()
                        if any(k in table_name for k in marketing_keywords):
                            found.add(table_name)
        assert found, (
            "❌ No marketing source tables found in sources.yml\n"
            "   Add raw_adwords and raw_facebook as tables under the raw_marketing source"
        )
