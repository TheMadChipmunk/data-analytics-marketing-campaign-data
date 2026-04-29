"""
Test documentation for Marketing Campaign Data
- Checks documentation and tests for marketing models
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

class TestDocumentation:
    def test_schema_yml_exists(self, greenweez_dbt_dir):
        staging_schema = greenweez_dbt_dir / "models" / "staging" / "schema.yml"
        assert staging_schema.exists(), (
            "❌ models/staging/schema.yml not found\n"
            "   Create a schema.yml in models/staging/ documenting stg_adwords and stg_facebook"
        )

    def test_marketing_models_have_docs(self, greenweez_dbt_dir):
        models_dir = greenweez_dbt_dir / "models"
        schema_files = list(models_dir.rglob("schema.yml")) + list(models_dir.rglob("*_schema.yml"))
        marketing_model_names = {'stg_adwords', 'stg_facebook'}
        documented = set()
        for file in schema_files:
            with open(file, 'r') as f:
                content = yaml.safe_load(f)
            if content and 'models' in content:
                for model in content['models']:
                    name = model.get('name', '')
                    if name in marketing_model_names and model.get('description'):
                        documented.add(name)
        assert documented, (
            "❌ No documentation found for marketing staging models\n"
            "   Add a description for stg_adwords and/or stg_facebook in schema.yml\n"
            "   models:\n"
            "     - name: stg_adwords\n"
            "       description: 'Google Ads campaign performance data'"
        )
