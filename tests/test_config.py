"""
Test Suite: Configuration Consistency
=====================================
Validates that critical configuration values (coin lists, table names, symbol mappings)
are consistent across all modules. A mismatch here causes silent production failures.
"""
import pytest
import yaml
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _extract_coins_from_file(filepath, pattern="COINS"):
    """Extract the COINS list from a Python file by parsing it safely."""
    import ast
    with open(filepath, 'r') as f:
        content = f.read()
    # Find the COINS assignment
    for line in content.split('\n'):
        stripped = line.strip()
        if stripped.startswith(f'{pattern} =') or stripped.startswith(f'{pattern}='):
            # Handle multi-line list — find the complete list
            start = content.index(stripped)
            bracket_start = content.index('[', start)
            bracket_count = 0
            i = bracket_start
            while i < len(content):
                if content[i] == '[':
                    bracket_count += 1
                elif content[i] == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        list_str = content[bracket_start:i+1]
                        return ast.literal_eval(list_str)
                i += 1
    return None


def _extract_symbol_map(filepath):
    """Extract the SYMBOL_MAP dict from api-edg.py."""
    import ast
    with open(filepath, 'r') as f:
        content = f.read()
    start_marker = "SYMBOL_MAP = {"
    if start_marker not in content:
        return None
    start = content.index(start_marker)
    brace_start = content.index('{', start)
    brace_count = 0
    i = brace_start
    while i < len(content):
        if content[i] == '{':
            brace_count += 1
        elif content[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                dict_str = content[brace_start:i+1]
                return ast.literal_eval(dict_str)
        i += 1
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCoinListConsistency:
    """COINS list must be identical across pipeline.py and scheduler.py.
    The integer encoding (coin_id_encoded) depends on list ORDER."""

    def test_pipeline_and_scheduler_coins_match(self):
        pipeline_coins = _extract_coins_from_file(
            os.path.join(PROJECT_ROOT, 'ml_models', 'pipeline.py'))
        scheduler_coins = _extract_coins_from_file(
            os.path.join(PROJECT_ROOT, 'ml_models', 'scheduler.py'))

        assert pipeline_coins is not None, "Could not extract COINS from pipeline.py"
        assert scheduler_coins is not None, "Could not extract COINS from scheduler.py"
        assert pipeline_coins == scheduler_coins, (
            f"COINS mismatch!\n"
            f"pipeline.py: {pipeline_coins}\n"
            f"scheduler.py: {scheduler_coins}\n"
            f"This will cause coin_id_encoded misalignment (train/serve skew)!"
        )

    def test_coin_count_is_ten(self):
        coins = _extract_coins_from_file(
            os.path.join(PROJECT_ROOT, 'ml_models', 'pipeline.py'))
        assert coins is not None
        assert len(coins) == 10, f"Expected 10 coins, got {len(coins)}"

    def test_api_symbol_map_covers_all_coins(self):
        """Every coin in COINS must have a symbol mapping in the API."""
        coins = _extract_coins_from_file(
            os.path.join(PROJECT_ROOT, 'ml_models', 'scheduler.py'))
        symbol_map = _extract_symbol_map(
            os.path.join(PROJECT_ROOT, 'ml_models', 'api-edg.py'))

        assert coins is not None
        assert symbol_map is not None

        api_coin_ids = set(symbol_map.values())
        pipeline_coin_ids = set(coins)

        missing_from_api = pipeline_coin_ids - api_coin_ids
        assert len(missing_from_api) == 0, (
            f"Coins in pipeline but NOT in API SYMBOL_MAP: {missing_from_api}"
        )


class TestConfigYAML:
    """Validate the centralized config.yaml if it exists."""

    @pytest.fixture
    def config(self):
        config_path = os.path.join(PROJECT_ROOT, 'config.yaml')
        if not os.path.exists(config_path):
            pytest.skip("config.yaml not found")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def test_config_has_required_sections(self, config):
        required_sections = ['coins', 'data_sources', 'storage', 'unity_catalog', 'ml', 'scheduling']
        for section in required_sections:
            assert section in config, f"Missing config section: {section}"

    def test_config_coin_count(self, config):
        assert len(config['coins']) == 10

    def test_config_coin_ids_match_pipeline(self, config):
        config_ids = [c['id'] for c in config['coins']]
        pipeline_coins = _extract_coins_from_file(
            os.path.join(PROJECT_ROOT, 'ml_models', 'pipeline.py'))
        assert config_ids == pipeline_coins, (
            f"config.yaml coin order doesn't match pipeline.py!\n"
            f"config.yaml: {config_ids}\n"
            f"pipeline.py: {pipeline_coins}"
        )

    def test_ml_features_defined(self, config):
        training_features = config['ml']['features']['training']
        assert len(training_features) >= 7, f"Expected >= 7 training features, got {len(training_features)}"
        assert 'coin_id_encoded' in training_features
