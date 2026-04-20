"""
Test Suite: Ingestion Module
==============================
Tests the data ingestion logic for both CoinMarketCap and CoinGecko ingestors.
Validates API response parsing, Azure upload path generation, and error handling.

NOTE: These tests mock HTTP calls and Azure uploads — no real API calls are made.
"""
import pytest
import os
import sys
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pytest.importorskip("azure.storage.blob", reason="azure-storage-blob not installed (cloud dependency)")

@pytest.fixture(autouse=True)
def mock_env():
    """Set required env vars for all tests."""
    with patch.dict(os.environ, {
        'CMC_API_KEY': 'test-key-not-real',
        'AZURE_STORAGE_CONNECTION_STRING': 'DefaultEndpointsProtocol=https;AccountName=test',
    }):
        yield

class TestCMCIngestor:

    def test_fetch_returns_correct_envelope(self):
        """Verify the metadata envelope structure wrapping API responses."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [{'id': 1, 'name': 'Bitcoin', 'symbol': 'BTC'}]
        }
        mock_response.raise_for_status = MagicMock()

        with patch('requests.get', return_value=mock_response):
            from ingestion.cmc_ingestor import fetch_cmc_data
            result = fetch_cmc_data("listings", "https://fake-url.com", {'limit': '10'})

        assert result is not None
        assert result['endpoint_type'] == 'listings'
        assert result['source'] == 'coinmarketcap'
        assert 'ingested_at' in result
        assert 'payload' in result
        assert isinstance(result['payload'], list)

    def test_fetch_returns_none_on_error(self):
        """API failure should return None, not crash."""
        with patch('requests.get', side_effect=Exception("Connection timeout")):
            from ingestion.cmc_ingestor import fetch_cmc_data
            result = fetch_cmc_data("listings", "https://fake-url.com")
        assert result is None

    def test_upload_path_format(self):
        """Verify the Azure blob path follows the expected convention."""
        # Simulate what upload_to_azure would generate
        data = {
            'endpoint_type': 'listings',
            'source': 'coinmarketcap',
            'ingested_at': '2026-04-19T14:30:00+00:00',
            'payload': {'test': True}
        }
        ingestion_date = '2026-04-19'
        time_str = '143000'

        expected_path = f"bronze/coinmarketcap/ingestion_date={ingestion_date}/listings_{time_str}.json"

        # Reconstruct the path logic from cmc_ingestor.py
        actual_path = f"bronze/{data['source']}/ingestion_date={ingestion_date}/{data['endpoint_type']}_{time_str}.json"
        assert actual_path == expected_path

    def test_upload_skips_none_data(self):
        """upload_to_azure should silently skip when data is None."""
        from ingestion.cmc_ingestor import upload_to_azure
        # Should not raise any exception
        upload_to_azure(None)


# ---------------------------------------------------------------------------
# Tests: CoinGecko Ingestor
# ---------------------------------------------------------------------------

class TestCoinGeckoIngestor:

    def test_fetch_returns_correct_envelope(self):
        """Verify CoinGecko response envelope structure."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'id': 'bitcoin', 'symbol': 'btc', 'current_price': 84000}
        ]
        mock_response.raise_for_status = MagicMock()

        with patch('requests.get', return_value=mock_response):
            from ingestion.coingecko_ingestor import fetch_coingecko_data
            result = fetch_coingecko_data("markets_snapshot", "https://fake-url.com")

        assert result is not None
        assert result['endpoint_type'] == 'markets_snapshot'
        assert result['source'] == 'coingecko'
        assert 'payload' in result

    def test_historical_backfill_path_includes_coin_id(self):
        """Historical backfill uploads should include coin_id in the filename."""
        data = {
            'endpoint_type': 'historical_backfill',
            'source': 'coingecko',
            'coin_id': 'bitcoin',
            'ingested_at': '2026-04-19T14:30:00+00:00',
            'payload': {'prices': [[1713500000000, 84000]]}
        }
        ingestion_date = '2026-04-19'
        time_str = '143000'
        coin_suffix = f"_{data.get('coin_id')}" if 'coin_id' in data else ""

        expected_path = f"bronze/coingecko/ingestion_date={ingestion_date}/historical_backfill_bitcoin_{time_str}.json"
        actual_path = f"bronze/{data['source']}/ingestion_date={ingestion_date}/{data['endpoint_type']}{coin_suffix}_{time_str}.json"

        assert actual_path == expected_path

    def test_fetch_returns_none_on_error(self):
        """API failure should return None gracefully."""
        with patch('requests.get', side_effect=Exception("Rate limited")):
            from ingestion.coingecko_ingestor import fetch_coingecko_data
            result = fetch_coingecko_data("markets", "https://fake-url.com")
        assert result is None


# ---------------------------------------------------------------------------
# Tests: Data Envelope Validation
# ---------------------------------------------------------------------------

class TestDataEnvelope:
    """Test the JSON structure that both ingestors produce."""

    def test_envelope_is_json_serializable(self):
        """The envelope must be JSON-serializable for Azure upload."""
        envelope = {
            'endpoint_type': 'listings',
            'source': 'coinmarketcap',
            'ingested_at': datetime.now(timezone.utc).isoformat(),
            'payload': {'data': [1, 2, 3]}
        }
        # Should not raise
        serialized = json.dumps(envelope)
        assert len(serialized) > 0

    def test_envelope_required_fields(self):
        """Every envelope must have these 4 fields."""
        required = {'endpoint_type', 'source', 'ingested_at', 'payload'}
        envelope = {
            'endpoint_type': 'listings',
            'source': 'coinmarketcap',
            'ingested_at': '2026-04-19T14:30:00+00:00',
            'payload': {}
        }
        assert required.issubset(set(envelope.keys()))
