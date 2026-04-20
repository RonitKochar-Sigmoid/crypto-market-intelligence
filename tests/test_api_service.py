import pytest
import sys
import os
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pytest.importorskip("deltalake", reason="deltalake not installed (cloud dependency)")

@pytest.fixture
def mock_gold_daily_df():
    """Mock Gold daily summary DataFrame matching real schema."""
    return pd.DataFrame({
        'symbol': ['BTC', 'ETH', 'SOL', 'ADA', 'DOGE'],
        'price_usd': [84231.50, 3200.00, 142.50, 0.62, 0.15],
        'daily_return': [0.012, -0.005, 0.08, -0.03, 0.15],
        'volume_24h': [28e9, 12e9, 3e9, 800e6, 1.2e9],
        'market_cap': [1.65e12, 384e9, 62e9, 22e9, 20e9],
        '7d_moving_avg': [83500.0, 3150.0, 138.0, 0.64, 0.14],
        '14d_moving_avg': [82800.0, 3100.0, 135.0, 0.65, 0.13],
        'volume_spike': [0, 0, 1, 0, 1],
        'market_dominance_pct': [54.2, 12.6, 2.0, 0.7, 0.6],
        'event_timestamp': ['2026-04-19 08:00:00'] * 5,
    })


@pytest.fixture
def mock_volatility_df():
    """Mock Gold volatility DataFrame."""
    return pd.DataFrame({
        'symbol': ['BTC', 'ETH', 'SOL'],
        'rolling_std': [1250.50, 85.30, 12.40],
        'market_cap': [1.65e12, 384e9, 62e9],
        'event_timestamp': ['2026-04-19 08:00:00'] * 3,
    })


@pytest.fixture
def client(mock_gold_daily_df, mock_volatility_df):
    """Create a FastAPI TestClient with mocked data access."""
    with patch.dict(os.environ, {'AZURE_STORAGE_KEY': 'test-key-not-real'}):
        # Need to import AFTER setting env var
        from api_service.main import app

        # Mock the get_df function to return test data
        def mock_get_df(path):
            if 'daily_price_summary' in path:
                return mock_gold_daily_df
            elif 'volatility' in path:
                return mock_volatility_df
            elif 'global' in path:
                return pd.DataFrame({
                    'total_market_cap': [3.04e12],
                    'total_volume_24h': [95e9],
                    'btc_dominance': [54.2],
                    'event_timestamp': ['2026-04-19 08:00:00']
                })
            return pd.DataFrame()

        with patch('api_service.main.get_df', side_effect=mock_get_df):
            from fastapi.testclient import TestClient
            yield TestClient(app)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_has_status(self, client):
        data = client.get("/health").json()
        assert data['status'] == 'healthy'

    def test_health_has_timestamp(self, client):
        data = client.get("/health").json()
        assert 'utc_now' in data


class TestHomeEndpoint:
    def test_home_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_home_has_docs_link(self, client):
        data = client.get("/").json()
        assert 'docs' in data


class TestCoinsEndpoint:
    def test_coins_returns_200(self, client):
        response = client.get("/coins")
        assert response.status_code == 200

    def test_coins_has_symbols(self, client):
        data = client.get("/coins").json()
        assert 'symbols' in data
        assert len(data['symbols']) > 0

    def test_coins_symbols_are_sorted(self, client):
        data = client.get("/coins").json()
        symbols = data['symbols']
        assert symbols == sorted(symbols)


class TestLatestMetrics:
    def test_valid_symbol_returns_200(self, client):
        response = client.get("/coins/BTC/latest")
        assert response.status_code == 200

    def test_invalid_symbol_returns_404(self, client):
        response = client.get("/coins/FAKECOIN/latest")
        assert response.status_code == 404

    def test_case_insensitive(self, client):
        """Should work with btc, BTC, Btc, etc."""
        response = client.get("/coins/btc/latest")
        assert response.status_code == 200

    def test_response_has_price(self, client):
        data = client.get("/coins/BTC/latest").json()
        assert 'price_usd' in data
        assert data['price_usd'] > 0

    def test_response_has_metrics(self, client):
        data = client.get("/coins/BTC/latest").json()
        assert 'daily_return' in data
        assert '7d_moving_avg' in data


class TestRankings:
    def test_rankings_returns_200(self, client):
        response = client.get("/market/rankings")
        assert response.status_code == 200

    def test_rankings_has_gainers_and_losers(self, client):
        data = client.get("/market/rankings").json()
        assert 'top_gainers' in data
        assert 'top_losers' in data

    def test_rankings_default_top_5(self, client):
        data = client.get("/market/rankings").json()
        assert len(data['top_gainers']) <= 5
        assert len(data['top_losers']) <= 5

    def test_rankings_custom_top(self, client):
        data = client.get("/market/rankings?top=2").json()
        assert len(data['top_gainers']) <= 2

    def test_gainers_sorted_descending(self, client):
        data = client.get("/market/rankings").json()
        returns = [g['daily_return'] for g in data['top_gainers']]
        assert returns == sorted(returns, reverse=True)


class TestVolatility:
    def test_valid_symbol_returns_200(self, client):
        response = client.get("/coins/BTC/volatility")
        assert response.status_code == 200

    def test_invalid_symbol_returns_404(self, client):
        response = client.get("/coins/FAKECOIN/volatility")
        assert response.status_code == 404

    def test_response_has_rolling_std(self, client):
        data = client.get("/coins/BTC/volatility").json()
        assert 'rolling_std' in data
        assert data['rolling_std'] > 0
