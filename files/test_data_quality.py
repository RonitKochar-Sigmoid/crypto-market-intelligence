"""
Test Suite: Data Quality Checks
=================================
Validates data quality assertions that should be applied at each layer boundary.
These tests simulate the kind of checks that run inside Databricks notebooks
to ensure data integrity before promoting records downstream.
"""
import pytest
import pandas as pd
import numpy as np

def create_valid_bronze_record():
    """A valid Bronze-layer record as it arrives from ingestion."""
    return {
        'coin_id': 'bitcoin',
        'timestamp': '2026-04-19T10:00:00',
        'price_usd': 84231.50,
        'total_volume': 28000000000,
        'market_cap': 1650000000000,
        'source': 'coingecko'
    }


def create_valid_silver_df():
    """A valid Silver-layer DataFrame matching expected schema."""
    return pd.DataFrame({
        'coin_id': ['bitcoin', 'ethereum', 'solana'],
        'symbol': ['BTC', 'ETH', 'SOL'],
        'price_usd': [84231.50, 3200.00, 142.50],
        'volume_24h': [28e9, 12e9, 3e9],
        'market_cap': [1.65e12, 384e9, 62e9],
        'event_timestamp': pd.to_datetime(['2026-04-19 08:00:00'] * 3),
    })


def create_valid_gold_df():
    """A valid Gold-layer DataFrame with computed features."""
    return pd.DataFrame({
        'symbol': ['BTC', 'ETH', 'SOL'],
        'price_usd': [84231.50, 3200.00, 142.50],
        'daily_return': [0.012, -0.005, 0.08],
        '7d_moving_avg': [83500.0, 3150.0, 138.0],
        'rolling_std': [1250.50, 85.30, 12.40],
        'market_dominance_pct': [54.2, 12.6, 2.0],
    })


# ---------------------------------------------------------------------------
# Bronze Layer Quality Tests
# ---------------------------------------------------------------------------

class TestBronzeQuality:
    """Checks applied when data enters the Bronze layer."""

    def test_price_must_be_positive(self):
        record = create_valid_bronze_record()
        assert record['price_usd'] > 0, "Price cannot be zero or negative"

    def test_price_not_none(self):
        record = create_valid_bronze_record()
        assert record['price_usd'] is not None, "Price cannot be null"

    def test_volume_must_be_positive(self):
        record = create_valid_bronze_record()
        assert record['total_volume'] > 0, "Volume cannot be zero or negative"

    def test_market_cap_must_be_positive(self):
        record = create_valid_bronze_record()
        assert record['market_cap'] > 0, "Market cap cannot be zero or negative"

    def test_coin_id_must_be_string(self):
        record = create_valid_bronze_record()
        assert isinstance(record['coin_id'], str)
        assert len(record['coin_id']) > 0

    def test_timestamp_must_be_present(self):
        record = create_valid_bronze_record()
        assert record['timestamp'] is not None
        assert len(record['timestamp']) > 0


# ---------------------------------------------------------------------------
# Silver Layer Quality Tests
# ---------------------------------------------------------------------------

class TestSilverQuality:
    """Checks applied after Silver transformations."""

    def test_no_null_prices(self):
        df = create_valid_silver_df()
        assert df['price_usd'].notna().all(), "Silver layer should have no null prices"

    def test_no_null_symbols(self):
        df = create_valid_silver_df()
        assert df['symbol'].notna().all(), "Every record must have a symbol"

    def test_no_duplicate_coins_per_timestamp(self):
        """Each coin should appear at most once per timestamp."""
        df = create_valid_silver_df()
        dupes = df.duplicated(subset=['symbol', 'event_timestamp'], keep=False)
        assert not dupes.any(), "Duplicate coin-timestamp combinations found"

    def test_price_within_reasonable_range(self):
        """BTC shouldn't be $0.01 or $10M — catch likely API errors."""
        df = create_valid_silver_df()
        btc = df[df['symbol'] == 'BTC']
        if len(btc) > 0:
            price = btc.iloc[0]['price_usd']
            assert 1000 < price < 500000, f"BTC price {price} seems unreasonable"

    def test_volume_not_negative(self):
        df = create_valid_silver_df()
        assert (df['volume_24h'] >= 0).all(), "Negative volume detected"

    def test_market_cap_not_negative(self):
        df = create_valid_silver_df()
        assert (df['market_cap'] >= 0).all(), "Negative market cap detected"


# ---------------------------------------------------------------------------
# Gold Layer Quality Tests
# ---------------------------------------------------------------------------

class TestGoldQuality:
    """Checks applied after Gold aggregations."""

    def test_daily_return_in_valid_range(self):
        """Daily return should rarely exceed ±50% (even for volatile crypto)."""
        df = create_valid_gold_df()
        assert (df['daily_return'].abs() <= 0.50).all(), (
            f"Daily return exceeds ±50%: {df['daily_return'].tolist()}")

    def test_moving_average_positive(self):
        """Moving averages should never be negative."""
        df = create_valid_gold_df()
        assert (df['7d_moving_avg'] > 0).all(), "Negative 7d moving average"

    def test_volatility_non_negative(self):
        """Standard deviation cannot be negative."""
        df = create_valid_gold_df()
        assert (df['rolling_std'] >= 0).all()

    def test_market_dominance_sums_to_less_than_100(self):
        """Market dominance percentages should sum to ≤ 100%."""
        df = create_valid_gold_df()
        total = df['market_dominance_pct'].sum()
        assert total <= 100.0, f"Market dominance sums to {total}%, exceeds 100%"

    def test_market_dominance_between_0_and_100(self):
        """Each coin's dominance must be between 0% and 100%."""
        df = create_valid_gold_df()
        assert (df['market_dominance_pct'] >= 0).all()
        assert (df['market_dominance_pct'] <= 100).all()


# ---------------------------------------------------------------------------
# Cross-Layer Consistency Tests
# ---------------------------------------------------------------------------

class TestCrossLayerConsistency:
    """Tests that validate data consistency between layers."""

    def test_gold_has_all_silver_coins(self):
        """Every coin in Silver should appear in Gold."""
        silver = create_valid_silver_df()
        gold = create_valid_gold_df()
        silver_coins = set(silver['symbol'].unique())
        gold_coins = set(gold['symbol'].unique())
        missing = silver_coins - gold_coins
        assert len(missing) == 0, f"Coins in Silver but missing from Gold: {missing}"

    def test_row_count_preservation(self):
        """Gold should not have MORE rows than Silver (aggregation reduces or preserves)."""
        silver = create_valid_silver_df()
        gold = create_valid_gold_df()
        assert len(gold) <= len(silver), (
            f"Gold ({len(gold)} rows) has more rows than Silver ({len(silver)} rows)")

    def test_price_alignment(self):
        """Gold prices should match Silver prices for the same snapshot."""
        silver = create_valid_silver_df()
        gold = create_valid_gold_df()
        for symbol in gold['symbol']:
            gold_price = gold[gold['symbol'] == symbol]['price_usd'].values[0]
            silver_price = silver[silver['symbol'] == symbol]['price_usd'].values[0]
            assert gold_price == silver_price, (
                f"{symbol}: Gold price ({gold_price}) ≠ Silver price ({silver_price})")
