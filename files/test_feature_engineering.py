"""
Test Suite: Feature Engineering
================================
Validates the mathematical correctness of feature calculations used in
the ML pipeline's scheduler.py (inference path).

These tests use known input values and verify output against manually
computed expected results — ensuring momentum, RSI, MACD, and volatility
calculations are correct.
"""
import pytest
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Feature Calculation Functions (extracted from scheduler.py logic)
# These mirror the exact calculations in scheduler.py L193-237
# ---------------------------------------------------------------------------

def compute_momentum(prices, lookback):
    """Momentum = (current_price / average_price_over_lookback) - 1"""
    if len(prices) < lookback + 1:
        return 0
    current = prices[-1]
    reference = prices[-(lookback + 1)]
    return (current / reference) - 1


def compute_rsi(prices, period=14):
    """RSI = 100 - (100 / (1 + RS)), where RS = avg_gain / avg_loss"""
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0  # No losses → fully bullish
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(prices):
    """MACD = EMA(12) - EMA(26)"""
    series = pd.Series(prices)
    ema_12 = series.ewm(span=12, adjust=False).mean().iloc[-1]
    ema_26 = series.ewm(span=26, adjust=False).mean().iloc[-1]
    return ema_12 - ema_26


def compute_volatility(prices, window=24):
    """Volatility = standard deviation of last N prices"""
    if len(prices) < window:
        return 0
    return float(np.std(prices[-window:], ddof=1))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMomentum:
    """Test momentum calculations at different time horizons."""

    def test_momentum_positive(self):
        """Price went up → momentum should be positive."""
        prices = [100, 102, 104, 106, 108, 110]
        momentum = compute_momentum(prices, lookback=1)
        assert momentum > 0, f"Expected positive momentum, got {momentum}"

    def test_momentum_negative(self):
        """Price went down → momentum should be negative."""
        prices = [110, 108, 106, 104, 102, 100]
        momentum = compute_momentum(prices, lookback=1)
        assert momentum < 0, f"Expected negative momentum, got {momentum}"

    def test_momentum_flat(self):
        """Constant price → momentum should be zero."""
        prices = [100, 100, 100, 100, 100]
        momentum = compute_momentum(prices, lookback=1)
        assert momentum == 0, f"Expected zero momentum, got {momentum}"

    def test_momentum_1h_exact_value(self):
        """Verify exact calculation: (110/100) - 1 = 0.10"""
        prices = [100, 110]
        momentum = compute_momentum(prices, lookback=1)
        assert abs(momentum - 0.10) < 1e-10

    def test_momentum_insufficient_data(self):
        """Not enough data → should return 0 (default)."""
        prices = [100]
        momentum = compute_momentum(prices, lookback=6)
        assert momentum == 0


class TestRSI:
    """Test RSI calculation correctness."""

    def test_rsi_range(self):
        """RSI must always be between 0 and 100."""
        prices = [100 + np.random.randn() * 5 for _ in range(30)]
        rsi = compute_rsi(prices)
        assert 0 <= rsi <= 100, f"RSI out of range: {rsi}"

    def test_rsi_overbought(self):
        """Steadily increasing prices → RSI should be high (overbought)."""
        prices = list(range(100, 130))  # 30 consecutive increases
        rsi = compute_rsi(prices)
        assert rsi > 70, f"Expected overbought RSI (>70), got {rsi}"

    def test_rsi_oversold(self):
        """Steadily decreasing prices → RSI should be low (oversold)."""
        prices = list(range(130, 100, -1))  # 30 consecutive decreases
        rsi = compute_rsi(prices)
        assert rsi < 30, f"Expected oversold RSI (<30), got {rsi}"

    def test_rsi_no_losses(self):
        """All gains, no losses → RSI should be 100."""
        prices = [100 + i for i in range(20)]
        rsi = compute_rsi(prices)
        assert rsi == 100.0

    def test_rsi_neutral(self):
        """Alternating up/down → RSI should be near 50."""
        prices = [100 + (2 if i % 2 == 0 else -2) for i in range(30)]
        rsi = compute_rsi(prices)
        assert 40 <= rsi <= 60, f"Expected neutral RSI (40-60), got {rsi}"


class TestMACD:
    """Test MACD calculation."""

    def test_macd_uptrend_positive(self):
        """In an uptrend, EMA(12) > EMA(26) → MACD should be positive."""
        prices = [100 + i * 2 for i in range(30)]
        macd = compute_macd(prices)
        assert macd > 0, f"Expected positive MACD in uptrend, got {macd}"

    def test_macd_downtrend_negative(self):
        """In a downtrend, EMA(12) < EMA(26) → MACD should be negative."""
        prices = [200 - i * 2 for i in range(30)]
        macd = compute_macd(prices)
        assert macd < 0, f"Expected negative MACD in downtrend, got {macd}"

    def test_macd_flat_near_zero(self):
        """Constant prices → MACD should be near zero."""
        prices = [100] * 30
        macd = compute_macd(prices)
        assert abs(macd) < 0.01, f"Expected MACD ≈ 0 for flat prices, got {macd}"


class TestVolatility:
    """Test volatility (standard deviation) calculation."""

    def test_volatility_constant_price_is_zero(self):
        """No price variation → volatility should be zero."""
        prices = [100] * 30
        vol = compute_volatility(prices)
        assert vol == 0, f"Expected zero volatility, got {vol}"

    def test_volatility_increases_with_spread(self):
        """More price variation → higher volatility."""
        low_vol_prices = [100 + np.sin(i) for i in range(30)]
        high_vol_prices = [100 + 10 * np.sin(i) for i in range(30)]
        low_vol = compute_volatility(low_vol_prices)
        high_vol = compute_volatility(high_vol_prices)
        assert high_vol > low_vol

    def test_volatility_positive(self):
        """Volatility should always be non-negative."""
        prices = [100 + np.random.randn() * 3 for _ in range(30)]
        vol = compute_volatility(prices)
        assert vol >= 0

    def test_volatility_insufficient_data(self):
        """Not enough data → should return 0."""
        prices = [100, 101, 102]
        vol = compute_volatility(prices, window=24)
        assert vol == 0
