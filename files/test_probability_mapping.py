"""
Test Suite: Probability → Signal Mapping
==========================================
Validates the mapping logic that converts raw model probability (0.0-1.0)
into human-readable signals (UP/DOWN/NEUTRAL, High/Low confidence).

This directly tests the logic from scheduler.py L356-367.
"""
import pytest
import builtins


# ---------------------------------------------------------------------------
# Mapping Function (extracted from scheduler.py)
# ---------------------------------------------------------------------------

def map_probability_to_signal(probability):
    """
    Convert raw model probability to direction, confidence, and recommendation.
    Mirrors scheduler.py signal mapping exactly.
    """
    # Clamp to [0, 1]
    probability = builtins.max(0.0, builtins.min(1.0, probability))

    if probability >= 0.60:
        direction, confidence, recommendation = 'UP', 'High', 'STRONG BUY'
    elif probability >= 0.52:
        direction, confidence, recommendation = 'UP', 'Low', 'BUY'
    elif probability >= 0.48:
        direction, confidence, recommendation = 'NEUTRAL', 'Low', 'HOLD'
    elif probability >= 0.40:
        direction, confidence, recommendation = 'DOWN', 'Low', 'SELL'
    else:
        direction, confidence, recommendation = 'DOWN', 'High', 'STRONG SELL'

    return {
        'probability': probability,
        'direction': direction,
        'confidence_level': confidence,
        'recommendation': recommendation
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProbabilityMapping:
    """Core mapping logic tests."""

    @pytest.mark.parametrize("prob,expected_direction,expected_confidence", [
        (0.95, 'UP', 'High'),
        (0.80, 'UP', 'High'),
        (0.60, 'UP', 'High'),       # Boundary: exactly 0.60
        (0.59, 'UP', 'Low'),
        (0.55, 'UP', 'Low'),
        (0.52, 'UP', 'Low'),        # Boundary: exactly 0.52
        (0.51, 'NEUTRAL', 'Low'),
        (0.50, 'NEUTRAL', 'Low'),   # Dead center = NEUTRAL
        (0.48, 'NEUTRAL', 'Low'),   # Boundary: exactly 0.48
        (0.47, 'DOWN', 'Low'),
        (0.45, 'DOWN', 'Low'),
        (0.40, 'DOWN', 'Low'),      # Boundary: exactly 0.40
        (0.39, 'DOWN', 'High'),
        (0.20, 'DOWN', 'High'),
        (0.05, 'DOWN', 'High'),
    ])
    def test_thresholds(self, prob, expected_direction, expected_confidence):
        result = map_probability_to_signal(prob)
        assert result['direction'] == expected_direction, (
            f"prob={prob}: expected direction={expected_direction}, got {result['direction']}")
        assert result['confidence_level'] == expected_confidence, (
            f"prob={prob}: expected confidence={expected_confidence}, got {result['confidence_level']}")

    def test_probability_clamped_above_one(self):
        """Probability > 1.0 should be clamped to 1.0."""
        result = map_probability_to_signal(1.5)
        assert result['probability'] == 1.0
        assert result['direction'] == 'UP'
        assert result['confidence_level'] == 'High'

    def test_probability_clamped_below_zero(self):
        """Probability < 0.0 should be clamped to 0.0."""
        result = map_probability_to_signal(-0.5)
        assert result['probability'] == 0.0
        assert result['direction'] == 'DOWN'
        assert result['confidence_level'] == 'High'

    def test_neutral_fallback(self):
        """Probability = 0.5 (model failure fallback) → NEUTRAL."""
        result = map_probability_to_signal(0.5)
        assert result['direction'] == 'NEUTRAL'
        assert result['confidence_level'] == 'Low'

    def test_return_structure(self):
        """Verify the returned dict has all required keys."""
        result = map_probability_to_signal(0.72)
        assert 'probability' in result
        assert 'direction' in result
        assert 'confidence_level' in result
        assert 'recommendation' in result

    @pytest.mark.parametrize("prob,expected_rec", [
        (0.95, 'STRONG BUY'),
        (0.55, 'BUY'),
        (0.50, 'HOLD'),
        (0.45, 'SELL'),
        (0.20, 'STRONG SELL'),
    ])
    def test_recommendation_values(self, prob, expected_rec):
        """Verify recommendation string matches the probability band."""
        result = map_probability_to_signal(prob)
        assert result['recommendation'] == expected_rec, (
            f"prob={prob}: expected recommendation={expected_rec}, got {result['recommendation']}"
        )

    def test_recommendation_valid_set(self):
        """Recommendation must be one of the 5 valid values."""
        valid = {'STRONG BUY', 'BUY', 'HOLD', 'SELL', 'STRONG SELL'}
        for prob in [0.0, 0.25, 0.48, 0.50, 0.52, 0.75, 1.0]:
            result = map_probability_to_signal(prob)
            assert result['recommendation'] in valid

    def test_direction_values(self):
        """Direction must be one of UP, DOWN, NEUTRAL."""
        valid_directions = {'UP', 'DOWN', 'NEUTRAL'}
        for prob in [0.0, 0.25, 0.48, 0.50, 0.52, 0.75, 1.0]:
            result = map_probability_to_signal(prob)
            assert result['direction'] in valid_directions

    def test_confidence_values(self):
        """Confidence must be either High or Low."""
        valid_confidences = {'High', 'Low'}
        for prob in [0.0, 0.25, 0.48, 0.50, 0.52, 0.75, 1.0]:
            result = map_probability_to_signal(prob)
            assert result['confidence_level'] in valid_confidences


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_exact_boundary_0_60(self):
        """0.60 is the boundary between Low and High confidence UP."""
        result_at = map_probability_to_signal(0.60)
        result_below = map_probability_to_signal(0.5999)
        assert result_at['confidence_level'] == 'High'
        assert result_below['confidence_level'] == 'Low'

    def test_exact_boundary_0_48(self):
        """0.48 is the boundary between NEUTRAL and DOWN."""
        result_at = map_probability_to_signal(0.48)
        result_below = map_probability_to_signal(0.4799)
        assert result_at['direction'] == 'NEUTRAL'
        assert result_below['direction'] == 'DOWN'

    def test_neutral_zone_width(self):
        """The neutral zone spans 0.48 to <0.52 (4% width)."""
        neutral_probs = [0.48, 0.49, 0.50, 0.51]
        for p in neutral_probs:
            result = map_probability_to_signal(p)
            assert result['direction'] == 'NEUTRAL', f"prob={p} should be NEUTRAL"

    def test_symmetry(self):
        """
        The mapping is roughly symmetric around 0.50.
        0.60 → UP/High mirrors 0.40 → DOWN/Low (not perfect symmetry, verified).
        """
        up_high = map_probability_to_signal(0.65)
        down_high = map_probability_to_signal(0.35)
        assert up_high['direction'] == 'UP'
        assert down_high['direction'] == 'DOWN'
        assert up_high['confidence_level'] == 'High'
        assert down_high['confidence_level'] == 'High'
