import pytest

from logic import is_decimal_milestone, parse_strict_true_false


def test_strict_feature_flag_accepts_only_documented_values() -> None:
    assert parse_strict_true_false("true", setting_name="FEATURE") is True
    assert parse_strict_true_false("false", setting_name="FEATURE") is False
    with pytest.raises(ValueError, match="FEATURE must be exactly"):
        parse_strict_true_false("yes", setting_name="FEATURE")


def test_decimal_milestones_are_positive_powers_of_ten() -> None:
    assert is_decimal_milestone(1)
    assert is_decimal_milestone(1_000)
    assert not is_decimal_milestone(0)
    assert not is_decimal_milestone(11)
