import pytest
from datetime import datetime

from app.common import validation_utils


def test_validate_plan_year_valid():
    result = validation_utils.validate_plan_year('2025-26')
    assert result == '2025-26'


def test_validate_plan_year_with_spaces():
    result = validation_utils.validate_plan_year(' 2025-26 ')
    assert result == '2025-26'


def test_validate_plan_year_none_returns_none():
    result = validation_utils.validate_plan_year(None)
    assert result is None


def test_validate_plan_year_invalid_format_raises():
    with pytest.raises(validation_utils.PlanYearError):
        validation_utils.validate_plan_year('202526')


def test_validate_plan_year_invalid_suffix_raises():
    with pytest.raises(validation_utils.PlanYearError):
        validation_utils.validate_plan_year('2025-27')


def test_validate_plan_year_below_minimum_raises():
    with pytest.raises(validation_utils.PlanYearError):
        validation_utils.validate_plan_year('1999-00')


def test_validate_plan_year_beyond_maximum_raises(monkeypatch):
    class MockDatetime:
        @classmethod
        def now(cls):
            return datetime(2026, 9, 21)

    monkeypatch.setattr(
        validation_utils,
        'datetime',
        MockDatetime
    )

    with pytest.raises(validation_utils.PlanYearError):
        validation_utils.validate_plan_year('2029-30')