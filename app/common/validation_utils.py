import re
from datetime import datetime


MIN_PLAN_YEAR = 2000
MAX_PLAN_YEAR_OFFSET = 2
PLAN_YEAR_PATTERN = re.compile(r'^(\d{4})-(\d{2})$')

class PlanYearError(ValueError):
    pass


def validate_plan_year(plan_year):
    """
    Validate financial year in YYYY-YY format.

    Returns None when plan_year is not provided.
    """
    if plan_year is None:
        return None

    match = PLAN_YEAR_PATTERN.match(str(plan_year).strip())

    if not match:
        raise PlanYearError(
            f"plan_year must be in financial year format YYYY-YY "
            f"(e.g. 2025-26), got: {plan_year!r}"
        )

    start_year = int(match.group(1))
    expected_suffix = f"{(start_year + 1) % 100:02d}"

    if match.group(2) != expected_suffix:
        raise PlanYearError(
            f"plan_year must be a valid financial year, "
            f"expected {start_year}-{expected_suffix}, got: {plan_year!r}"
        )

    today = datetime.now()
    current_fy_start_year = (
        today.year if today.month >= 4 else today.year - 1
    )

    max_fy_start_year = current_fy_start_year + MAX_PLAN_YEAR_OFFSET

    if not (MIN_PLAN_YEAR <= start_year <= max_fy_start_year):
        min_fy = f"{MIN_PLAN_YEAR}-{(MIN_PLAN_YEAR + 1) % 100:02d}"
        max_fy = (
            f"{max_fy_start_year}-"
            f"{(max_fy_start_year + 1) % 100:02d}"
        )

        raise PlanYearError(
            f"plan_year must be between {min_fy} and {max_fy}, "
            f"got: {plan_year!r}"
        )

    return f"{start_year}-{expected_suffix}"