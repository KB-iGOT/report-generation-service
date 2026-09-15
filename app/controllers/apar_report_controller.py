from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.services.apar_report_service import AparReportService
from datetime import datetime, time
import logging
import gc
import ctypes
from constants import APAR_FILTER_KEY, IS_APAR_DATE_VALIDATION
import time as time_module
import re

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

apar_report_controller = Blueprint('apar_report_controller', __name__)

# trainingPlanYear is optional, but when provided, it must be a valid
# financial year in "YYYY-YY" format (e.g. "2025-26").
# MAX_TRAINING_PLAN_YEAR_OFFSET allows future planning years up to the
# configured limit.
MIN_TRAINING_PLAN_YEAR = 2000
MAX_TRAINING_PLAN_YEAR_OFFSET = 2
TRAINING_PLAN_YEAR_PATTERN = re.compile(r'^(\d{4})-(\d{2})$')

class TrainingPlanYearError(ValueError):
    """
    Raised when the `trainingPlanYear` input is invalid.

    This custom exception allows year-validation errors to be handled
    separately from date-parsing errors. Without it, the error could be
    caught by the generic `ValueError` handler and incorrectly return
    the date-specific message: "Invalid date format. Use YYYY-MM-DD."
    """
    pass

@apar_report_controller.route('/report/apar/assigned/courses', methods=['POST'])
def _get_assigned_courses_apar_report():
    start_timer = time_module.time()
    try:
        logger.info("Received request to generate APAR assigned courses report")
        data = request.get_json()
        validate_request_data(data)

        assigned_on_start_date = data['assigned_on_start_date']
        assigned_on_end_date = data['assigned_on_end_date']
        filters = data.get('filters', {})
        required_columns = data.get('required_columns', [])

        validate_filters(filters)
        # validate_date_range now returns normalized datetimes (start at 00:00:00, end at 23:59:59.999999)
        start_dt, end_dt = validate_date_range(assigned_on_start_date, assigned_on_end_date)
        training_plan_year = validate_training_plan_year(data.get('training_plan_year'))

        logger.info(f"Generating APAR report from {start_dt} to {end_dt} with filters: {filters}, trainingPlanYear: {training_plan_year}")
        csv_data = generate_report(start_dt, end_dt, filters, required_columns, training_plan_year)

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"APAR Report generated successfully in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": 'attachment; filename="report.csv"'
            }
        )

        del csv_data
        gc.collect()
        return response

    except KeyError as e:
        return handle_error(e, "Invalid input. Please provide assigned_on_start_date and assigned_on_end_date.", 400)
    except TrainingPlanYearError as e:
        return handle_error(e, str(e), 400)
    except ValueError as e:
        return handle_error(e, "Invalid date format. Use YYYY-MM-DD.", 400)
    except FileNotFoundError as e:
        return handle_error(e, "Report file could not be generated.", 500)
    except Exception as e:
        return handle_error(e, "An unexpected error occurred. Please try again later.", 500)
    finally:
        gc.collect()
        try:
            logger.info("inside malloc_trim:")
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception as e:
            logger.exception("malloc_trim failed: %s", str(e))


def validate_request_data(data):
    if not data or 'assigned_on_start_date' not in data or 'assigned_on_end_date' not in data:
        raise KeyError("Missing 'assigned_on_start_date' or 'assigned_on_end_date' in request body.")


def validate_filters(filters):
    if filters:
        allowed_keys = APAR_FILTER_KEY.split(',')
        for key in filters:
            if key not in allowed_keys:
                raise ValueError(f"Invalid filter key '{key}'. Allowed keys: {', '.join(allowed_keys)}")
        if not any(filters.get(k) for k in allowed_keys):
            raise ValueError(f"At least one of {', '.join(allowed_keys)} must be provided in filters.")


def validate_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # Normalize to full-day ranges (00:00:00 ... 23:59:59.999999)
    start_date = datetime.combine(start_date.date(), time.min)
    end_date = datetime.combine(end_date.date(), time.max)
    if IS_APAR_DATE_VALIDATION.lower() == 'true':
        if (end_date - start_date).days > 365:
            raise ValueError('Date range cannot exceed 1 year')

    # Return normalized datetimes so callers can use timestamped ranges
    return start_date, end_date


def validate_training_plan_year(training_plan_year):
    """
    Validates the optional trainingPlanYear field.

    Expects a financial year string in "YYYY-YY" format (e.g. "2025-26"),
    and returns it normalized, or None if the field wasn't provided at
    all -- callers/downstream service code should treat None as "no
    year filter applied", same as before this validation existed.

    Rejects anything that isn't in "YYYY-YY" format, anything where the
    second part isn't the two-digit continuation of the first year, and
    anything whose start year is outside a plausible range, before it
    ever reaches the service/DB layer.
    """
    if training_plan_year is None:
        return None

    match = TRAINING_PLAN_YEAR_PATTERN.match(str(training_plan_year).strip())
    if not match:
        raise TrainingPlanYearError(
            f"trainingPlanYear must be in financial year format YYYY-YY (e.g. 2025-26), got: {training_plan_year!r}"
        )

    start_year = int(match.group(1))
    expected_suffix = f"{(start_year + 1) % 100:02d}"
    if match.group(2) != expected_suffix:
        raise TrainingPlanYearError(
            f"trainingPlanYear must be a valid financial year, expected {start_year}-{expected_suffix}, got: {training_plan_year!r}"
        )

    # Financial year runs April to March, so the "current" FY start year
    # rolls over in April rather than on the calendar new year.
    today = datetime.now()
    current_fy_start_year = today.year if today.month >= 4 else today.year - 1
    max_fy_start_year = current_fy_start_year + MAX_TRAINING_PLAN_YEAR_OFFSET

    if not (MIN_TRAINING_PLAN_YEAR <= start_year <= max_fy_start_year):
        min_fy = f"{MIN_TRAINING_PLAN_YEAR}-{(MIN_TRAINING_PLAN_YEAR + 1) % 100:02d}"
        max_fy = f"{max_fy_start_year}-{(max_fy_start_year + 1) % 100:02d}"
        raise TrainingPlanYearError(
            f"trainingPlanYear must be between {min_fy} and {max_fy}, got: {training_plan_year!r}"
        )

    return f"{start_year}-{expected_suffix}"


def generate_report(start_date, end_date, filters, required_columns, training_plan_year=None):
    try:
        csv_data = AparReportService.fetch_apar_assigned_courses_report(start_date, end_date, filters, required_columns, training_plan_year)
        if not csv_data:
            raise FileNotFoundError('No data found for the given filters/date range.')
        return csv_data
    except Exception as e:
        logger.error(f"Error generating CSV stream for APAR report: {str(e)}")
        raise


def handle_error(exception, message, status_code):
    error_message = str(exception)
    logger.error(f"{message}: {error_message}")
    return jsonify({'error': message, 'details': error_message}), status_code