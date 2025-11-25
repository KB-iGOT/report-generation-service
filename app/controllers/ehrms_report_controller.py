from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.services.ehrms_report_service import EhrmsReportService
from datetime import datetime, time
import logging
import gc
import ctypes
import time as time_module
from constants import TEXT_CSV_HOLDER,UNEXPECTED_ERROR_MESSAGE

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


ehrms_report_controller = Blueprint('ehrms_report_controller', __name__)

@ehrms_report_controller.route('/report/ehrms/org/enrolment', methods=['POST'])
def get_user_enrolment_report():
    start_timer = time_module.time()
    try:
        logger.info("Received request to generate report for ehrms")
    
        data = request.get_json()
        start_date, end_date = _parse_and_validate_dates(data)
        if not start_date or not end_date:
            return jsonify({'error': 'Invalid input. Please provide start_date and end_date.'}), 400

        required_columns = data.get('required_columns', [])

        if (end_date - start_date).days > 365:
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        csv_data = _generate_report(start_date, end_date, required_columns)
        if not csv_data:
            return jsonify({'error': 'No data found for the given  date range.'}), 404

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully for ehrms in {time_taken} seconds")

        return _create_csv_response(csv_data)

    except Exception as e:
        return _handle_exception(e)
    finally:
        _cleanup()

@ehrms_report_controller.route('/report/ehrms/user/sync', methods=['POST'])
def get_user_report():
    try:
        start_timer = time_module.time()
        logger.info("Received request to generate user report")
        
        data = _validate_and_get_request_data()
        user_email, user_phone, ehrms_id = _extract_and_validate_user_identifiers(data)
        start_date, end_date = _parse_and_validate_dates(data)
        required_columns = data.get('required_columns', [])

        logger.info(f"Generating user report for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}")
        
        csv_data = _generate_user_report(user_email, user_phone, ehrms_id, start_date, end_date, required_columns)
        response = _create_csv_response(csv_data, "user-enrolment-report.csv")

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully in {time_taken} seconds")
        return response

    except KeyError as e:
        return _error_response(f"Key error: {str(e)}", 400)
    except Exception as e:
        return _handle_exception(e)
    finally:
        _cleanup()

@ehrms_report_controller.route('/report/ehrms/org/user', methods=['POST'])
def get_org_user_report():
    start_timer = time_module.time()
    try:
        logger.info("Received request to generate organization user report")
        
        data = request.get_json()
        if not data:
            logger.error("Request body is missing")
            return _error_response("Request body is missing.", 400)

        # Parse both possible date ranges
        user_creation_start_date, user_creation_end_date = _validate_and_parse_date_range(
            data, start_key='user_creation_start_date', end_key='user_creation_end_date'
        )
        user_updated_start_date, user_updated_end_date = _validate_and_parse_date_range(
            data, start_key='user_updated_start_date', end_key='user_updated_end_date'
        )

        # Require at least one complete date-pair
        if not (user_creation_start_date and user_creation_end_date) and not (user_updated_start_date and user_updated_end_date):
            logger.error("Neither user_creation nor user_updated date ranges provided")
            return _error_response("At least one of user_creation_(start|end)_date or user_updated_(start|end)_date must be provided.", 400)

        # Prefer updated range if provided, otherwise use creation range
        if user_updated_start_date and user_updated_end_date:
            chosen_start, chosen_end = user_updated_start_date, user_updated_end_date
        else:
            chosen_start, chosen_end = user_creation_start_date, user_creation_end_date

        # Validate chosen date range does not exceed 1 year
        if (chosen_end - chosen_start).days > 365:
            logger.error("Date range cannot exceed 1 year")
            return _error_response("Date range cannot exceed 1 year", 400)

        required_columns = data.get('required_columns', [])

        # Fetch report data - pass both ranges (one may be None) so service can validate which to use
        csv_data = EhrmsReportService.fetch_master_user_data(
            required_columns=required_columns,
            user_creation_start_date=user_creation_start_date,
            user_creation_end_date=user_creation_end_date,
            user_updated_start_date=user_updated_start_date,
            user_updated_end_date=user_updated_end_date
        )

        if not csv_data:
            logger.warning("No data found for the date range.")
            return _error_response("No data found for the given date range.", 404)

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Organization User Report generated successfully in {time_taken} seconds.")
        return _create_csv_response(csv_data, filename="org-user-report.csv")

    except KeyError as e:
        logger.error(f"Missing required fields in request: {str(e)}")
        return _error_response("Invalid input. Please provide valid parameters.", 400)

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return _error_response("Invalid input. Please provide valid parameters.", 400)

    except Exception as e:
        logger.exception(f"Unexpected error occurred: {str(e)}")
        return _handle_exception(e)

    finally:
        _cleanup()


def _validate_and_parse_date_range(data, start_key='start_date', end_key='end_date'):
    """
    Validates and parses the date range from the request data.
    Returns (start_datetime, end_datetime) or (None, None) if not present.
    """
    start_val = data.get(start_key)
    end_val = data.get(end_key)

    if start_val and end_val:
        try:
            start_date = datetime.strptime(start_val, '%Y-%m-%d')
            end_date = datetime.strptime(end_val, '%Y-%m-%d')
            return datetime.combine(start_date.date(), time.min), datetime.combine(end_date.date(), time.max)
        except ValueError:
            raise ValueError(f"Invalid date format for {start_key} or {end_key}. Use YYYY-MM-DD.")
    return None, None


def _parse_and_validate_dates(data):
    try:
        start_date = datetime.strptime(data['enrolment_start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['enrolment_end_date'], '%Y-%m-%d')
        return datetime.combine(start_date.date(), time.min), datetime.combine(end_date.date(), time.max)
    except (KeyError, ValueError):
        logger.error("Invalid or missing date fields in request.")
        return None, None


def _generate_report(start_date, end_date, required_columns):
    try:
        return EhrmsReportService.fetch_master_enrolments_data(
            start_date, end_date, required_columns=required_columns
        )
    except Exception as e:
        logger.error(f"Error generating CSV stream: {str(e)}")
        raise


def _create_csv_response(csv_data, filename="report.csv"):
    response = Response(
        stream_with_context(csv_data),
        mimetype=TEXT_CSV_HOLDER,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
    del csv_data
    gc.collect()
    return response


def _error_response(message, status_code):
    logger.error(message)
    return jsonify({'error': message}), status_code

def _handle_exception(e):
    logger.exception(f"Unexpected error occurred: {str(e)}")
    return jsonify({'error': UNEXPECTED_ERROR_MESSAGE, 'details': str(e)}), 500


def _cleanup():
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception as e:
        logger.exception("malloc_trim failed: %s", str(e))

def _validate_and_get_request_data():
    data = request.get_json()
    if not data:
        logger.error("Request body is missing")
        raise KeyError("Request body is missing.")
    return data


def _extract_and_validate_user_identifiers(data):
    user_email = data.get('userEmail', '').strip()
    user_phone = data.get('userPhone', '').strip()
    ehrms_id = data.get('ehrmsId', '').strip()

    if not (user_email or user_phone or ehrms_id):
        logger.error("At least one of 'userEmail', 'userPhone', or 'ehrmsId' must be provided.")
        raise KeyError("At least one of 'userEmail', 'userPhone', or 'ehrmsId' must be provided.")
    return user_email, user_phone, ehrms_id


def _generate_user_report(user_email, user_phone, ehrms_id, start_date, end_date, required_columns):
    try:
        csv_data = EhrmsReportService.fetch_user_cumulative_report(
            user_email, user_phone, ehrms_id, start_date, end_date, required_columns
        )
        if not csv_data:
            logger.warning("No data found for the given user details.")
            raise ValueError("No data found for the given user details.")
        return csv_data
    except Exception as e:
        logger.error(f"Error generating CSV stream: {str(e)}")
        raise