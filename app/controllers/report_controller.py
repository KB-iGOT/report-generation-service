from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.services.report_service import ReportService
from datetime import datetime, time
import logging
import gc
import ctypes
import time as time_module
from app.authentication.AccessTokenValidator import AccessTokenValidator
from constants import X_AUTHENTICATED_USER_TOKEN, IS_VALIDATION_ENABLED, X_ORG_ID, APAR_FILTER_KEY, TEXT_CSV_HOLDER, LIBC_SO_6, MALLOC_TRIM_HOLDER_MSG
from errormsg import MISSING_X_ORG_ID_HEADER, ORGANIZATION_ID_REQUIRED_ERROR, INVALID_DATE_FORMAT_ERROR, UNEXPECTED_ERROR_OCCURRED, MALLOC_TRIM_HOLDER_ERROR_MSG
from app.services.GcsToBigQuerySyncService import GcsToBigQuerySyncService

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

report_controller = Blueprint('report_controller', __name__)

@report_controller.route('/report/org/enrolment/<org_id>', methods=['POST'])
def get_report(org_id):
    start_timer = time_module.time()
    try:
        logger.info(f"Received request to generate report for org_id={org_id}")
        x_org_id = request.headers.get(X_ORG_ID)
        if not validate_org_id(x_org_id, org_id):
            return jsonify({'error': ORGANIZATION_ID_REQUIRED_ERROR}), 400

        if IS_VALIDATION_ENABLED.lower() == 'true' and not validate_user_token(org_id):
            return jsonify({'error': 'Authentication failed.'}), 401

        data = request.get_json()
        start_date, end_date = parse_and_validate_dates(data)

        if (end_date - start_date).days > 365:
            logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        csv_data = generate_report(org_id, start_date, end_date, data)
        if not csv_data:
            return jsonify({'error': 'No data found for the given organization ID.'}), 404

        return create_csv_response(csv_data, org_id, start_timer)

    except KeyError as e:
        return handle_key_error(e)

    except ValueError as e:
        return handle_value_error(e)

    except FileNotFoundError as e:
        return handle_file_not_found_error(e)

    except Exception as e:
        return handle_generic_error(e)

    finally:
        cleanup_memory()

@report_controller.route('/report/user/sync/<org_id>', methods=['POST'])
def get_user_report(org_id):
    try:
        start_timer = time_module.time()
        logger.info("Received request to generate user report")
        
        # Validate headers and organization ID
        validation_response = validate_headers_and_org(x_org_id=request.headers.get(X_ORG_ID), org_id=org_id)
        if validation_response:
            return validation_response

        # Parse and validate input parameters
        data, validation_response = parse_and_validate_user_report_input(request.get_json())
        if validation_response:
            return validation_response

        # Generate the report
        response = generate_user_report(data, org_id, start_timer)
        return response

    except KeyError as e:
        return handle_key_error(e)

    except Exception as e:
        return handle_generic_error(e)

    finally:
        cleanup_memory()

@report_controller.route('/report/org/user/<org_id>', methods=['POST'])
def get_org_user_report(org_id):
    try:
        start_timer = time_module.time()
        logger.info("Received request to generate user report")
        x_org_id = request.headers.get(X_ORG_ID)
        logger.info(f"Received x_org_id={x_org_id}")
        if not x_org_id:
            logger.error("Missing 'x_org_id' in headers.")
            return jsonify({'error': 'Organization ID is required.'}), 400
        if not ReportService.is_valid_org(x_org_id, org_id):
            logger.error(f"Invalid organization ID: {org_id}")
            return jsonify({'error': f'Not authorized to view the report for : {org_id}'}), 401        
        # Parse and validate input parameters
        data = request.get_json()
        if not data:
            logger.error("Request body is missing")
            return jsonify({'error': 'Request body is missing.'}), 400

        user_creation_start_date = data.get('user_creation_start_date')
        user_creation_end_date = data.get('user_creation_end_date')

        # Validate date range if provided
        if user_creation_start_date and user_creation_end_date:
            try:
                user_creation_start_date = datetime.strptime(user_creation_start_date, '%Y-%m-%d')
                user_creation_end_date = datetime.strptime(user_creation_end_date, '%Y-%m-%d')
                user_creation_start_date = datetime.combine(user_creation_start_date.date(), time.min)  # 00:00:00
                user_creation_end_date = datetime.combine(user_creation_end_date.date(), time.max)      # 23:59:59.999999
            except ValueError:
                return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400

        # New parameters from request body
        is_full_report_required = data.get('isFullReportRequired', False)
        required_columns = data.get('required_columns', [])


        logger.info(f"Generating user report for orgId={org_id}")
        
        try:
            csv_data = ReportService.fetch_master_user_data(
                org_id, is_full_report_required, required_columns=required_columns, user_creation_start_date=user_creation_start_date, user_creation_end_date=user_creation_end_date
            )

            if not csv_data:
                logger.warning(f"No data found for orgId={org_id}")
                return jsonify({'error': 'No data found for the given org details.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for orgId: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Org User Report generated successfully for  in {time_taken} seconds for orgId={org_id}")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": 'attachment; filename="user-report.csv"'
            }
        )
        
         # Explicitly trigger garbage collection to free up memory
        del csv_data
        gc.collect()

        return response

    except KeyError as e:
        error_message = str(e)
        logger.error(f"Missing required fields in request: {error_message}")
        return jsonify({'error': 'Invalid input. Please provide valid parameters.', 'details': error_message}), 400

    except Exception as e:
        error_message = str(e)
        logger.exception(f"Unexpected error occurred: {error_message}")
        return jsonify({'error': 'An unexpected error occurred. Please try again later.', 'details': error_message}), 500
    finally: 
        gc.collect()
        try:
            logger.info("inside malloc_trim:")
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception as e:
            logger.exception("malloc_trim failed: %s", str(e))

@report_controller.route('/gcs-to-bq/sync', methods=['GET'])
def sync_gcs_to_bq():
    try:
        sync_service = GcsToBigQuerySyncService()
        sync_service.sync_all_tables()
        return jsonify({"status": "success", "message": "All tables synced successfully"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@report_controller.route('/report/apar/enrolment', methods=['POST'])
def get_apar_report():
    start_timer = time_module.time()
    try:
        logger.info("Received request to generate APAR report")
        data = request.get_json()

        # Parse and validate request fields
        enrolment_start_date, enrolment_end_date, filters, required_columns = parse_apar_request(data)

        # Validate filters and date range
        validate_apar_filters(filters)
        validate_apar_date_range(enrolment_start_date, enrolment_end_date)

        # Generate the report
        return generate_apar_report(enrolment_start_date, enrolment_end_date, filters, required_columns, start_timer)

    except KeyError as e:
        return handle_key_error(e)

    except ValueError as e:
        return handle_value_error(e)

    except FileNotFoundError as e:
        return handle_file_not_found_error(e)

    except Exception as e:
        return handle_generic_error(e)

    finally:
        cleanup_memory()


def parse_apar_request(data):
    if not data or 'enrolment_start_date' not in data or 'enrolment_end_date' not in data:
        raise KeyError("Missing 'enrolment_start_date' or 'enrolment_end_date' in request body.")
    enrolment_start_date = data['enrolment_start_date']
    enrolment_end_date = data['enrolment_end_date']
    filters = data.get('filters', {})
    required_columns = data.get('required_columns', [])
    return enrolment_start_date, enrolment_end_date, filters, required_columns


def validate_apar_filters(filters):
    if filters:
        allowed_keys = APAR_FILTER_KEY.split(',')
        for key in filters:
            if key not in allowed_keys:
                raise ValueError(f"Invalid filter key '{key}'. Allowed keys: {', '.join(allowed_keys)}")
        if not any(filters.get(k) for k in allowed_keys):
            raise ValueError(f"At least one of {', '.join(allowed_keys)} must be provided in filters.")


def validate_apar_date_range(enrolment_start_date, enrolment_end_date):
    start_date = datetime.strptime(enrolment_start_date, '%Y-%m-%d')
    end_date = datetime.strptime(enrolment_end_date, '%Y-%m-%d')
    if (end_date - start_date).days > 365:
        raise ValueError("Date range cannot exceed 1 year")


def generate_apar_report(enrolment_start_date, enrolment_end_date, filters, required_columns, start_timer):
    logger.info(f"Generating APAR report from {enrolment_start_date} to {enrolment_end_date} with filters: {filters}")
    csv_data = ReportService.fetch_apar_enrolment_report(
        enrolment_start_date, enrolment_end_date, filters, required_columns
    )
    if not csv_data:
        logger.warning(f"No data found for the given date range: {enrolment_start_date} to {enrolment_end_date}")
        return jsonify({'error': 'No data found for the given filters/date range.'}), 404

    time_taken = round(time_module.time() - start_timer, 2)
    logger.info(f"APAR Report generated successfully in {time_taken} seconds")

    response = Response(
        stream_with_context(csv_data),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="report.csv"'}
    )
    del csv_data
    gc.collect()
    return response


def validate_org_id(x_org_id, org_id):
    logger.info(f"Received x_org_id={x_org_id}")
    if not x_org_id:
        logger.error(MISSING_X_ORG_ID_HEADER)
        return False
    if not ReportService.is_valid_org(x_org_id, org_id):
        logger.error(f"Invalid organization ID: {org_id}")
        return False
    return True


def validate_user_token(org_id):
    user_token = request.headers.get(X_AUTHENTICATED_USER_TOKEN)
    if not user_token:
        logger.error("Missing 'x-authenticated-user-token' in headers.")
        return False

    user_org_id = AccessTokenValidator.verify_user_token_get_org(user_token, True)
    if not user_org_id or user_org_id != org_id:
        logger.error("Invalid or expired authentication token.")
        return False
    return True


def parse_and_validate_dates(data):
    if not data or 'start_date' not in data or 'end_date' not in data:
        raise KeyError("Missing 'start_date' or 'end_date' in request body.")
    start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')
    return datetime.combine(start_date.date(), time.min), datetime.combine(end_date.date(), time.max)


def generate_report(org_id, start_date, end_date, data):
    is_full_report_required = data.get('isFullReportRequired', False)
    required_columns = data.get('required_columns', [])
    logger.info(f"Generating report for org_id={org_id} from {start_date} to {end_date}")
    return ReportService.fetch_master_enrolments_data(
        start_date, end_date, org_id, is_full_report_required, required_columns=required_columns
    )


def create_csv_response(csv_data, org_id, start_timer):
    time_taken = round(time_module.time() - start_timer, 2)
    logger.info(f"Report generated successfully for org_id={org_id} in {time_taken} seconds")
    response = Response(
        stream_with_context(csv_data),
        mimetype=TEXT_CSV_HOLDER,
        headers={"Content-Disposition": f'attachment; filename="report_{org_id}.csv"'}
    )
    del csv_data
    gc.collect()
    return response


def handle_key_error(e):
    error_message = str(e)
    logger.error(f"Missing required fields in request: {error_message}")
    return jsonify({'error': 'Invalid input. Please provide start_date and end_date.', 'details': error_message}), 400


def handle_value_error(e):
    error_message = str(e)
    logger.error(f"Invalid date format in request: {error_message}")
    return jsonify({'error': INVALID_DATE_FORMAT_ERROR, 'details': error_message}), 400


def handle_file_not_found_error(e):
    error_message = str(e)
    logger.error(f"File not found during report generation: {error_message}")
    return jsonify({'error': 'Report file could not be generated.', 'details': error_message}), 500


def handle_generic_error(e):
    error_message = str(e)
    logger.exception(f"Unexpected error occurred: {error_message}")
    return jsonify({'error': UNEXPECTED_ERROR_OCCURRED, 'details': error_message}), 500


def cleanup_memory():
    gc.collect()
    try:
        logger.info(MALLOC_TRIM_HOLDER_MSG)
        ctypes.CDLL(LIBC_SO_6).malloc_trim(0)
    except Exception as e:
        logger.exception(MALLOC_TRIM_HOLDER_ERROR_MSG, str(e))

def validate_headers_and_org(x_org_id, org_id):
    logger.info(f"Received x_org_id={x_org_id}")
    if not x_org_id:
        logger.error("Missing 'x_org_id' in headers.")
        return jsonify({'error': 'Organization ID is required.'}), 400
    if not ReportService.is_valid_org(x_org_id, org_id):
        logger.error(f"Invalid organization ID: {org_id}")
        return jsonify({'error': f'Not authorized to view the report for : {org_id}'}), 401
    return None


def parse_and_validate_user_report_input(data):
    if not data:
        logger.error("Request body is missing")
        return None, jsonify({'error': 'Request body is missing.'}), 400

    user_email = data.get('userEmail', '').strip()
    user_phone = data.get('userPhone', '').strip()
    ehrms_id = data.get('ehrmsId', '').strip()

    if not (user_email or user_phone or ehrms_id):
        logger.error("At least one of 'userEmail', 'userPhone', or 'ehrmsId' must be provided.")
        return None, jsonify({'error': "At least one of 'userEmail', 'userPhone', or 'ehrmsId' must be provided."}), 400

    start_date, end_date = parse_date_range(data.get('start_date'), data.get('end_date'))
    if isinstance(start_date, Response):
        return None, start_date

    required_columns = data.get('required_columns', [])
    return {'user_email': user_email, 'user_phone': user_phone, 'ehrms_id': ehrms_id, 'start_date': start_date, 'end_date': end_date, 'required_columns': required_columns}, None


def parse_date_range(start_date, end_date):
    if start_date and end_date:
        try:
            start_date = datetime.combine(datetime.strptime(start_date, '%Y-%m-%d').date(), time.min)
            end_date = datetime.combine(datetime.strptime(end_date, '%Y-%m-%d').date(), time.max)
        except ValueError:
            return jsonify({'error': INVALID_DATE_FORMAT_ERROR}), 400
    return start_date, end_date


def generate_user_report(data, org_id, start_timer):
    try:
        csv_data = ReportService.fetch_user_cumulative_report(
            data['user_email'], data['user_phone'], data['ehrms_id'], data['start_date'], data['end_date'], org_id,
            data['required_columns']
        )

        if not csv_data:
            logger.warning(f"No data found for userEmail={data['user_email']}, userPhone={data['user_phone']}, ehrmsId={data['ehrms_id']}")
            return jsonify({'error': 'No data found for the given user details.'}), 404

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype=TEXT_CSV_HOLDER,
            headers={"Content-Disposition": 'attachment; filename="user-report.csv"'}
        )
        del csv_data
        gc.collect()
        return response

    except Exception as e:
        error_message = str(e)
        logger.error(f"Error generating CSV stream: {error_message}")
        return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500
