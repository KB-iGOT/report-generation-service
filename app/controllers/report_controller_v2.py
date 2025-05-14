from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.services.report_service_v2 import ReportServiceV2
from app.services.report_service import ReportService
from datetime import datetime, time
import logging
import gc
import ctypes
import time as time_module
from app.authentication.AccessTokenValidator import AccessTokenValidator
from constants import X_AUTHENTICATED_USER_TOKEN, IS_VALIDATION_ENABLED, X_ORG_ID

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

report_controller_v2 = Blueprint('report_controller_v2', __name__)

def _validate_request_common(org_id):
    """Common validation logic for report endpoints"""
    x_org_id = request.headers.get(X_ORG_ID)
    logger.info(f"Received x_org_id={x_org_id}")
    if not x_org_id:
        logger.error("Missing 'x_org_id' in headers.")
        return {'error': 'Organization ID is required.'}, 400
    
    if not ReportService.isValidOrg(x_org_id, org_id):
        logger.error(f"Invalid organization ID: {org_id}")
        return {'error': f'Not authorized to view the report for : {org_id}'}, 401
    
    if IS_VALIDATION_ENABLED.lower() == 'true':
        # Extract and validate user token
        user_token = request.headers.get(X_AUTHENTICATED_USER_TOKEN)
        if not user_token:
            logger.error("Missing 'x-authenticated-user-token' in headers.")
            return {'error': 'Authentication token is required.'}, 401
        
        user_org_id = AccessTokenValidator.verify_user_token_get_org(user_token, True)
        if not user_org_id:
            logger.error("Invalid or expired authentication token.")
            return {'error': 'Invalid or expired authentication token.'}, 401

        logger.info(f"Authenticated user with user_org_id={user_org_id}")
        if user_org_id != org_id:
            logger.error(f"User does not have access to organization ID {org_id}.")
            return {'error': f'Access denied for the specified organization ID {org_id}.'}, 403
    
    return None

def _parse_date_range(data, start_key='start_date', end_key='end_date'):
    """Parse and validate date range from request data"""
    if not data or start_key not in data or end_key not in data:
        raise KeyError(f"Missing '{start_key}' or '{end_key}' in request body.")

    start_date = datetime.strptime(data[start_key], '%Y-%m-%d')
    end_date = datetime.strptime(data[end_key], '%Y-%m-%d')

    start_date = datetime.combine(start_date.date(), time.min)  # 00:00:00
    end_date = datetime.combine(end_date.date(), time.max)      # 23:59:59.999999
    
    # Validate date range
    if (end_date - start_date).days > 365:
        logger.warning(f"Date range exceeds 1 year: {start_key}={start_date}, {end_key}={end_date}")
        return None, None, {'error': 'Date range cannot exceed 1 year'}, 400
    
    return start_date, end_date, None

@report_controller_v2.route('/report/v2/org/enrolment/<org_id>', methods=['POST'])
def get_report(org_id):
    """V2 endpoint for organization enrollment report with advanced filtering"""
    start_timer = time_module.time()
    try:
        logger.info(f"Received request to generate v2 report for org_id={org_id}")
        
        # Validate request
        validation_result = _validate_request_common(org_id)
        if validation_result:
            return jsonify(validation_result[0]), validation_result[1]
        
        # Parse request data
        data = request.get_json()
        if not data:
            logger.error("Request body is missing")
            return jsonify({'error': 'Request body is missing'}), 400
        
        # Parse and validate date range
        try:
            start_date, end_date, error = _parse_date_range(data)
            if error:
                return jsonify(error[0]), error[1]
        except KeyError as e:
            error_message = str(e)
            logger.error(f"Missing required fields in request: {error_message}")
            return jsonify({'error': 'Invalid input. Please provide start_date and end_date.', 'details': error_message}), 400
        except ValueError as e:
            error_message = str(e)
            logger.error(f"Invalid date format in request: {error_message}")
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.', 'details': error_message}), 400
        
        # Get parameters from request body
        is_full_report_required = data.get('isFullReportRequired', False)
        required_columns = data.get('required_columns', [])
        
        # Get additional filters
        additional_filters = data.get('additionalFilter', {})
        
        logger.info(f"Generating v2 report for org_id={org_id} from {start_date} to {end_date}")
        logger.info(f"Additional filters: {additional_filters}")
        
        # Call service to generate report
        try:
            csv_data = ReportServiceV2.generate_report(
                start_date=start_date, 
                end_date=end_date, 
                org_id=org_id, 
                is_full_report_required=is_full_report_required,
                required_columns=required_columns,
                additional_filters=additional_filters
            )

            if not csv_data:
                logger.warning(f"No data found for org_id={org_id} within given filters.")
                return jsonify({'error': 'No data found for the given organization ID and filters.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for org_id={org_id}: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"V2 Report generated successfully for org_id={org_id} in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="report_v2_{org_id}.csv"'
            }
        )

        # Explicitly trigger garbage collection to free up memory
        del csv_data
        gc.collect()

        return response

    except FileNotFoundError as e:
        error_message = str(e)
        logger.error(f"File not found during report generation: {error_message}")
        return jsonify({'error': 'Report file could not be generated.', 'details': error_message}), 500

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

@report_controller_v2.route('/report/v2/user/sync/<orgId>', methods=['POST'])
def get_user_report(orgId):
    """V2 endpoint for user report with advanced filtering"""
    start_timer = time_module.time()
    try:
        logger.info(f"Received request to generate v2 user report for orgId={orgId}")
        
        # Validate request
        validation_result = _validate_request_common(orgId)
        if validation_result:
            return jsonify(validation_result[0]), validation_result[1]
        
        # Parse and validate input parameters
        try:
            data = request.get_json()
            if not data:
                logger.error("Request body is missing")
                return jsonify({'error': 'Request body is missing'}), 400
        except Exception as e:
            logger.error(f"Request body is missing: {str(e)}")
            return jsonify({'error': 'Request body is missing'}), 400

        user_email = data.get('userEmail')
        user_phone = data.get('userPhone')
        ehrms_id = data.get('ehrmsId')

        # Trim whitespace if present
        user_email = user_email.strip() if user_email else None
        user_phone = user_phone.strip() if user_phone else None
        ehrms_id = ehrms_id.strip() if ehrms_id else None

        if not (user_email or user_phone or ehrms_id):
            logger.error("At least one of 'userEmail', 'userPhone', or 'ehrmsId' must be provided.")
            return jsonify({'error': "At least one of 'userEmail', 'userPhone', or 'ehrmsId' must be provided."}), 400

        # New date filter and orgId parameter
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        # Validate date range if provided
        if start_date and end_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
                start_date = datetime.combine(start_date.date(), time.min)  # 00:00:00
                end_date = datetime.combine(end_date.date(), time.max)      # 23:59:59.999999
            except ValueError:
                return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400

        required_columns = data.get('required_columns', [])
        
        # Get additional filters
        additional_filters = data.get('additionalFilter', {})
        
        logger.info(f"Generating v2 user report for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}")
        logger.info(f"Additional filters: {additional_filters}")
        
        try:
            csv_data = ReportServiceV2.generate_user_report(
                email=user_email, 
                phone=user_phone, 
                ehrms_id=ehrms_id, 
                start_date=start_date, 
                end_date=end_date, 
                orgId=orgId, 
                required_columns=required_columns,
                additional_filters=additional_filters
            )

            if not csv_data:
                logger.warning(f"No data found for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}")
                return jsonify({'error': 'No data found for the given user details and filters.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"V2 Report generated successfully for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id} in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="user-report-v2.csv"'
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

@report_controller_v2.route('/report/v2/org/user/<orgId>', methods=['POST'])
def get_org_user_report(orgId):
    """V2 endpoint for organization user report with advanced filtering"""
    start_timer = time_module.time()
    try:
        logger.info(f"Received request to generate v2 organization user report for orgId={orgId}")
        
        # Validate request
        validation_result = _validate_request_common(orgId)
        if validation_result:
            return jsonify(validation_result[0]), validation_result[1]
        
        # Parse and validate input parameters
        try:
            data = request.get_json()
            if not data:
                logger.error("Request body is missing")
                return jsonify({'error': 'Request body is missing'}), 400
        except Exception as e:
            logger.error(f"Request body is missing: {str(e)}")
            return jsonify({'error': 'Request body is missing'}), 400

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

        # Parameters from request body
        is_full_report_required = data.get('isFullReportRequired', False)
        required_columns = data.get('required_columns', [])
        
        # Get additional filters
        additional_filters = data.get('additionalFilter', {})
        
        logger.info(f"Generating v2 organization user report for orgId={orgId}")
        logger.info(f"Additional filters: {additional_filters}")
        
        try:
            csv_data = ReportServiceV2.generate_org_user_report(
                mdo_id=orgId, 
                is_full_report_required=is_full_report_required, 
                required_columns=required_columns, 
                user_creation_start_date=user_creation_start_date, 
                user_creation_end_date=user_creation_end_date,
                additional_filters=additional_filters
            )

            if not csv_data:
                logger.warning(f"No data found for orgId={orgId}")
                return jsonify({'error': 'No data found for the given org details and filters.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for orgId: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"V2 Org User Report generated successfully in {time_taken} seconds for orgId={orgId}")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="user-org-report-v2.csv"'
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