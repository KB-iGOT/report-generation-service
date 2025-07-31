from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.services.report_service import ReportService
from datetime import datetime, time
import logging
import gc
import ctypes
import time as time_module
from app.authentication.AccessTokenValidator import AccessTokenValidator
from constants import X_AUTHENTICATED_USER_TOKEN, IS_VALIDATION_ENABLED, X_ORG_ID
from app.services.GcsToBigQuerySyncService import GcsToBigQuerySyncService
import pandas as pd
import io
import uuid
import random
from datetime import timedelta

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
        logger.info(f"Received x_org_id={x_org_id}")
        if not x_org_id:
            logger.error("Missing 'x_org_id' in headers.")
            return jsonify({'error': 'Organization ID is required.'}), 400
        if not ReportService.isValidOrg(x_org_id, org_id):
            logger.error(f"Invalid organization ID: {org_id}")
            return jsonify({'error': f'Not authorized to view the report for : {org_id}'}), 401
        if IS_VALIDATION_ENABLED.lower() == 'true':
            # Extract and validate user token
            user_token = request.headers.get(X_AUTHENTICATED_USER_TOKEN)
            if not user_token:
                logger.error("Missing 'x-authenticated-user-token' in headers.")
                return jsonify({'error': 'Authentication token is required.'}), 401
            
            user_org_id = AccessTokenValidator.verify_user_token_get_org(user_token, True)
            if not user_org_id:
                logger.error("Invalid or expired authentication token.")
                return jsonify({'error': 'Invalid or expired authentication token.'}), 401

            logger.info(f"Authenticated user with user_org_id={user_org_id}")
            if user_org_id != org_id:
                logger.error(f"User does not have access to organization ID {org_id}.")
                return jsonify({'error': f'Access denied for the specified organization ID {org_id}.'}), 403

        # Parse and validate date range
        data = request.get_json()
        if not data or 'start_date' not in data or 'end_date' not in data:
            raise KeyError("Missing 'start_date' or 'end_date' in request body.")

        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')

        start_date = datetime.combine(start_date.date(), time.min)  # 00:00:00
        end_date = datetime.combine(end_date.date(), time.max)      # 23:59:59.999999

        # New parameters from request body
        is_full_report_required = data.get('isFullReportRequired', False)
        required_columns = data.get('required_columns', [])
         
        logger.info(f"Generating report for org_id={org_id} from {start_date} to {end_date}")
         #Validate date range
        if (end_date - start_date).days > 365:
            logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        try:
            csv_data = _get_enrolments_csv(
                start_date, end_date, org_id, is_full_report_required,
                required_columns=required_columns
            )

            if not csv_data:
                logger.warning(f"No data found for org_id={org_id} within given date range.")
                return jsonify({'error': 'No data found for the given organization ID.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for org_id={org_id}: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully for org_id={org_id} in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="report_{org_id}.csv"'
            }
        )

        # Explicitly trigger garbage collection to free up memory
        del csv_data
        gc.collect()

        return response

    except KeyError as e:
        error_message = str(e)
        logger.error(f"Missing required fields in request: {error_message}")
        return jsonify({'error': 'Invalid input. Please provide start_date and end_date.', 'details': error_message}), 400

    except ValueError as e:
        error_message = str(e)
        logger.error(f"Invalid date format in request: {error_message}")
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.', 'details': error_message}), 400

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

@report_controller.route('/report/user/sync/<orgId>', methods=['POST'])
def get_user_report(orgId):
    try:
        start_timer = time_module.time()
        logger.info("Received request to generate user report")
        x_org_id = request.headers.get(X_ORG_ID)
        logger.info(f"Received x_org_id={x_org_id}")
        if not x_org_id:
            logger.error("Missing 'x_org_id' in headers.")
            return jsonify({'error': 'Organization ID is required.'}), 400
        if not ReportService.isValidOrg(x_org_id, orgId):
            logger.error(f"Invalid organization ID: {orgId}")
            return jsonify({'error': f'Not authorized to view the report for : {orgId}'}), 401
        # Parse and validate input parameters
        data = request.get_json()
        if not data:
            logger.error("Request body is missing")
            return jsonify({'error': 'Request body is missing.'}), 400

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

        logger.info(f"Generating user report for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}")
        
        try:
            csv_data = ReportService.fetch_user_cumulative_report(
                user_email, user_phone, ehrms_id, start_date, end_date, orgId,
                required_columns
            )

            if not csv_data:
                logger.warning(f"No data found for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}")
                return jsonify({'error': 'No data found for the given user details.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id}: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully for userEmail={user_email}, userPhone={user_phone}, ehrmsId={ehrms_id} in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="user-report.csv"'
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

@report_controller.route('/report/org/user/<orgId>', methods=['POST'])
def get_org_user_report(orgId):
    try:
        start_timer = time_module.time()
        logger.info("Received request to generate user report")
        x_org_id = request.headers.get(X_ORG_ID)
        logger.info(f"Received x_org_id={x_org_id}")
        if not x_org_id:
            logger.error("Missing 'x_org_id' in headers.")
            return jsonify({'error': 'Organization ID is required.'}), 400
        if not ReportService.isValidOrg(x_org_id, orgId):
            logger.error(f"Invalid organization ID: {orgId}")
            return jsonify({'error': f'Not authorized to view the report for : {orgId}'}), 401        
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


        logger.info(f"Generating user report for orgId={orgId}")
        
        try:
            csv_data = ReportService.fetch_master_user_data(
                orgId, is_full_report_required, required_columns=required_columns, user_creation_start_date=user_creation_start_date, user_creation_end_date=user_creation_end_date
            )

            if not csv_data:
                logger.warning(f"No data found for orgId={orgId}")
                return jsonify({'error': 'No data found for the given org details.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for orgId: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Org User Report generated successfully for  in {time_taken} seconds for orgId={orgId}")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="user-report.csv"'
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
        # Parse and validate date range
        data = request.get_json()
        if not data or 'start_date' not in data or 'end_date' not in data:
            raise KeyError("Missing 'start_date' or 'end_date' in request body.")

        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')

        start_date = datetime.combine(start_date.date(), time.min)  # 00:00:00
        end_date = datetime.combine(end_date.date(), time.max)      # 23:59:59.999999

        required_columns = data.get('required_columns', [])
         
        logger.info(f"Generating report from {start_date} to {end_date}")
         #Validate date range
        if (end_date - start_date).days > 365:
            logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        try:
            csv_data = _get_enrolments_csv(
                start_date, end_date,
                required_columns=required_columns
            )

            if not csv_data:
                logger.warning(f"No data found for the given date range: {start_date} to {end_date}")
                return jsonify({'error': 'No data found for the given organization ID.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for APAR report: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"APAR Report generated successfully in {time_taken} seconds")

        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="report.csv"'
            }
        )

        # Explicitly trigger garbage collection to free up memory
        del csv_data
        gc.collect()

        return response

    except KeyError as e:
        error_message = str(e)
        logger.error(f"Missing required fields in request: {error_message}")
        return jsonify({'error': 'Invalid input. Please provide start_date and end_date.', 'details': error_message}), 400

    except ValueError as e:
        error_message = str(e)
        logger.error(f"Invalid date format in request: {error_message}")
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.', 'details': error_message}), 400

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

def _generate_dummy_enrolments_data(
    start_date, end_date, required_columns
):
    """
    Generate dummy enrolment data as a pandas DataFrame, filter by date, and return as CSV stream.
    """
    # Dummy value lists
    mdo_names = ['Ministry of Railways', 'Ministry of Post', 'Ministry of Defence']
    content_types = ['Course', 'Program', 'Comprehensive Assessment']
    content_statuses = ['Completed', 'In Progress', 'Not Started']
    competency_types = ['Behavioural', 'Functional', 'Domain']
    assessment_statuses = ['Pass', 'Fail']
    real_names = [
        "Amit Sharma", "Priya Singh", "Rahul Verma", "Neha Gupta", "Vikram Patel",
        "Sunita Reddy", "Rohit Mehra", "Anjali Nair", "Deepak Joshi", "Meena Kumari"
    ]

    # Generate 35 dummy records
    records = []
    for _ in range(35):
        enrolled_on = start_date + timedelta(days=random.randint(0, max(1, (end_date - start_date).days)))
        last_accessed = enrolled_on + timedelta(days=random.randint(0, 10))
        first_completed = last_accessed + timedelta(days=random.randint(0, 10))
        content_progress = random.randint(0, 100)
        status = 'Completed' if content_progress == 100 else random.choice(content_statuses)
        certificate_id = str(uuid.uuid4()) if status == 'Completed' else None
        certificate_generated = str(uuid.uuid4()) if status == 'Completed' else None
        content_type = random.choice(content_types)
        records.append({
            "user_id": str(uuid.uuid4()),
            "mdo_id": random.randint(100077777, 999900000),
            "mdo_name": random.choice(mdo_names),
            "full_name": random.choice(real_names),
            "content_id": str(uuid.uuid4()),
            "content_name": random.choice([
                "Yoga Day Course", "AI Generated Course", "AI Assessment", "Digital Literacy Program",
                "Cyber Security Basics", "Leadership Essentials", "Project Management 101",
                "Health & Wellness", "Climate Change Awareness", "Data Analytics Bootcamp"
            ]),
            "content_type": content_type,
            "enrolled_on": enrolled_on.strftime("%Y-%m-%d"),
            "content_progress_percentage": content_progress,
            "certificate_generated": certificate_generated,
            "content_last_accessed_on": last_accessed.strftime("%Y-%m-%d"),
            "first_completed_on": first_completed.strftime("%Y-%m-%d") if status == 'Completed' else None,
            "certificate_id": certificate_id,
            "content_duration": random.randint(1, 20) * 10,
            "content_status": status,
            "phone": f"+91{random.randint(7000000000, 9999999999)}",
            "email": f"user{random.randint(1, 100)}@example.com",
            "external_system_id": str(uuid.uuid4()),
            "isApar": True,
            "cbp_plan_id": str(uuid.uuid4()),
            "CompetencyType": random.choice(competency_types),
            "Status_Comprehensive_Level_Assessment": random.choice(assessment_statuses),
            "from_Date": enrolled_on.strftime("%Y-%m-%d"),
            "to_date": first_completed.strftime("%Y-%m-%d") if status == 'Completed' else last_accessed.strftime("%Y-%m-%d"),
        })
    df = pd.DataFrame(records)

    # Mask email and phone columns
    def mask_row(row):
        row_dict = row.copy()
        # Mask email
        if 'email' in row_dict and row_dict['email']:
            parts = row_dict['email'].split('@')
            if len(parts) == 2:
                domain_parts = parts[1].split('.')
                masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
                row_dict['email'] = f"{parts[0]}@{masked_domain}"
            else:
                row_dict['email'] = parts[0]
        # Mask phone (column name is 'phone')
        if 'phone' in row_dict and row_dict['phone']:
            phone = str(row_dict['phone'])
            if len(phone) >= 4:
                row_dict['phone'] = '*' * (len(phone) - 4) + phone[-4:]
            else:
                row_dict['phone'] = '*' * len(phone)
        return row_dict

    df = df.apply(mask_row, axis=1)

    # Filter by enrolled_on date range
    df = df[
        (pd.to_datetime(df['enrolled_on']) >= start_date) &
        (pd.to_datetime(df['enrolled_on']) <= end_date)
    ]

    # Filter columns if required_columns is provided and not empty
    if required_columns:
        # Only keep columns that exist in DataFrame
        filtered_cols = [col for col in required_columns if col in df.columns]
        if filtered_cols:
            df = df[filtered_cols]

    # Return CSV as a stream with pipe delimiter
    csv_stream = io.StringIO()
    df.to_csv(csv_stream, index=False, sep='|')
    csv_stream.seek(0)
    return csv_stream

def _get_enrolments_csv(start_date, end_date, required_columns):
    """
    Private helper to fetch enrolments CSV data (dummy data version).
    """
    return _generate_dummy_enrolments_data(
        start_date, end_date, required_columns
    )

