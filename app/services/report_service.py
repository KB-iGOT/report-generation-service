import logging
from app.services.fetch_data_bigQuery import BigQueryService
from constants import MASTER_ENROLMENTS_TABLE, MASTER_USER_TABLE,MASTER_ORG_HIERARCHY_TABLE, IS_MASKING_ENABLED, MAX_ORG_CACHE_SIZE, MAX_ORG_CACHE_AGE
import gc
import pandas as pd
from cachetools import TTLCache

_mdo_org_cache = TTLCache(maxsize=int(MAX_ORG_CACHE_SIZE), ttl=int(MAX_ORG_CACHE_AGE))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class ReportService:
    logger = logging.getLogger(__name__)

    @staticmethod
    def fetch_user_cumulative_report(email=None, phone=None, ehrms_id=None, start_date=None, end_date=None, orgId=None, required_columns=None):
        try:
            # Check if any user filter is provided
            if not any([email, phone, ehrms_id]):
                ReportService.logger.info("No user filters provided for fetching user data.")
                return None

            bigquery_service = BigQueryService()

            # Build filters for user details
            user_filters = []
            if email:
                user_filters.append(f"email = '{email}'")
            if phone:
                user_filters.append(f"phone_number = '{phone}'")
            if ehrms_id:
                user_filters.append(f"external_system_id = '{ehrms_id}'")

            if not user_filters:
                ReportService.logger.info("No valid filters provided for fetching user data.")
                return None

            # Construct the query for fetching user data
            user_filter_query = ' AND '.join(user_filters)
            user_query = f"""
                SELECT user_id, mdo_id
                FROM `{MASTER_USER_TABLE}`
                WHERE {user_filter_query}
            """

            ReportService.logger.info(f"Executing user query: {user_query}")
            user_df = bigquery_service.run_query(user_query)

            if user_df.empty:
                ReportService.logger.info("No users found matching the provided filters.")
                return None

            user_ids = user_df["user_id"].tolist()
            ReportService.logger.info(f"Fetched {len(user_ids)} users.")
            
            # Get the user's MDO ID
            user_mdo_id = user_df["mdo_id"].iloc[0]  # Get the first user's MDO ID
            
            # Check if organization ID is valid
            if orgId and orgId != user_mdo_id:
                mdo_id_org_list = ReportService._get_mdo_id_org_list(bigquery_service, orgId)
                mdo_id_org_list.append(orgId)  # Include the orgId itself
                
                if user_mdo_id not in mdo_id_org_list:
                    ReportService.logger.error(f"Invalid organization ID for user: {orgId}")
                    raise ValueError(f"Invalid organization ID for user: {orgId}")
            
            # Construct the query for fetching enrollment data
            enrollment_query = f"""
                SELECT *
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE user_id IN ({', '.join([f"'{uid}'" for uid in user_ids])})
            """

            if start_date and end_date:
                enrollment_query += f" AND enrolled_on BETWEEN '{start_date}' AND '{end_date}'"

            ReportService.logger.info(f"Executing enrollment query: {enrollment_query}")
            enrollment_df = bigquery_service.run_query(enrollment_query)

            if enrollment_df.empty:
                ReportService.logger.info("No enrollment data found for the given user.")
                return None

            # Filter columns if specified
            if required_columns:
                existing_columns = [col for col in required_columns if col in enrollment_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                merged_df = enrollment_df[existing_columns]
            else:
                merged_df = enrollment_df

            def generate_csv_stream(df, cols):
                try:
                    yield '|'.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        yield '|'.join(map(str, row)) + '\n'
                finally:
                    # Safe cleanup after generator is fully consumed
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")

            ReportService.logger.info(f"CSV stream generated with {len(merged_df)} rows.")

            # Return CSV content without closing the stream
            return generate_csv_stream(merged_df, merged_df.columns.tolist())

        except MemoryError as me:
            ReportService.logger.error("MemoryError encountered. Consider processing data in smaller chunks.")
            raise
        except Exception as e:
            ReportService.logger.error(f"Error generating cumulative report: {e}")
            raise

    @staticmethod
    def fetch_master_enrolments_data(start_date, end_date, mdo_id, is_full_report_required, required_columns):
        try:
            bigquery_service = BigQueryService()

            # Add date filtering to the query if start_date and end_date are provided
            date_filter = ""
            if start_date and end_date:
                date_filter = f" AND enrolled_on BETWEEN '{start_date}' AND '{end_date}'"
            if is_full_report_required:
                # Dynamically fetch orgs using hierarchy
                mdo_id_org_list = ReportService._get_mdo_id_org_list(bigquery_service, mdo_id)
                mdo_id_org_list.append(mdo_id)  # Add input mdo_id to the list

                ReportService.logger.info(f"Fetched {len(mdo_id_org_list)} MDO IDs (including input): {mdo_id_org_list}")
            else:
                mdo_id_org_list = [mdo_id]
                ReportService.logger.info(f"Full report not required. Using single mdo_id: {mdo_id}")

            mdo_id_list = [f"'{mid}'" for mid in mdo_id_org_list]
            mdo_id_str = ', '.join(mdo_id_list)

            query = f"""
                SELECT * 
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE mdo_id in ({mdo_id_str}){date_filter}
            """

            ReportService.logger.info(f"Executing enrolments query: {query}")
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                ReportService.logger.info("No data found for the given mdo_id and date range.")
                return None

            ReportService.logger.info(f"Fetched {len(result_df)} rows from master_enrolments_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                result_df = result_df[existing_columns]

            # Generate CSV stream from the result DataFrame
            def generate_csv_stream(df, cols):
                try:
                    yield '|'.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        yield '|'.join(map(str, row)) + '\n'
                finally:
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")

            ReportService.logger.info(f"CSV stream generated with {len(result_df)} rows.")
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportService.logger.error(f"Error fetching master enrolments data: {e}")
            return None

    @staticmethod
    def fetch_master_user_data(mdo_id,  is_full_report_required, required_columns=None, user_creation_start_date=None, user_creation_end_date=None):
        try:
            bigquery_service = BigQueryService()
            # Add date filtering to the query if start_date and end_date are provided
            date_filter = ""
            if user_creation_start_date and user_creation_end_date:
                date_filter = f" AND user_registration_date BETWEEN '{user_creation_start_date}' AND '{user_creation_end_date}'"
            if is_full_report_required:
                mdo_id_org_list = ReportService._get_mdo_id_org_list(bigquery_service, mdo_id)
                mdo_id_org_list.append(mdo_id) 
            else: 
                mdo_id_org_list = [mdo_id]   
            
            mdo_id_list = [f"'{mid}'" for mid in mdo_id_org_list]  # Quote each ID
            mdo_id_str = ', '.join(mdo_id_list)  # Join them with commas
            query = f"""
                SELECT * 
                FROM `{MASTER_USER_TABLE}`
                WHERE mdo_id in ({mdo_id_str}){date_filter}
            """

            ReportService.logger.info(f"Executing query: {query}")

            # Update to use run_query instead of execute_query
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                ReportService.logger.info("No data found for user the given mdo_id and date range.")
                return None

            ReportService.logger.info(f"Fetched {len(result_df)} rows from master_enrolments_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                result_df = result_df[existing_columns]

            # Generate CSV stream from the result DataFrame
            def generate_csv_stream(df, cols):
                try:
                    yield '|'.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        row_dict = dict(zip(cols, row))
                        if IS_MASKING_ENABLED.lower() == 'true':
                        # Mask email
                            if 'email' in row_dict and row_dict['email']:
                                parts = row_dict['email'].split('@')
                                if len(parts) == 2:
                                    domain_parts = parts[1].split('.')
                                    masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
                                    row_dict['email'] = f"{parts[0]}@{masked_domain}"
                                else:
                                    row_dict['email'] = parts[0]

                            # Mask phone number: e.g., ******2245
                            if 'phone_number' in row_dict and row_dict['phone_number']:
                                phone = str(row_dict['phone_number'])
                                if len(phone) >= 4:
                                    row_dict['phone_number'] = '*' * (len(phone) - 4) + phone[-4:]
                                else:
                                    row_dict['phone_number'] = '*' * len(phone)

                        # Convert back to row and yield
                        yield '|'.join([str(row_dict.get(col, '')) for col in cols]) + '\n'
                finally:
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")
            ReportService.logger.info(f"CSV stream generated with {len(result_df)} rows.")

            # Return CSV content without closing the stream
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportService.logger.error(f"Error fetching master user data: {e}")
            return None


    @staticmethod
    def _get_mdo_id_org_list(bigquery_service: BigQueryService, mdo_id: str) -> list:
        if mdo_id in _mdo_org_cache:
            ReportService.logger.info(f"Cache hit for mdo_id: {mdo_id}")
            return _mdo_org_cache[mdo_id]
        ReportService.logger.info(f"Cache miss for mdo_id: {mdo_id}. Fetching from BigQuery.")
        org_hierarchy_query = f"""
            DECLARE input_id STRING;
            SET input_id = '{mdo_id}';

            WITH level_check AS (
                SELECT
                    CAST(input_id AS STRING) AS id,
                    MAX(CASE WHEN ministry_id = input_id THEN 1 ELSE 0 END) AS is_ministry,
                    MAX(CASE WHEN department_id = input_id THEN 1 ELSE 0 END) AS is_department
                FROM `{MASTER_ORG_HIERARCHY_TABLE}`
            ),
            orgs_from_department AS (
                SELECT CAST(mdo_id AS STRING) AS organisation_id
                FROM `{MASTER_ORG_HIERARCHY_TABLE}`
                WHERE department_id = input_id
            ),
            orgs_from_ministry AS (
                SELECT CAST(mdo_id AS STRING) AS organisation_id
                FROM `{MASTER_ORG_HIERARCHY_TABLE}`
                WHERE ministry_id = input_id
            )
            SELECT organisation_id
            FROM orgs_from_department
            WHERE EXISTS (
                SELECT 1 FROM level_check WHERE is_department = 1 AND is_ministry = 0
            )
            UNION ALL
            SELECT organisation_id
            FROM orgs_from_ministry
            WHERE EXISTS (
                SELECT 1 FROM level_check WHERE is_ministry = 1
            )
        """

        ReportService.logger.info(f"Executing org hierarchy query for mdo_id: {mdo_id}")
        hierarchy_df = bigquery_service.run_query(org_hierarchy_query)

        # Ensure all IDs are strings
        mdo_id_org_list = hierarchy_df["organisation_id"].tolist()

        _mdo_org_cache[mdo_id] = mdo_id_org_list
        return mdo_id_org_list

    @staticmethod
    def isValidOrg(x_org_id, request_org_id):
        try:
            # Check for None or empty request_org_id
            if not request_org_id:
                ReportService.logger.error("request_org_id is None or empty")
                return False
            ReportService.logger.debug(f"request_org_id={request_org_id}, type={type(request_org_id)}")
            ReportService.logger.debug(f"x_org_id={x_org_id}, type={type(x_org_id)}")
            # Ensure x_org_id is valid
            if not x_org_id:
                ReportService.logger.error("x_org_id is None or empty")
                return False

            bigquery_service = BigQueryService()

            # Fetch the organization list using _get_mdo_id_org_list
            org_list = ReportService._get_mdo_id_org_list(bigquery_service, x_org_id)
            org_list.append(x_org_id)  # Add input mdo_id to the list
            ReportService.logger.info(f"The OrgId list for {request_org_id}: {len(org_list)}")
            # Check if request_org_id is in the organization list
            is_valid = request_org_id in org_list
            ReportService.logger.info(f"Validation result for org_id {request_org_id}: {is_valid}")
            return is_valid

        except Exception as e:
            ReportService.logger.error(f"Error fetching mdo_list_data: {e}", exc_info=True)
            return False
