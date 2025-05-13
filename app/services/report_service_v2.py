import logging
from app.services.fetch_data_bigQuery import BigQueryService
from constants import (MASTER_ENROLMENTS_TABLE, MASTER_USER_TABLE, 
                      IS_MASKING_ENABLED,
                      ENROLMENT_FILTER_CONFIG, USER_FILTER_CONFIG, USER_REPORT_FILTER_CONFIG)
import gc
import pandas as pd
from cachetools import TTLCache
from app.services.report_service import ReportService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class ReportServiceV2:
    logger = logging.getLogger(__name__)

    @staticmethod
    def _process_filters(filters, filter_config, where_clause_parts):
        """Process filters based on configuration"""
        for filter_name, filter_value in filters.items():
            if filter_name in filter_config:
                filter_config_item = filter_config[filter_name]
                
                # Skip already processed filters
                if filter_name == 'mdo_id_list':
                    continue
                
                # Process based on filter type
                if filter_config_item['type'] == 'string' and filter_value:
                    where_clause_parts.append(f"{filter_name} = '{filter_value}'")
                
                elif filter_config_item['type'] == 'list' and isinstance(filter_value, list) and filter_value:
                    values_str = ', '.join([f"'{val}'" for val in filter_value])
                    where_clause_parts.append(f"{filter_name} IN ({values_str})")
                
                elif filter_config_item['type'] == 'comparison' and filter_value:
                    # Parse comparison operator and value
                    filter_str = str(filter_value).strip()
                    for operator in filter_config_item['valid_operators']:
                        if filter_str.startswith(operator):
                            value = filter_str[len(operator):].strip()
                            try:
                                # Ensure value is numeric
                                float_value = float(value)
                                where_clause_parts.append(f"{filter_name} {operator} {float_value}")
                                break
                            except ValueError:
                                ReportServiceV2.logger.warning(f"Invalid numeric value for {filter_name}: {value}")
                
                elif filter_config_item['type'] == 'boolean' and filter_value is not None:
                    # Convert to boolean value
                    if filter_value in filter_config_item['values']:
                        bool_value = filter_config_item['values'][filter_value]
                        bool_str = "TRUE" if bool_value else "FALSE"
                        where_clause_parts.append(f"{filter_name} = {bool_str}")
        
        return where_clause_parts

    @staticmethod
    def generate_report(start_date, end_date, org_id, is_full_report_required, required_columns=None, additional_filters=None):
        """
        Enhanced version of fetch_master_enrolments_data with additional filtering capabilities.
        
        Args:
            start_date: Start date for enrollment filtering
            end_date: End date for enrollment filtering
            org_id: Organization ID
            is_full_report_required: Whether to include all sub-organizations
            required_columns: List of columns to include in the report
            additional_filters: Dictionary of additional filters to apply
            
        Returns:
            Generator yielding CSV data or None if no data found
        """
        try:
            bigquery_service = BigQueryService()
            additional_filters = additional_filters or {}

            # Build filters
            where_clause_parts = []
            
            # Add date filtering
            if start_date and end_date:
                where_clause_parts.append(f"enrolled_on BETWEEN '{start_date}' AND '{end_date}'")
            
            # Handle MDO ID filtering
            mdo_id_list = additional_filters.get('mdo_id_list', [])
            if mdo_id_list and isinstance(mdo_id_list, list) and len(mdo_id_list) > 0:
                # If specific MDO IDs are provided, use those
                ReportServiceV2.logger.info(f"Using provided MDO ID list: {mdo_id_list}")
                mdo_ids_to_use = [f"'{mid}'" for mid in mdo_id_list]
            else:
                # Otherwise use the standard logic based on is_full_report_required
                if is_full_report_required:
                    # Dynamically fetch orgs using hierarchy
                    mdo_id_org_list = ReportService._get_mdo_id_org_list(bigquery_service, org_id)
                    mdo_id_org_list.append(org_id)  # Add input mdo_id to the list
                    ReportServiceV2.logger.debug(f"Fetched {len(mdo_id_org_list)} MDO IDs (including input): {mdo_id_org_list}")
                else:
                    mdo_id_org_list = [org_id]
                    ReportServiceV2.logger.info(f"Full report not required. Using single mdo_id: {org_id}")
                
                mdo_ids_to_use = [f"'{mid}'" for mid in mdo_id_org_list]
            
            mdo_id_str = ', '.join(mdo_ids_to_use)
            where_clause_parts.append(f"mdo_id in ({mdo_id_str})")
            
            # Process additional filters
            where_clause_parts = ReportServiceV2._process_filters(additional_filters, ENROLMENT_FILTER_CONFIG, where_clause_parts)
            
            # Construct the WHERE clause
            where_clause = " AND ".join(where_clause_parts)
            
            query = f"""
                SELECT * 
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE {where_clause}
            """

            ReportServiceV2.logger.info(f"Executing enrolments query: {query}")
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                ReportServiceV2.logger.info("No data found for the given filters.")
                return None

            ReportServiceV2.logger.info(f"Fetched {len(result_df)} rows from master_enrolments_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportServiceV2.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
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
                    ReportServiceV2.logger.info("Cleaned up DataFrame after streaming.")

            ReportServiceV2.logger.info(f"CSV stream generated with {len(result_df)} rows.")
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportServiceV2.logger.error(f"Error fetching master enrolments data: {e}")
            return None

    @staticmethod
    def generate_user_report(email=None, phone=None, ehrms_id=None, start_date=None, end_date=None, orgId=None, required_columns=None, additional_filters=None):
        """
        Enhanced version of fetch_user_cumulative_report with additional filtering capabilities.
        
        Args:
            email: User email
            phone: User phone number
            ehrms_id: User EHRMS ID
            start_date: Start date for enrollment filtering
            end_date: End date for enrollment filtering
            orgId: Organization ID
            required_columns: List of columns to include in the report
            additional_filters: Dictionary of additional filters to apply
            
        Returns:
            Generator yielding CSV data or None if no data found
        """
        try:
            # Check if any user filter is provided
            if not any([email, phone, ehrms_id]):
                ReportServiceV2.logger.info("No user filters provided for fetching user data.")
                return None

            bigquery_service = BigQueryService()
            additional_filters = additional_filters or {}

            # Build filters for user details
            user_filters = []
            if email:
                user_filters.append(f"email = '{email}'")
            if phone:
                user_filters.append(f"phone_number = '{phone}'")
            if ehrms_id:
                user_filters.append(f"external_system_id = '{ehrms_id}'")

            if not user_filters:
                ReportServiceV2.logger.info("No valid filters provided for fetching user data.")
                return None

            # Construct the query for fetching user data
            user_filter_query = ' AND '.join(user_filters)
            user_query = f"""
                SELECT user_id, mdo_id
                FROM `{MASTER_USER_TABLE}`
                WHERE {user_filter_query}
            """

            ReportServiceV2.logger.info(f"Executing user query: {user_query}")
            user_df = bigquery_service.run_query(user_query)

            if user_df.empty:
                ReportServiceV2.logger.info("No users found matching the provided filters.")
                return None

            user_ids = user_df["user_id"].tolist()
            ReportServiceV2.logger.info(f"Fetched {len(user_ids)} users.")
            
            # Get the user's MDO ID
            user_mdo_id = user_df["mdo_id"].iloc[0]  # Get the first user's MDO ID
            
            # Check if organization ID is valid
            if orgId and orgId != user_mdo_id:
                mdo_id_org_list = ReportService._get_mdo_id_org_list(bigquery_service, orgId)
                mdo_id_org_list.append(orgId)  # Include the orgId itself
                
                if user_mdo_id not in mdo_id_org_list:
                    ReportServiceV2.logger.error(f"Invalid organization ID for user: {orgId}")
                    raise ValueError(f"Invalid organization ID for user: {orgId}")
            
            # Build filters for enrollment data
            # Fix the string formatting issue by breaking it down into simpler steps
            user_ids_quoted = [f"'{uid}'" for uid in user_ids]
            user_ids_str = ", ".join(user_ids_quoted)
            where_clause_parts = [f"user_id IN ({user_ids_str})"]
            
            # Add date filtering
            if start_date and end_date:
                where_clause_parts.append(f"enrolled_on BETWEEN '{start_date}' AND '{end_date}'")
            
            # Process additional filters
            where_clause_parts = ReportServiceV2._process_filters(additional_filters, USER_REPORT_FILTER_CONFIG, where_clause_parts)
            
            # Construct the WHERE clause
            where_clause = " AND ".join(where_clause_parts)
            
            enrollment_query = f"""
                SELECT *
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE {where_clause}
            """

            ReportServiceV2.logger.info(f"Executing enrollment query: {enrollment_query}")
            enrollment_df = bigquery_service.run_query(enrollment_query)

            if enrollment_df.empty:
                ReportServiceV2.logger.info("No enrollment data found for the given user and filters.")
                return None

            # Filter columns if specified
            if required_columns:
                existing_columns = [col for col in required_columns if col in enrollment_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportServiceV2.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
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
                    ReportServiceV2.logger.info("Cleaned up DataFrame after streaming.")

            ReportServiceV2.logger.info(f"CSV stream generated with {len(merged_df)} rows.")

            # Return CSV content without closing the stream
            return generate_csv_stream(merged_df, merged_df.columns.tolist())

        except MemoryError as me:
            ReportServiceV2.logger.error("MemoryError encountered. Consider processing data in smaller chunks.")
            raise
        except Exception as e:
            ReportServiceV2.logger.error(f"Error generating user report: {e}")
            raise

    @staticmethod
    def generate_org_user_report(mdo_id, is_full_report_required, required_columns=None, user_creation_start_date=None, user_creation_end_date=None, additional_filters=None):
        """
        Enhanced version of fetch_master_user_data with additional filtering capabilities.
        
        Args:
            mdo_id: Organization ID
            is_full_report_required: Whether to include all sub-organizations
            required_columns: List of columns to include in the report
            user_creation_start_date: Start date for user creation filtering
            user_creation_end_date: End date for user creation filtering
            additional_filters: Dictionary of additional filters to apply
            
        Returns:
            Generator yielding CSV data or None if no data found
        """
        try:
            bigquery_service = BigQueryService()
            additional_filters = additional_filters or {}
            
            # Build filters
            where_clause_parts = []
            
            # Add date filtering
            if user_creation_start_date and user_creation_end_date:
                where_clause_parts.append(f"user_registration_date BETWEEN '{user_creation_start_date}' AND '{user_creation_end_date}'")
            
            # Handle MDO ID filtering
            mdo_id_list = additional_filters.get('mdo_id_list', [])
            if mdo_id_list and isinstance(mdo_id_list, list) and len(mdo_id_list) > 0:
                # If specific MDO IDs are provided, use those
                ReportServiceV2.logger.info(f"Using provided MDO ID list: {mdo_id_list}")
                mdo_ids_to_use = [f"'{mid}'" for mid in mdo_id_list]
            else:
                # Otherwise use the standard logic based on is_full_report_required
                if is_full_report_required:
                    # Dynamically fetch orgs using hierarchy
                    mdo_id_org_list = ReportService._get_mdo_id_org_list(bigquery_service, mdo_id)
                    mdo_id_org_list.append(mdo_id)  # Add input mdo_id to the list
                    ReportServiceV2.logger.debug(f"Fetched {len(mdo_id_org_list)} MDO IDs (including input): {mdo_id_org_list}")
                else:
                    mdo_id_org_list = [mdo_id]
                    ReportServiceV2.logger.info(f"Full report not required. Using single mdo_id: {mdo_id}")
                
                mdo_ids_to_use = [f"'{mid}'" for mid in mdo_id_org_list]
            
            mdo_id_str = ', '.join(mdo_ids_to_use)
            where_clause_parts.append(f"mdo_id in ({mdo_id_str})")
            
            # Process additional filters
            where_clause_parts = ReportServiceV2._process_filters(additional_filters, USER_FILTER_CONFIG, where_clause_parts)
            
            # Construct the WHERE clause
            where_clause = " AND ".join(where_clause_parts)
            
            query = f"""
                SELECT * 
                FROM `{MASTER_USER_TABLE}`
                WHERE {where_clause}
            """

            ReportServiceV2.logger.info(f"Executing user query: {query}")
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                ReportServiceV2.logger.info("No data found for the given filters.")
                return None

            ReportServiceV2.logger.info(f"Fetched {len(result_df)} rows from master_user_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportServiceV2.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
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
                    ReportServiceV2.logger.info("Cleaned up DataFrame after streaming.")
            
            ReportServiceV2.logger.info(f"CSV stream generated with {len(result_df)} rows.")
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportServiceV2.logger.error(f"Error fetching master user data: {e}")
            return None