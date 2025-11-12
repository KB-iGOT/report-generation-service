import logging
from app.services.fetch_data_bigQuery import BigQueryService
from app.services.redis_service import RedisService
from app.services.report_helper import (
    build_date_filter,
    generate_csv_stream,
    filter_required_columns
)
from constants import (MASTER_ENROLMENTS_TABLE, MASTER_USER_TABLE, 
                      IS_MASKING_ENABLED, MAX_ORG_CACHE_AGE,
                      ENROLMENT_FILTER_CONFIG, USER_FILTER_CONFIG, USER_REPORT_FILTER_CONFIG, AND)
import gc
import pandas as pd
from app.services.report_service import ReportService

# Configure logging for the service. This configuration is safe as it is scoped to this module.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class ReportServiceV2:
    logger = logging.getLogger(__name__)

    @staticmethod
    def _build_query(table_name: str, where_parts: list, is_select_all: bool = True) -> str:
        """Build SQL query from components"""
        cols = "*" if is_select_all else "user_id, mdo_id"
        where_clause = f" {AND} ".join(where_parts)
        return f"""
            SELECT {cols} 
            FROM `{table_name}`
            WHERE {where_clause}
        """

    @staticmethod
    def _execute_query(bigquery_service: BigQueryService, query: str, context: str = "") -> pd.DataFrame:
        """Execute query and handle logging"""
        ReportServiceV2.logger.info(f"Executing {context} query: {query}")
        result_df = bigquery_service.run_query(query)
        if not result_df.empty:
            ReportServiceV2.logger.info(f"Fetched {len(result_df)} rows{' ' + context if context else ''}")
        return result_df

    @staticmethod
    def _generate_csv_stream(df, cols):
        """Generate a CSV stream from a DataFrame with proper cleanup"""
        return generate_csv_stream(df, cols)

    @staticmethod
    def _handle_mdo_ids(bigquery_service, org_id, is_full_report_required, mdo_id_list=None):
        """Handle MDO ID filtering logic"""
        if mdo_id_list and isinstance(mdo_id_list, list) and mdo_id_list:
            ReportServiceV2.logger.info(f"Using provided MDO ID list: {mdo_id_list}")
            mdo_id_org_list = list(ReportService._get_mdo_id_org_list(bigquery_service, org_id))
            mdo_id_list = [mid for mid in mdo_id_list if mid in mdo_id_org_list]
            if org_id not in mdo_id_list:
                mdo_id_list.append(org_id)
            ReportServiceV2.logger.info(f"Filtered MDO ID list: {mdo_id_list}")
        else:
            if is_full_report_required:
                mdo_id_list = list(ReportService._get_mdo_id_org_list(bigquery_service, org_id))
                mdo_id_list.append(org_id)
                ReportServiceV2.logger.debug(f"Fetched {len(mdo_id_list)} MDO IDs (including input): {mdo_id_list}")
            else:
                mdo_id_list = [org_id]
                ReportServiceV2.logger.info(f"Full report not required. Using single mdo_id: {org_id}")

        return [f"'{mid}'" for mid in mdo_id_list]

    @staticmethod
    def _filter_required_columns(df, required_columns):
        """Filter DataFrame to include only required columns"""
        if required_columns:
            existing_columns = [col for col in required_columns if col in df.columns]
            missing_columns = list(set(required_columns) - set(existing_columns))
            if missing_columns:
                ReportServiceV2.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
            return df[existing_columns]
        return df

    @staticmethod
    def _process_string_filter(filter_name, filter_value, filter_config_item, escaped_filter_name):
        """Process string type filters"""
        if not filter_value:
            return None
        
        if 'values' in filter_config_item:
            if isinstance(filter_config_item['values'], dict):
                if filter_value in filter_config_item['values']:
                    mapped_value = filter_config_item['values'][filter_value]
                    return f"{escaped_filter_name} = {mapped_value}" if isinstance(mapped_value, int) else f"{escaped_filter_name} = '{mapped_value}'"
            else:
                return f"{escaped_filter_name} IN ({', '.join(map(str, filter_config_item['values']))})"
        return f"{escaped_filter_name} = '{filter_value}'"

    @staticmethod
    def _process_list_filter(filter_name, filter_value, escaped_filter_name):
        """Process list type filters"""
        if not isinstance(filter_value, list) or not filter_value:
            return None
        values_str = ', '.join([f"'{val}'" for val in filter_value])
        return f"{escaped_filter_name} IN ({values_str})"

    @staticmethod
    def _process_comparison_filter(filter_name, filter_value, filter_config_item, escaped_filter_name):
        """Process comparison type filters"""
        if not filter_value:
            return None
            
        for operator in filter_config_item.get('valid_operators', []):
            if filter_value.startswith(operator):
                value = filter_value[len(operator):].strip()
                try:
                    float_value = float(value)
                    return f"{escaped_filter_name} {operator} {float_value}"
                except ValueError:
                    ReportServiceV2.logger.warning(f"Invalid numeric value for {filter_name}: {value}")
                    
        valid_ops = filter_config_item.get('valid_operators', [])
        raise ValueError(f"Invalid operator for {filter_name}: '{filter_value}'. Allowed operators: {valid_ops}")

    @staticmethod
    def _process_boolean_filter(filter_name, filter_value, filter_config_item, escaped_filter_name):
        """Process boolean type filters"""
        if filter_value is None:
            return None
            
        if 'values' in filter_config_item and filter_value in filter_config_item['values']:
            bool_value = filter_config_item['values'][filter_value]
            bool_str = "TRUE" if bool_value else "FALSE"
            return f"{escaped_filter_name} = {bool_str}"
        return None

    @staticmethod
    def _process_filters(filters, filter_config, where_clause_parts):
        """Process filters based on configuration"""
        for filter_name, filter_value in filters.items():
            if filter_name not in filter_config or filter_name == 'mdo_id_list':
                continue

            filter_config_item = filter_config[filter_name]
            escaped_filter_name = f"`{filter_name}`"
            filter_type = filter_config_item['type']
            
            filter_processors = {
                'string': ReportServiceV2._process_string_filter,
                'list': ReportServiceV2._process_list_filter,
                'comparison': ReportServiceV2._process_comparison_filter,
                'boolean': ReportServiceV2._process_boolean_filter
            }
            
            if filter_type in filter_processors:
                result = filter_processors[filter_type](
                    filter_name, filter_value, filter_config_item, escaped_filter_name
                )
                if result:
                    where_clause_parts.append(result)

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
            where_clause_parts = []
            
            # Add date filtering
            if start_date and end_date:
                where_clause_parts.append(
                    build_date_filter(start_date, end_date, "enrolled_on")
                )
            
            # Handle MDO ID filtering
            mdo_ids = ReportServiceV2._handle_mdo_ids(
                bigquery_service, 
                org_id, 
                is_full_report_required, 
                (additional_filters or {}).get('mdo_id_list', [])
            )
            where_clause_parts.append(f"mdo_id IN ({', '.join(mdo_ids)})")
            
            # Process additional filters
            where_clause_parts = ReportServiceV2._process_filters(
                additional_filters or {},
                ENROLMENT_FILTER_CONFIG,
                where_clause_parts
            )
            
            # Build and execute query
            query = ReportServiceV2._build_query(MASTER_ENROLMENTS_TABLE, where_clause_parts)
            result_df = ReportServiceV2._execute_query(bigquery_service, query, "from master_enrolments_data")
            
            if result_df.empty:
                return None
            
            # Process results
            result_df = filter_required_columns(result_df, required_columns)
            return ReportServiceV2._generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportServiceV2.logger.error(f"Error fetching master enrolments data: {e}")
            raise

    @staticmethod
    def _build_user_filters(email=None, phone=None, ehrms_id=None):
        """Build user filters list"""
        filters = []
        if email:
            filters.append(f"email = '{email}'")
        if phone:
            filters.append(f"phone_number = '{phone}'")
        if ehrms_id:
            filters.append(f"external_system_id = '{ehrms_id}'")
        return filters

    @staticmethod
    def _validate_org_id(bigquery_service, user_mdo_id, org_id):
        """Validate organization ID against user's MDO ID"""
        if org_id and org_id != user_mdo_id:
            mdo_id_org_list = list(ReportService._get_mdo_id_org_list(bigquery_service, org_id))
            mdo_id_org_list.append(org_id)
            if user_mdo_id not in mdo_id_org_list:
                raise ValueError(f"Invalid organization ID for user: {org_id}")

    @staticmethod
    def generate_user_report(email=None, phone=None, ehrms_id=None, start_date=None, end_date=None, org_id=None, required_columns=None, additional_filters=None):
        """
        Enhanced version of fetch_user_cumulative_report with additional filtering capabilities.
        
        Args:
            email: User email
            phone: User phone number
            ehrms_id: User EHRMS ID
            start_date: Start date for enrollment filtering
            end_date: End date for enrollment filtering
            org_id: Organization ID
            required_columns: List of columns to include in the report
            additional_filters: Dictionary of additional filters to apply
            
        Returns:
            Generator yielding CSV data or None if no data found
        """
        try:
            # Build and validate user filters
            user_filters = ReportServiceV2._build_user_filters(email, phone, ehrms_id)
            if not user_filters:
                ReportServiceV2.logger.info("No valid user filters provided.")
                return None

            # Query user data
            bigquery_service = BigQueryService()
            user_query = ReportServiceV2._build_query(
                MASTER_USER_TABLE, 
                user_filters,
                is_select_all=False
            )
            user_df = ReportServiceV2._execute_query(bigquery_service, user_query, "user")

            if user_df.empty:
                ReportServiceV2.logger.info("No users found matching the provided filters.")
                return None

            # Validate organization ID
            user_mdo_id = user_df["mdo_id"].iloc[0]
            ReportServiceV2._validate_org_id(bigquery_service, user_mdo_id, org_id)

            # Build enrollment filters
            user_ids = user_df["user_id"].tolist()
            user_ids_quoted = [f"'{uid}'" for uid in user_ids]
            where_clause_parts = [f"user_id IN ({', '.join(user_ids_quoted)})"]
            
            if start_date and end_date:
                where_clause_parts.append(build_date_filter(start_date, end_date, "enrolled_on"))
            
            # Add additional filters
            where_clause_parts = ReportServiceV2._process_filters(
                additional_filters or {},
                USER_REPORT_FILTER_CONFIG,
                where_clause_parts
            )
            
            # Query enrollments
            enrollment_query = ReportServiceV2._build_query(
                MASTER_ENROLMENTS_TABLE,
                where_clause_parts
            )
            enrollment_df = ReportServiceV2._execute_query(
                bigquery_service,
                enrollment_query,
                "enrollment"
            )

            if enrollment_df.empty:
                ReportServiceV2.logger.info("No enrollment data found for the given user and filters.")
                return None

            # Process and return results
            result_df = filter_required_columns(enrollment_df, required_columns)
            return ReportServiceV2._generate_csv_stream(result_df, result_df.columns.tolist())

        except MemoryError:
            ReportServiceV2.logger.error("MemoryError encountered. Consider processing data in smaller chunks.")
            raise
        except Exception as e:
            ReportServiceV2.logger.error(f"Error generating user report: {e}")
            raise

    @staticmethod
    def _mask_sensitive_data(row_dict):
        """Mask sensitive data in user reports"""
        if IS_MASKING_ENABLED.lower() != 'true':
            return row_dict

        masked_dict = row_dict.copy()
        
        # Mask email
        if 'email' in masked_dict and masked_dict['email']:
            parts = masked_dict['email'].split('@')
            if len(parts) == 2:
                domain_parts = parts[1].split('.')
                masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
                masked_dict['email'] = f"{parts[0]}@{masked_domain}"
            else:
                masked_dict['email'] = parts[0]

        # Mask phone number
        if 'phone_number' in masked_dict and masked_dict['phone_number']:
            phone = str(masked_dict['phone_number'])
            if len(phone) >= 4:
                masked_dict['phone_number'] = '*' * (len(phone) - 4) + phone[-4:]
            else:
                masked_dict['phone_number'] = '*' * len(phone)

        return masked_dict

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
            where_clause_parts = []
            
            # Add date filtering
            if user_creation_start_date and user_creation_end_date:
                where_clause_parts.append(
                    build_date_filter(user_creation_start_date, user_creation_end_date, "user_registration_date")
                )
            
            # Handle MDO ID filtering
            mdo_ids = ReportServiceV2._handle_mdo_ids(
                bigquery_service,
                mdo_id,
                is_full_report_required,
                (additional_filters or {}).get('mdo_id_list', [])
            )
            where_clause_parts.append(f"mdo_id IN ({', '.join(mdo_ids)})")
            
            # Remove date filters from additional filters to avoid conflicts
            if user_creation_start_date and user_creation_end_date and additional_filters:
                additional_filters = {
                    key: value for key, value in additional_filters.items()
                    if not key.startswith("user_registration_date")
                }
            
            # Process additional filters
            where_clause_parts = ReportServiceV2._process_filters(
                additional_filters or {},
                USER_FILTER_CONFIG,
                where_clause_parts
            )
            
            # Build and execute query
            query = ReportServiceV2._build_query(MASTER_USER_TABLE, where_clause_parts)
            result_df = ReportServiceV2._execute_query(bigquery_service, query, "from master_user_data")
            
            if result_df.empty:
                return None
            
            # Process results with masking
            result_df = filter_required_columns(result_df, required_columns)
            
            def generate_masked_csv_stream(df, cols):
                try:
                    yield '|'.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        row_dict = dict(zip(cols, row))
                        masked_dict = ReportServiceV2._mask_sensitive_data(row_dict)
                        yield '|'.join([str(masked_dict.get(col, '')) for col in cols]) + '\n'
                finally:
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportServiceV2.logger.info("Cleaned up DataFrame after streaming.")
            
            return generate_masked_csv_stream(result_df, result_df.columns.tolist())
            
            ReportServiceV2.logger.info(f"CSV stream generated with {len(result_df)} rows.")
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportServiceV2.logger.error(f"Error fetching master user data: {e}")
            return None