import logging
from app.services.fetch_data_bigQuery import BigQueryService
from app.services.redis_service import RedisService
from constants import MASTER_ENROLMENTS_TABLE, MASTER_USER_TABLE, MASTER_ORG_HIERARCHY_TABLE, IS_MASKING_ENABLED, MAX_ORG_CACHE_AGE, MASTER_APAR_TABLE, APAR_FILTER_KEY_MAP, CLEANUP_MESSAGE, DOPT_EHRMS_EXTERNAL_SYSTEM_NAME
import gc
import io
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class EhrmsReportService:
    logger = logging.getLogger(__name__)

    @staticmethod
    def fetch_user_cumulative_report(email=None, phone=None, ehrms_id=None, start_date=None, end_date=None, required_columns=None):
        try:
            if not any([email, phone, ehrms_id]):
                EhrmsReportService.logger.info("No user filters provided for fetching user data.")
                return None

            bigquery_service = BigQueryService()
            user_ids = EhrmsReportService._get_user_ids(bigquery_service, email, phone, ehrms_id)
            if not user_ids:
                return None

            enrollment_df = EhrmsReportService._get_enrollment_data(bigquery_service, user_ids, start_date, end_date)
            if enrollment_df.empty:
                EhrmsReportService.logger.info("No enrollment data found for the given user.")
                return None

            merged_df = EhrmsReportService._filter_columns(enrollment_df, required_columns)
            return EhrmsReportService._generate_csv_stream(merged_df)

        except MemoryError:
            EhrmsReportService.logger.error("MemoryError encountered. Consider processing data in smaller chunks.")
            raise
        except Exception as e:
            EhrmsReportService.logger.error(f"Error generating cumulative report: {e}")
            raise

    @staticmethod
    def _get_user_ids(bigquery_service, email, phone, ehrms_id):
        user_filters = []
        if email:
            user_filters.append(f"email = '{email}'")
        if phone:
            user_filters.append(f"phone_number = '{phone}'")
        if ehrms_id:
            user_filters.append(f"external_system_id = '{ehrms_id}'")

        if not user_filters:
            EhrmsReportService.logger.info("No valid filters provided for fetching user data.")
            return None

        user_filter_query = ' AND '.join(user_filters)
        user_query = f"""
            SELECT user_id, mdo_id
            FROM `{MASTER_USER_TABLE}`
            WHERE {user_filter_query}
        """
        EhrmsReportService.logger.info(f"Executing user query: {user_query}")
        user_df = bigquery_service.run_query(user_query)

        if user_df.empty:
            EhrmsReportService.logger.info("No users found matching the provided filters.")
            return None

        user_ids = user_df["user_id"].tolist()
        EhrmsReportService.logger.info(f"Fetched {len(user_ids)} users.")
        return user_ids

    @staticmethod
    def _get_enrollment_data(bigquery_service, user_ids, start_date, end_date):
        enrollment_query = f"""
            SELECT *
            FROM `{MASTER_ENROLMENTS_TABLE}`
            WHERE user_id IN ({', '.join([f"'{uid}'" for uid in user_ids])})
        """
        if start_date and end_date:
            enrollment_query += f" AND enrolled_on BETWEEN '{start_date}' AND '{end_date}'"

        EhrmsReportService.logger.info(f"Executing enrollment query: {enrollment_query}")
        return bigquery_service.run_query(enrollment_query)

    @staticmethod
    def _filter_columns(df, required_columns):
        if required_columns:
            existing_columns = [col for col in required_columns if col in df.columns]
            missing_columns = list(set(required_columns) - set(existing_columns))
            if missing_columns:
                EhrmsReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
            return df[existing_columns]
        return df

    @staticmethod
    def _generate_csv_stream(df):
        def generate_csv(df, cols):
            try:
                yield '|'.join(cols) + '\n'
                for row in df.itertuples(index=False, name=None):
                    yield '|'.join(map(str, row)) + '\n'
            finally:
                df.drop(df.index, inplace=True)
                del df
                EhrmsReportService.logger.info(CLEANUP_MESSAGE)

        EhrmsReportService.logger.info(f"CSV stream generated with {len(df)} rows.")
        return generate_csv(df, df.columns.tolist())

    @staticmethod
    def fetch_master_enrolments_data(start_date, end_date, required_columns):
        try:
            bigquery_service = BigQueryService()
            external_system_name_filter = DOPT_EHRMS_EXTERNAL_SYSTEM_NAME
            # Add date filtering to the query if start_date and end_date are provided
            date_filter = ""
            if start_date and end_date:
                date_filter = f" AND enrolled_on BETWEEN '{start_date}' AND '{end_date}'"
            
            query = f"""
                SELECT * 
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE external_system = '{external_system_name_filter}' {date_filter}
            """

            EhrmsReportService.logger.info(f"Executing enrolments query: {query}")
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                EhrmsReportService.logger.info("No data found for the given mdo_id and date range.")
                return None

            EhrmsReportService.logger.info(f"Fetched {len(result_df)} rows from master_enrolments_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    EhrmsReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
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
                    EhrmsReportService.logger.info(CLEANUP_MESSAGE)

            EhrmsReportService.logger.info(f"CSV stream generated with {len(result_df)} rows.")
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            EhrmsReportService.logger.error(f"Error fetching master enrolments data: {e}")
            return None

    @staticmethod
    def fetch_master_user_data(user_creation_start_date=None, user_creation_end_date=None, user_updated_start_date=None, user_updated_end_date=None, required_columns=None):
        try:
            bigquery_service = BigQueryService()
            query = EhrmsReportService._build_user_data_query(
                user_creation_start_date, user_creation_end_date, user_updated_start_date, user_updated_end_date
            )
            result_df = EhrmsReportService._execute_query(bigquery_service, query)
            if result_df is None:
                return None
            result_df = EhrmsReportService._filter_dataframe_columns(result_df, required_columns)
            return EhrmsReportService._generate_masked_csv_stream(result_df)
        except Exception as e:
            EhrmsReportService.logger.error(f"Error fetching master user data: {e}")
            return None

    @staticmethod
    def _build_user_data_query(user_creation_start_date, user_creation_end_date, user_update_start_date, user_update_end_date):
        date_filter = ""
        external_system_name_filter = DOPT_EHRMS_EXTERNAL_SYSTEM_NAME
        if user_creation_start_date and user_creation_end_date:
            date_filter = f" AND user_registration_date BETWEEN '{user_creation_start_date}' AND '{user_creation_end_date}'"
        if user_update_start_date and user_update_end_date:
            date_filter += f" AND last_updated_on BETWEEN '{user_update_start_date}' AND '{user_update_end_date}'"
        query = f"""
            SELECT * 
            FROM `{MASTER_USER_TABLE}`
            WHERE external_system = '{external_system_name_filter}' {date_filter}
        """
        EhrmsReportService.logger.info(f"Executing query: {query}")
        return query

    @staticmethod
    def _execute_query(bigquery_service, query):
        result_df = bigquery_service.run_query(query)
        if result_df.empty:
            EhrmsReportService.logger.info("No data found for the given filters.")
            return None
        EhrmsReportService.logger.info(f"Fetched {len(result_df)} rows from master_user_data.")
        return result_df

    @staticmethod
    def _filter_dataframe_columns(df, required_columns):
        return EhrmsReportService._filter_columns(df, required_columns)

    @staticmethod
    def _generate_masked_csv_stream(df):
        def generate_csv_stream(df, cols):
            try:
                yield '|'.join(cols) + '\n'
                for row in df.itertuples(index=False, name=None):
                    row_dict = EhrmsReportService._mask_sensitive_data(dict(zip(cols, row)))
                    yield '|'.join([str(row_dict.get(col, '')) for col in cols]) + '\n'
            finally:
                df.drop(df.index, inplace=True)
                del df
                gc.collect()
                EhrmsReportService.logger.info("Cleaned up DataFrame after streaming.")
        EhrmsReportService.logger.info(f"CSV stream generated with {len(df)} rows.")
        return generate_csv_stream(df, df.columns.tolist())

    @staticmethod
    def _mask_sensitive_data(row_dict):
        if IS_MASKING_ENABLED.lower() == 'true':
            if 'email' in row_dict and row_dict['email']:
                parts = row_dict['email'].split('@')
                if len(parts) == 2:
                    domain_parts = parts[1].split('.')
                    masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
                    row_dict['email'] = f"{parts[0]}@{masked_domain}"
                else:
                    row_dict['email'] = parts[0]
            if 'phone_number' in row_dict and row_dict['phone_number']:
                phone = str(row_dict['phone_number'])
                if len(phone) >= 4:
                    row_dict['phone_number'] = '*' * (len(phone) - 4) + phone[-4:]
                else:
                    row_dict['phone_number'] = '*' * len(phone)
        return row_dict
