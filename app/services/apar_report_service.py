import logging
from app.services.fetch_data_bigQuery import BigQueryService
from constants import IS_MASKING_ENABLED, MAX_ORG_CACHE_AGE, MASTER_APAR_ASSIGNED_COURSES_TABLE, APAR_FILTER_KEY_MAP
import gc
import io
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class AparReportService:
    logger = logging.getLogger(__name__)

    @staticmethod
    def fetch_apar_assigned_courses_report(assigned_on_start_date, assigned_on_end_date, filters, required_columns):
        """
        Fetch data from BQ table master_enrolment_apar_dummy, apply filters, and return CSV stream.
        """
        try:
            client = BigQueryService()
            table = MASTER_APAR_ASSIGNED_COURSES_TABLE

            # Build query and parameters
            query, params = AparReportService._build_query_and_params(
                assigned_on_start_date, assigned_on_end_date, filters, table
            )

            # Execute query
            df = AparReportService._execute_query(client, query, params)

            # Filter columns if required
            if required_columns:
                df = AparReportService._filter_columns(df, required_columns)

            # Generate CSV stream
            return AparReportService._generate_csv_stream(df)
        except Exception as e:
            AparReportService.logger.error(f"Error fetching APAR enrolment report: {e}")
            return None

    @staticmethod
    def _build_query_and_params(start_date, end_date, filters, table):
        date_filter = ""
        
        filter_key_map = APAR_FILTER_KEY_MAP
        filter_clauses = []
        params = []

        for key, value in filters.items():
            if value and key in filter_key_map:
                bq_col = filter_key_map[key]
                filter_clauses.append(f"{bq_col} = @{bq_col}")
                params.append(bigquery.ScalarQueryParameter(bq_col, "STRING", value.strip()))

        if start_date and end_date:
            filter_clauses.insert(0, "assigned_on >= @start_date AND assigned_on <= @end_date")
            params.insert(0, bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date))
            params.insert(0, bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date))

        query = f"""
            SELECT *
            FROM `{table}`
            WHERE {" AND ".join(filter_clauses)}
        """
        return query, params

    @staticmethod
    def _execute_query(client, query, params):
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        AparReportService.logger.info(f"Executing query: {query} with params: {params}")
        return client.query(query, job_config=job_config).to_dataframe()

    @staticmethod
    def _filter_columns(df, required_columns):
        filtered_cols = [col for col in required_columns if col in df.columns]
        if filtered_cols:
            return df[filtered_cols]
        return df

    @staticmethod
    def _generate_csv_stream(df):
        try:
            # Work on a copy to avoid mutating the caller's DataFrame
            df = df.copy()
            cols = df.columns.tolist()

            # Ensure content_progress_percentage is set to 0 if present
            if 'content_progress_percentage' in cols:
                df['content_progress_percentage'] = 0
            yield '|'.join(cols) + '\n'
            for row in df.itertuples(index=False, name=None):
                row_dict = dict(zip(cols, row))
                AparReportService._apply_masking(row_dict)
                yield '|'.join([str(row_dict.get(col, '')) for col in cols]) + '\n'
        finally:
            df.drop(df.index, inplace=True)
            del df
            gc.collect()
            AparReportService.logger.info("Cleaned up DataFrame after streaming.")

    @staticmethod
    def _apply_masking(row_dict):
        if IS_MASKING_ENABLED.lower() == 'true':
            if 'email' in row_dict and row_dict['email']:
                parts = row_dict['email'].split('@')
                if len(parts) == 2:
                    domain_parts = parts[1].split('.')
                    masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
                    row_dict['email'] = f"{parts[0]}@{masked_domain}"
                else:
                    row_dict['email'] = parts[0]

            if 'phone' in row_dict and row_dict['phone']:
                phone = str(row_dict['phone'])
                if len(phone) >= 4:
                    row_dict['phone'] = '*' * (len(phone) - 4) + phone[-4:]
                else:
                    row_dict['phone'] = '*' * len(phone)
