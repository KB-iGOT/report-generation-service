import os
import logging
import time
from google.cloud import bigquery
from constants import GCP_CREDENTIALS_PATH, SYNC_TABLES, DATASET
import constants as Constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GcsToBigQuerySyncService:
    def __init__(self):
        credentials_path = GCP_CREDENTIALS_PATH
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            logger.info(f"Using credentials from: {credentials_path}")
        else:
            logger.warning("No credentials path set in Config.")
        self.client = bigquery.Client()
        self.bq_client = bigquery.Client()

    def sync_all_tables(self):
        try:
            sync_config = self.get_sync_config()
            for table_config in sync_config:
                logger.info(f"Starting sync for {table_config['table']}")
                self.merge_parquet_to_bq(
                    table_config["gcs_uri"],
                    table_config["dataset"],
                    table_config["table"],
                    table_config["merge_keys"]
                )
        except Exception as e:
            logger.exception(f"Error during sync: {e}")
            raise

    def get_sync_config(self):
        sync_config = []
        sync_tables = SYNC_TABLES.split(',')
        dataset = DATASET

        for table in sync_tables:
            table_name = table.strip()
            gcs_uri_attr = f'GCS_URI_{table_name.upper()}'
            gcs_uri = getattr(Constants, gcs_uri_attr, None)
            merge_keys_attr = f'MERGE_KEYS_{table_name.upper()}'
            merge_keys_str = getattr(Constants, merge_keys_attr, None)

            if not all([gcs_uri, dataset, merge_keys_str]):
                logger.warning(f"Skipping table {table_name}: Missing GCS URI, dataset, or merge keys.")
                continue

            merge_keys = [key.strip() for key in merge_keys_str.split(',')]

            sync_config.append({
                "gcs_uri": gcs_uri,
                "dataset": dataset,
                "table": table_name,
                "merge_keys": merge_keys
            })

        return sync_config

    def merge_parquet_to_bq(self, gcs_uri, dataset, target_table, merge_keys):
        staging_table = f"{target_table}_staging"
        full_staging_table = f"{dataset}.{staging_table}"
        full_target_table = f"{dataset}.{target_table}"

        try:
            logger.info(f"Loading data from {gcs_uri} into {full_staging_table}")
            job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
            self.bq_client.load_table_from_uri(gcs_uri, full_staging_table, job_config=job_config).result()

            logger.info(f"Loaded staging table: {full_staging_table}")

            schema = self.bq_client.get_table(full_staging_table).schema
            column_names = [field.name for field in schema]

            merge_condition = ' AND '.join([f"T.`{key}` = S.`{key}`" for key in merge_keys])
            update_clause = ', '.join([f"T.`{col}` = S.`{col}`" for col in column_names])
            insert_clause = f"({', '.join([f'`{col}`' for col in column_names])}) VALUES ({', '.join([f'S.`{col}`' for col in column_names])})"

            merge_query = f"""
            MERGE `{full_target_table}` T
            USING `{full_staging_table}` S
            ON {merge_condition}
            WHEN MATCHED THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT {insert_clause}
            """

            logger.info(f"Running merge query for {target_table}")
            start_time = time.time()
            self.bq_client.query(merge_query).result()
            duration = time.time() - start_time
            logger.info(f"Merge completed for {target_table} in {duration:.2f} seconds")

        except Exception as e:
            logger.error(f"Error merging table {target_table}: {e}", exc_info=True)
            raise

        finally:
            self.cleanup_staging_table(full_staging_table)

    def cleanup_staging_table(self, staging_table):
        try:
            self.bq_client.delete_table(staging_table, not_found_ok=True)
            logger.info(f"Deleted staging table {staging_table}")
        except Exception as e:
            logger.warning(f"Failed to delete staging table {staging_table}: {e}")
