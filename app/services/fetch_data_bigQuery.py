import os
import logging
from threading import Lock
from google.cloud import bigquery
from constants import GCP_CREDENTIALS_PATH


logger = logging.getLogger(__name__)

class BigQueryService:
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(BigQueryService, cls).__new__(cls)
                    cls._instance._initialize_client()
        return cls._instance

    def _initialize_client(self):
        credentials_path = GCP_CREDENTIALS_PATH
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            logger.info(f"Using credentials from: {credentials_path}")
        else:
            logger.warning("No credentials path set in Config. Will use default ADC.")
        self.client = bigquery.Client()

    def run_query(self, query, timeout=None):
        try:
            import time
            start_time = time.time()
            query_job = self.client.query(query)
            data_frame = query_job.result(timeout=timeout).to_dataframe()
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"Query executed in {execution_time:.2f} seconds.")
            return data_frame
        except Exception as e:
            logger.error(f"Error running query: {e}")
            return None

    @staticmethod
    def test_connection(service=None):
        try:
            if service is None:
                service = BigQueryService()
            results = service.run_query("SELECT 1")
            if results is not None:
                logger.info("BigQuery is accessible.")
                return True
            else:
                logger.warning("Failed to access BigQuery.")
                return False
        except Exception as e:
            logger.error(f"Error testing BigQuery connection: {e}")
            return False
