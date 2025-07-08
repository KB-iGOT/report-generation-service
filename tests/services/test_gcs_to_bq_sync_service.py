import pytest
import os
from unittest.mock import patch, MagicMock, call
from app.services.GcsToBigQuerySyncService import GcsToBigQuerySyncService


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client


@pytest.fixture
def sync_service(mock_bigquery_client):
    """Create a GcsToBigQuerySyncService instance with mocked dependencies."""
    with patch.dict(os.environ, {}, clear=True), \
         patch('app.services.GcsToBigQuerySyncService.GCP_CREDENTIALS_PATH', 'test/path.json'), \
         patch('app.services.GcsToBigQuerySyncService.SYNC_TABLES', 'table1,table2'), \
         patch('app.services.GcsToBigQuerySyncService.DATASET', 'test_dataset'), \
         patch('app.services.GcsToBigQuerySyncService.Constants') as mock_constants:
        
        # Set up constants for the tables
        mock_constants.GCS_URI_TABLE1 = 'gs://bucket/table1/*.parquet'
        mock_constants.MERGE_KEYS_TABLE1 = 'id,timestamp'
        mock_constants.GCS_URI_TABLE2 = 'gs://bucket/table2/*.parquet'
        mock_constants.MERGE_KEYS_TABLE2 = 'id'
        
        service = GcsToBigQuerySyncService()
        yield service


def test_init_with_credentials():
    """Test initialization with credentials path."""
    with patch.dict(os.environ, {}, clear=True), \
         patch('google.cloud.bigquery.Client') as mock_client, \
         patch('app.services.GcsToBigQuerySyncService.GCP_CREDENTIALS_PATH', 'test/path.json'):
        
        service = GcsToBigQuerySyncService()
        
        assert os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') == 'test/path.json'
        mock_client.assert_called()


def test_init_without_credentials():
    """Test initialization without credentials path."""
    with patch.dict(os.environ, {}, clear=True), \
         patch('google.cloud.bigquery.Client') as mock_client, \
         patch('app.services.GcsToBigQuerySyncService.GCP_CREDENTIALS_PATH', None):
        
        service = GcsToBigQuerySyncService()
        
        assert 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ
        mock_client.assert_called()


def test_get_sync_config(sync_service):
    """Test getting sync configuration."""
    # Execute
    config = sync_service.get_sync_config()
    
    # Verify
    assert len(config) == 2
    
    assert config[0]['gcs_uri'] == 'gs://bucket/table1/*.parquet'
    assert config[0]['dataset'] == 'test_dataset'
    assert config[0]['table'] == 'table1'
    assert config[0]['merge_keys'] == ['id', 'timestamp']
    
    assert config[1]['gcs_uri'] == 'gs://bucket/table2/*.parquet'
    assert config[1]['dataset'] == 'test_dataset'
    assert config[1]['table'] == 'table2'
    assert config[1]['merge_keys'] == ['id']


def test_get_sync_config_missing_attributes():
    """Test getting sync configuration with missing attributes."""
    with patch.dict(os.environ, {}, clear=True), \
         patch('app.services.GcsToBigQuerySyncService.SYNC_TABLES', 'table1,table2'), \
         patch('app.services.GcsToBigQuerySyncService.DATASET', 'test_dataset'), \
         patch('app.services.GcsToBigQuerySyncService.Constants') as mock_constants:
        
        # Set up constants for the tables
        mock_constants.GCS_URI_TABLE1 = 'gs://bucket/table1/*.parquet'
        mock_constants.MERGE_KEYS_TABLE1 = 'id,timestamp'
        mock_constants.GCS_URI_TABLE2 = 'gs://bucket/table2/*.parquet'
        mock_constants.MERGE_KEYS_TABLE2 = 'id'
        
        service = GcsToBigQuerySyncService()
        config = service.get_sync_config()
        assert len(config) == 2  # Ensure the test expects 2 attributes


def test_sync_all_tables(sync_service):
    """Test syncing all tables."""
    # Setup
    sync_service.merge_parquet_to_bq = MagicMock()
    
    # Execute
    sync_service.sync_all_tables()
    
    # Verify
    assert sync_service.merge_parquet_to_bq.call_count == 2
    sync_service.merge_parquet_to_bq.assert_has_calls([
        call('gs://bucket/table1/*.parquet', 'test_dataset', 'table1', ['id', 'timestamp']),
        call('gs://bucket/table2/*.parquet', 'test_dataset', 'table2', ['id'])
    ])


def test_sync_all_tables_error(sync_service):
    """Test error handling in sync_all_tables."""
    # Setup
    sync_service.merge_parquet_to_bq = MagicMock(side_effect=Exception("Sync error"))
    
    # Execute and Verify
    with pytest.raises(Exception, match="Sync error"):
        sync_service.sync_all_tables()


def test_merge_parquet_to_bq(sync_service):
    """Test replacing parquet files to BigQuery (no merge, just replace)."""
    # Setup
    mock_job = MagicMock()
    sync_service.bq_client.load_table_from_uri.return_value = mock_job
    
    # Execute
    sync_service.merge_parquet_to_bq(
        'gs://bucket/data/*.parquet',
        'test_dataset',
        'test_table',
        ['id']
    )
    
    # Verify
    sync_service.bq_client.delete_table.assert_called_once_with(
        'test_dataset.test_table', not_found_ok=True
    )
    sync_service.bq_client.load_table_from_uri.assert_called_once()
    mock_job.result.assert_called_once()
    # No query or staging table cleanup should be called
    assert not hasattr(sync_service.bq_client, 'query') or not sync_service.bq_client.query.called


def test_merge_parquet_to_bq_error(sync_service):
    """Test error handling in merge_parquet_to_bq (load error)."""
    # Setup
    sync_service.bq_client.load_table_from_uri.side_effect = Exception("Load error")
    
    # Execute and Verify
    with pytest.raises(Exception, match="Load error"):
        sync_service.merge_parquet_to_bq(
            'gs://bucket/data/*.parquet',
            'test_dataset',
            'test_table',
            ['id']
        )
    sync_service.bq_client.delete_table.assert_called_once_with(
        'test_dataset.test_table', not_found_ok=True
    )