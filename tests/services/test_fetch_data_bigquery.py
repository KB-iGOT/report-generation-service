import pytest
import os
import pandas as pd
from unittest.mock import MagicMock, patch, call
from app.services.fetch_data_bigQuery import BigQueryService


@patch("app.services.fetch_data_bigQuery.bigquery.Client")
def test_singleton_pattern(mock_client):
    """Test that BigQueryService is a singleton."""
    # Reset the singleton instance for testing
    BigQueryService._instance = None
    
    # Create first instance
    service1 = BigQueryService()
    # Create second instance
    service2 = BigQueryService()
    
    # Both instances should be the same object
    assert service1 is service2
    # Client should be initialized only once
    mock_client.assert_called_once()


@patch("app.services.fetch_data_bigQuery.bigquery.Client")
@patch("app.services.fetch_data_bigQuery.GCP_CREDENTIALS_PATH", "test/path.json")
@patch.dict(os.environ, {}, clear=True)
def test_initialize_client_with_credentials(mock_client):
    """Test client initialization with credentials path."""
    # Reset the singleton instance for testing
    BigQueryService._instance = None
    
    service = BigQueryService()
    
    # Check if environment variable was set
    assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == "test/path.json"
    mock_client.assert_called_once()


@patch("app.services.fetch_data_bigQuery.bigquery.Client")
@patch("app.services.fetch_data_bigQuery.GCP_CREDENTIALS_PATH", None)
def test_initialize_client_without_credentials(mock_client):
    """Test client initialization without credentials path."""
    # Reset the singleton instance for testing
    BigQueryService._instance = None
    
    service = BigQueryService()
    
    # Client should still be initialized
    mock_client.assert_called_once()


@patch("app.services.fetch_data_bigQuery.bigquery.Client")
def test_run_query(mock_client):
    """Test successful query execution."""
    # Setup mock
    mock_result = MagicMock()
    mock_dataframe = pd.DataFrame([{"col1": "value1"}])
    mock_result.to_dataframe.return_value = mock_dataframe
    mock_client.return_value.query.return_value.result.return_value = mock_result
    
    # Reset the singleton instance for testing
    BigQueryService._instance = None
    
    # Execute
    service = BigQueryService()
    result = service.run_query("SELECT * FROM test_table")
    
    # Verify
    assert result.equals(mock_dataframe)
    mock_client.return_value.query.assert_called_once_with("SELECT * FROM test_table")


@patch("app.services.fetch_data_bigQuery.bigquery.Client")
def test_run_query_with_timeout(mock_client):
    """Test query execution with timeout parameter."""
    # Setup mock
    mock_result = MagicMock()
    mock_dataframe = pd.DataFrame([{"col1": "value1"}])
    mock_result.to_dataframe.return_value = mock_dataframe
    mock_client.return_value.query.return_value.result.return_value = mock_result
    
    # Reset the singleton instance for testing
    BigQueryService._instance = None
    
    # Execute
    service = BigQueryService()
    result = service.run_query("SELECT * FROM test_table", timeout=30)
    
    # Verify
    assert result.equals(mock_dataframe)
    mock_client.return_value.query.assert_called_once_with("SELECT * FROM test_table")
    mock_client.return_value.query.return_value.result.assert_called_once_with(timeout=30)


@patch("app.services.fetch_data_bigQuery.bigquery.Client")
def test_run_query_error(mock_client):
    """Test error handling in run_query method."""
    # Setup mock to raise exception
    mock_client.return_value.query.side_effect = Exception("Query error")
    
    # Reset the singleton instance for testing
    BigQueryService._instance = None
    
    # Execute
    service = BigQueryService()
    result = service.run_query("SELECT * FROM test_table")
    
    # Verify
    assert result is None
    mock_client.return_value.query.assert_called_once_with("SELECT * FROM test_table")


def test_test_connection_success():
    """Test successful connection test."""
    # Setup - create a mock instance directly
    service = MagicMock()
    service.run_query.return_value = pd.DataFrame([{"col1": 1}])
    
    # Call the method directly
    result = BigQueryService.test_connection(service)
    
    # Verify
    assert result is True
    service.run_query.assert_called_once_with("SELECT 1")


def test_test_connection_failure_none_result():
    """Test connection test with None result."""
    # Setup - create a mock instance directly
    service = MagicMock()
    service.run_query.return_value = None
    
    # Call the method directly
    result = BigQueryService.test_connection(service)
    
    # Verify
    assert result is False
    service.run_query.assert_called_once_with("SELECT 1")


def test_test_connection_exception():
    """Test connection test with exception."""
    # Setup - create a mock instance directly
    service = MagicMock()
    service.run_query.side_effect = Exception("Connection error")
    
    # Call the method directly
    result = BigQueryService.test_connection(service)
    
    # Verify
    assert result is False
    service.run_query.assert_called_once_with("SELECT 1")