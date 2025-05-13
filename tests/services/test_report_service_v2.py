import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime
from app.services.report_service_v2 import ReportServiceV2

@pytest.fixture
def mock_bigquery_service():
    """Create a mock BigQueryService."""
    with patch('app.services.report_service_v2.BigQueryService') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance

def test_process_filters():
    """Test the _process_filters method with different filter types."""
    # Setup
    filters = {
        'string_filter': 'string_value',
        'list_filter': ['value1', 'value2'],
        'comparison_filter': '> 50',
        'boolean_filter': 'Yes'
    }
    
    filter_config = {
        'string_filter': {'type': 'string'},
        'list_filter': {'type': 'list'},
        'comparison_filter': {'type': 'comparison', 'valid_operators': ['>', '<', '>=', '<=', '=']},
        'boolean_filter': {'type': 'boolean', 'values': {'Yes': True, 'No': False}}
    }
    
    where_clause_parts = []
    
    # Execute
    result = ReportServiceV2._process_filters(filters, filter_config, where_clause_parts)
    
    # Verify - check each item individually
    assert len(result) == 4
    assert any("string_filter = 'string_value'" in item for item in result)
    assert any("list_filter IN ('value1', 'value2')" in item for item in result)
    assert any("comparison_filter > 50" in item for item in result)
    assert any("boolean_filter = TRUE" in item for item in result)

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_report_with_additional_filters(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test report generation with additional filters."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    enrollment_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'course_id': ['course1', 'course2'],
        'content_progress_percentage': [80, 90],
        'mdo_id': ['org1', 'org1'],
        'content_id': ['content1', 'content2'],
        'certificate_generated': [True, False]
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = enrollment_df
    
    # Execute
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    
    additional_filters = {
        'content_id': 'content1',
        'content_progress_percentage': '> 50',
        'certificate_generated': 'Yes'
    }
    
    with patch('app.services.report_service_v2.pd.DataFrame.drop'):
        result = ReportServiceV2.generate_report(
            start_date=start_date,
            end_date=end_date,
            org_id='org1',
            is_full_report_required=False,
            required_columns=['user_id', 'course_id', 'content_progress_percentage'],
            additional_filters=additional_filters
        )
        
        # Verify
        assert result is not None
        
        # Check first item in generator is the header
        header = next(result)
        assert 'user_id|course_id|content_progress_percentage' in header
        
        # Check that the query was executed with correct parameters
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert "mdo_id in ('org1')" in query
        assert f"enrolled_on BETWEEN '{start_date}' AND '{end_date}'" in query
        assert "content_id = 'content1'" in query
        assert "content_progress_percentage > 50" in query
        assert "certificate_generated = TRUE" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_report_with_mdo_id_list(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test report generation with specific MDO ID list."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    enrollment_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'course_id': ['course1', 'course2'],
        'mdo_id': ['mdo1', 'mdo2']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = enrollment_df
    
    # Execute
    additional_filters = {
        'mdo_id_list': ['mdo1', 'mdo2']
    }
    
    with patch('app.services.report_service_v2.pd.DataFrame.drop'):
        result = ReportServiceV2.generate_report(
            start_date=None,
            end_date=None,
            org_id='org1',  # This should be ignored since mdo_id_list is provided
            is_full_report_required=True,  # This should be ignored since mdo_id_list is provided
            additional_filters=additional_filters
        )
        
        # Verify
        assert result is not None
        
        # Check that the query was executed with the specific MDO IDs
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert "mdo_id in ('mdo1', 'mdo2')" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_user_report_with_filters(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test user report generation with filters."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'mdo_id': ['org1']
    })
    
    enrollment_df = pd.DataFrame({
        'user_id': ['user1'],
        'content_id': ['content1'],
        'content_progress_percentage': [80],
        'certificate_generated': [True]
    })
    
    # Configure mock to return our test dataframes
    mock_bigquery_service.run_query.side_effect = [user_df, enrollment_df]
    
    # Execute
    additional_filters = {
        'content_id': 'content1',
        'content_progress_percentage': '>= 75',
        'certificate_generated': 'Yes'
    }
    
    result = ReportServiceV2.generate_user_report(
        email='test@example.com',
        phone=None,
        ehrms_id=None,
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 1, 31),
        orgId='org1',
        required_columns=['user_id', 'content_id', 'content_progress_percentage'],
        additional_filters=additional_filters
    )
    
    # Verify
    assert result is not None
    
    # Check first item in generator is the header
    header = next(result)
    assert 'user_id|content_id|content_progress_percentage' in header
    
    # Check that the queries were executed with correct parameters
    assert mock_bigquery_service.run_query.call_count == 2
    
    # Check the enrollment query
    enrollment_query = mock_bigquery_service.run_query.call_args_list[1][0][0]
    assert "user_id IN ('user1')" in enrollment_query
    assert "content_id = 'content1'" in enrollment_query
    assert "content_progress_percentage >= 75" in enrollment_query
    assert "certificate_generated = TRUE" in enrollment_query
    
    # Clean up the generator to avoid ResourceWarning
    for _ in result:
        pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_org_user_report_with_filters(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test organization user report generation with filters."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    user_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'email': ['user1@example.com', 'user2@example.com'],
        'role': ['admin', 'user'],
        'status': ['Active', 'Inactive'],
        'mdo_id': ['org1', 'org1']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Execute
    additional_filters = {
        'role': 'admin',
        'status': 'Active',
        'email_verified': 'Yes'
    }
    
    result = ReportServiceV2.generate_org_user_report(
        mdo_id='org1',
        is_full_report_required=False,
        required_columns=['user_id', 'email', 'role', 'status'],
        user_creation_start_date=datetime(2023, 1, 1),
        user_creation_end_date=datetime(2023, 1, 31),
        additional_filters=additional_filters
    )
    
    # Verify
    assert result is not None
    
    # Check first item in generator is the header
    header = next(result)
    assert 'user_id|email|role|status' in header
    
    # Check that the query was executed with correct parameters
    mock_bigquery_service.run_query.assert_called_once()
    query = mock_bigquery_service.run_query.call_args[0][0]
    assert "user_registration_date BETWEEN" in query
    assert "mdo_id in ('org1')" in query
    assert "role = 'admin'" in query
    assert "status = 'Active'" in query
    assert "email_verified = TRUE" in query
    
    # Clean up the generator to avoid ResourceWarning
    for _ in result:
        pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_report_no_data(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test report generation with no data found."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    # Setup - empty dataframe
    mock_bigquery_service.run_query.return_value = pd.DataFrame()
    
    # Execute
    result = ReportServiceV2.generate_report(
        start_date=None,
        end_date=None,
        org_id='org1',
        is_full_report_required=False,
        additional_filters={'content_id': 'content1'}
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_report_exception(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test report generation with exception."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    mock_bigquery_service.run_query.side_effect = Exception("Query error")
    
    # Execute
    result = ReportServiceV2.generate_report(
        start_date=None,
        end_date=None,
        org_id='org1',
        is_full_report_required=False,
        additional_filters={'certificate_generated': 'Yes'}
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_user_report_no_data(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test user report generation with no data found."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    # Return empty dataframe for user query
    mock_bigquery_service.run_query.return_value = pd.DataFrame()
    
    # Execute
    result = ReportServiceV2.generate_user_report(
        email='test@example.com',
        phone=None,
        ehrms_id=None,
        start_date=None,
        end_date=None,
        orgId='org1',
        additional_filters={}
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_org_user_report_no_data(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test organization user report generation with no data found."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    # Return empty dataframe
    mock_bigquery_service.run_query.return_value = pd.DataFrame()
    
    # Execute
    result = ReportServiceV2.generate_org_user_report(
        mdo_id='org1',
        is_full_report_required=False,
        additional_filters={}
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_org_user_report_with_masking(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test organization user report generation with masking enabled."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'email': ['user1@example.com'],
        'phone_number': ['1234567890'],
        'mdo_id': ['org1']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Enable masking
    with patch('app.services.report_service_v2.IS_MASKING_ENABLED', 'true'):
        result = ReportServiceV2.generate_org_user_report(
            mdo_id='org1',
            is_full_report_required=False,
            required_columns=['user_id', 'email', 'phone_number'],
            additional_filters={}
        )
        
        # Verify
        assert result is not None
        
        # Check first item in generator is the header
        header = next(result)
        assert 'user_id|email|phone_number' in header
        
        # Check that masking is applied
        row = next(result)
        assert '@' in row  # Email should be partially masked
        assert '******' in row  # Phone should be partially masked
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_report_with_full_report_required(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test report generation with is_full_report_required=True."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org2', 'org3']
    enrollment_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'mdo_id': ['org1', 'org2']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = enrollment_df
    
    # Execute
    result = ReportServiceV2.generate_report(
        start_date=None,
        end_date=None,
        org_id='org1',
        is_full_report_required=True,
        additional_filters={}
    )
    
    # Verify
    assert result is not None
    
    # Check that the query was executed with all MDO IDs
    mock_bigquery_service.run_query.assert_called_once()
    query = mock_bigquery_service.run_query.call_args[0][0]
    assert "mdo_id in ('org2', 'org3', 'org1')" in query
    
    # Clean up the generator to avoid ResourceWarning
    for _ in result:
        pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_user_report_with_invalid_org(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test user report generation with invalid organization ID."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org2', 'org3']
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'mdo_id': ['org4']  # Different from org1 and not in the org list
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Execute and expect ValueError
    with pytest.raises(ValueError, match="Invalid organization ID for user"):
        ReportServiceV2.generate_user_report(
            email='test@example.com',
            phone=None,
            ehrms_id=None,
            orgId='org1',
            additional_filters={}
        )

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_user_report_with_valid_org(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test user report generation with valid organization ID."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org2', 'org3']
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'mdo_id': ['org2']  # In the org list
    })
    
    enrollment_df = pd.DataFrame({
        'user_id': ['user1'],
        'content_id': ['content1']
    })
    
    # Configure mock to return our test dataframes
    mock_bigquery_service.run_query.side_effect = [user_df, enrollment_df]
    
    # Execute
    result = ReportServiceV2.generate_user_report(
        email='test@example.com',
        phone=None,
        ehrms_id=None,
        orgId='org1',
        additional_filters={}
    )
    
    # Verify
    assert result is not None
    
    # Clean up the generator to avoid ResourceWarning
    for _ in result:
        pass

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_user_report_with_memory_error(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test user report generation with memory error."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'mdo_id': ['org1']
    })
    
    # Configure mock to return our test dataframe for the first call
    mock_bigquery_service.run_query.side_effect = [user_df, MemoryError("Out of memory")]
    
    # Execute and expect MemoryError
    with pytest.raises(MemoryError):
        ReportServiceV2.generate_user_report(
            email='test@example.com',
            phone=None,
            ehrms_id=None,
            orgId='org1',
            additional_filters={}
        )

@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_generate_user_report_with_no_filters(mock_get_mdo_id_org_list, mock_bigquery_service):
    """Test user report generation with no user filters."""
    # Setup
    mock_get_mdo_id_org_list.return_value = ['org1']
    
    # Execute
    result = ReportServiceV2.generate_user_report(
        email=None,
        phone=None,
        ehrms_id=None,
        orgId='org1',
        additional_filters={}
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_not_called()