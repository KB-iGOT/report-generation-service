import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime
from app.services.report_service import ReportService


@pytest.fixture
def mock_bigquery_service():
    """Create a mock BigQueryService."""
    with patch('app.services.report_service.BigQueryService') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance


def test_fetch_user_cumulative_report_success(mock_bigquery_service):
    """Test successful user cumulative report generation."""
    # Setup
    user_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'mdo_id': ['org1', 'org1']
    })
    
    enrollment_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'course_id': ['course1', 'course2'],
        'progress': [80, 90]
    })
    
    # Configure mock to return our test dataframes
    mock_bigquery_service.run_query.side_effect = [user_df, enrollment_df]
    
    # Execute
    with patch('app.services.report_service.pd.DataFrame.drop') as mock_drop:
        result = ReportService.fetch_user_cumulative_report(
            email='test@example.com',
            orgId='org1'
        )
        
        # Verify
        assert result is not None
        # Check first item in generator is the header
        header = next(result)
        assert 'user_id' in header
        assert 'course_id' in header
        assert 'progress' in header
        
        # Check that the queries were executed
        assert mock_bigquery_service.run_query.call_count == 2
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


def test_fetch_user_cumulative_report_with_filters(mock_bigquery_service):
    """Test user cumulative report with various filters."""
    # Setup
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'mdo_id': ['org1']
    })
    
    enrollment_df = pd.DataFrame({
        'user_id': ['user1'],
        'course_id': ['course1'],
        'progress': [80]
    })
    
    # Configure mock to return our test dataframes
    mock_bigquery_service.run_query.side_effect = [user_df, enrollment_df]
    
    # Execute with all filter types
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    
    with patch('app.services.report_service.pd.DataFrame.drop'):
        result = ReportService.fetch_user_cumulative_report(
            email='test@example.com',
            phone='1234567890',
            ehrms_id='EMP123',
            start_date=start_date,
            end_date=end_date,
            orgId='org1',
            required_columns=['user_id', 'course_id']
        )
        
        # Verify
        assert result is not None
        
        # Check that the queries were executed with correct filters
        assert mock_bigquery_service.run_query.call_count == 2
        
        # First query should include all user filters
        first_query = mock_bigquery_service.run_query.call_args_list[0][0][0]
        assert "email = 'test@example.com'" in first_query
        assert "phone_number = '1234567890'" in first_query
        assert "external_system_id = 'EMP123'" in first_query
        
        # Second query should include date range
        second_query = mock_bigquery_service.run_query.call_args_list[1][0][0]
        assert "user_id IN ('user1')" in second_query
        assert f"enrolled_on BETWEEN '{start_date}' AND '{end_date}'" in second_query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


def test_fetch_user_cumulative_report_no_users(mock_bigquery_service):
    """Test user cumulative report when no users are found."""
    # Setup - empty dataframe
    mock_bigquery_service.run_query.return_value = pd.DataFrame()
    
    # Execute
    result = ReportService.fetch_user_cumulative_report(email='test@example.com')
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()


def test_fetch_user_cumulative_report_no_enrollments(mock_bigquery_service):
    """Test user cumulative report when no enrollments are found."""
    # Setup
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'mdo_id': ['org1']
    })
    
    # Empty enrollment dataframe
    enrollment_df = pd.DataFrame()
    
    # Configure mock to return our test dataframes
    mock_bigquery_service.run_query.side_effect = [user_df, enrollment_df]
    
    # Execute
    result = ReportService.fetch_user_cumulative_report(email='test@example.com')
    
    # Verify
    assert result is None
    assert mock_bigquery_service.run_query.call_count == 2


def test_fetch_user_cumulative_report_exception(mock_bigquery_service):
    """Test user cumulative report with exception."""
    # Setup
    mock_bigquery_service.run_query.side_effect = Exception("Query error")
    
    # Execute
    result = ReportService.fetch_user_cumulative_report(email='test@example.com')
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()


def test_fetch_master_enrolments_data_success(mock_bigquery_service):
    """Test successful master enrollments data retrieval."""
    # Setup
    enrollment_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'course_id': ['course1', 'course2'],
        'progress': [80, 90],
        'mdo_id': ['org1', 'org1']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = enrollment_df
    
    # Execute
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    
    with patch('app.services.report_service.pd.DataFrame.drop'):
        result = ReportService.fetch_master_enrolments_data(
            start_date=start_date,
            end_date=end_date,
            mdo_id='org1',
            is_full_report_required=False,
            required_columns=['user_id', 'course_id', 'progress']
        )
        
        # Verify
        assert result is not None
        
        # Check first item in generator is the header
        header = next(result)
        assert 'user_id|course_id|progress' in header
        
        # Check that the query was executed with correct parameters
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert "mdo_id in ('org1')" in query
        assert f"enrolled_on BETWEEN '{start_date}' AND '{end_date}'" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_fetch_master_enrolments_data_full_report(mock_get_mdo_list, mock_bigquery_service):
    """Test master enrollments data with full report required."""
    # Setup
    mock_get_mdo_list.return_value = ['org2', 'org3']
    
    enrollment_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'course_id': ['course1', 'course2'],
        'mdo_id': ['org1', 'org2']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = enrollment_df
    
    # Execute
    with patch('app.services.report_service.pd.DataFrame.drop'):
        result = ReportService.fetch_master_enrolments_data(
            start_date=None,
            end_date=None,
            mdo_id='org1',
            is_full_report_required=True,
            required_columns=None
        )
        
        # Verify
        assert result is not None
        
        # Check that the query was executed with all org IDs
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert "mdo_id in ('org2', 'org3', 'org1')" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


def test_fetch_master_enrolments_data_no_data(mock_bigquery_service):
    """Test master enrollments data with no data found."""
    # Setup - empty dataframe
    mock_bigquery_service.run_query.return_value = pd.DataFrame()
    
    # Execute
    result = ReportService.fetch_master_enrolments_data(
        start_date=None,
        end_date=None,
        mdo_id='org1',
        is_full_report_required=False,
        required_columns=None
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()


def test_fetch_master_enrolments_data_exception(mock_bigquery_service):
    """Test master enrollments data with exception."""
    # Setup
    mock_bigquery_service.run_query.side_effect = Exception("Query error")
    
    # Execute
    result = ReportService.fetch_master_enrolments_data(
        start_date=None,
        end_date=None,
        mdo_id='org1',
        is_full_report_required=False,
        required_columns=None
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()


def test_fetch_master_user_data_success(mock_bigquery_service):
    """Test successful master user data retrieval."""
    # Setup
    user_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'email': ['user1@example.com', 'user2@example.com'],
        'phone_number': ['1234567890', '0987654321'],
        'mdo_id': ['org1', 'org1']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Execute
    with patch('app.services.report_service.pd.DataFrame.drop'), \
         patch('app.services.report_service.IS_MASKING_ENABLED', 'false'):
        result = ReportService.fetch_master_user_data(
            mdo_id='org1',
            is_full_report_required=False,
            required_columns=['user_id', 'email', 'phone_number']
        )
        
        # Verify
        assert result is not None
        
        # Check first item in generator is the header
        header = next(result)
        assert 'user_id|email|phone_number' in header
        
        # Check that the query was executed with correct parameters
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert "mdo_id in ('org1')" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


@patch('app.services.report_service.ReportService._get_mdo_id_org_list')
def test_fetch_master_user_data_full_report(mock_get_mdo_list, mock_bigquery_service):
    """Test master user data with full report required."""
    # Setup
    mock_get_mdo_list.return_value = ['org2', 'org3']
    
    user_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'email': ['user1@example.com', 'user2@example.com'],
        'mdo_id': ['org1', 'org2']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Execute
    with patch('app.services.report_service.pd.DataFrame.drop'), \
         patch('app.services.report_service.IS_MASKING_ENABLED', 'false'):
        result = ReportService.fetch_master_user_data(
            mdo_id='org1',
            is_full_report_required=True
        )
        
        # Verify
        assert result is not None
        
        # Check that the query was executed with all org IDs
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert "mdo_id in ('org2', 'org3', 'org1')" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


def test_fetch_master_user_data_with_date_range(mock_bigquery_service):
    """Test master user data with date range."""
    # Setup
    user_df = pd.DataFrame({
        'user_id': ['user1', 'user2'],
        'email': ['user1@example.com', 'user2@example.com'],
        'mdo_id': ['org1', 'org1']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Execute
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    
    with patch('app.services.report_service.pd.DataFrame.drop'), \
         patch('app.services.report_service.IS_MASKING_ENABLED', 'false'):
        result = ReportService.fetch_master_user_data(
            mdo_id='org1',
            is_full_report_required=False,
            user_creation_start_date=start_date,
            user_creation_end_date=end_date
        )
        
        # Verify
        assert result is not None
        
        # Check that the query was executed with date range
        mock_bigquery_service.run_query.assert_called_once()
        query = mock_bigquery_service.run_query.call_args[0][0]
        assert f"user_registration_date BETWEEN '{start_date}' AND '{end_date}'" in query
        
        # Clean up the generator to avoid ResourceWarning
        for _ in result:
            pass


@patch('app.services.report_service.IS_MASKING_ENABLED', 'true')
def test_fetch_master_user_data_with_masking(mock_bigquery_service):
    """Test master user data with masking enabled."""
    # Setup
    user_df = pd.DataFrame({
        'user_id': ['user1'],
        'email': ['user1@example.com'],
        'phone_number': ['1234567890'],
        'mdo_id': ['org1']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = user_df
    
    # Execute
    with patch('app.services.report_service.pd.DataFrame.drop'):
        result = ReportService.fetch_master_user_data(
            mdo_id='org1',
            is_full_report_required=False
        )
        
        # Verify
        assert result is not None
        
        # Skip header
        next(result)
        
        # Check that the data is masked
        data_row = next(result)
        assert '@*******.**' in data_row or 'user1@*******.**' in data_row  # Masked email domain
        assert '******7890' in data_row  # Masked phone number
        
        # Clean up the generator to avoid ResourceWarning
        try:
            for _ in result:
                pass
        except StopIteration:
            pass


def test_fetch_master_user_data_no_data(mock_bigquery_service):
    """Test master user data with no data found."""
    # Setup - empty dataframe
    mock_bigquery_service.run_query.return_value = pd.DataFrame()
    
    # Execute
    result = ReportService.fetch_master_user_data(
        mdo_id='org1',
        is_full_report_required=False
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()


def test_fetch_master_user_data_exception(mock_bigquery_service):
    """Test master user data with exception."""
    # Setup
    mock_bigquery_service.run_query.side_effect = Exception("Query error")
    
    # Execute
    result = ReportService.fetch_master_user_data(
        mdo_id='org1',
        is_full_report_required=False
    )
    
    # Verify
    assert result is None
    mock_bigquery_service.run_query.assert_called_once()


def test_get_mdo_id_org_list(mock_bigquery_service):
    """Test getting MDO ID organization list."""
    # Setup
    hierarchy_df = pd.DataFrame({
        'organisation_id': ['org2', 'org3', 'org4']
    })
    
    # Configure mock to return our test dataframe
    mock_bigquery_service.run_query.return_value = hierarchy_df
    
    # Execute
    result = ReportService._get_mdo_id_org_list(mock_bigquery_service, 'org1')
    
    # Verify
    assert result == ['org2', 'org3', 'org4']
    mock_bigquery_service.run_query.assert_called_once()
    
    # Check that the query includes the correct MDO ID
    query = mock_bigquery_service.run_query.call_args[0][0]
    assert "input_id = 'org1'" in query