import pytest
import json
from unittest.mock import patch, MagicMock, call
from flask import Flask
from datetime import datetime
from app.controllers.report_controller import report_controller


@pytest.fixture
def app():
    """Create a Flask test app with the report_controller blueprint registered."""
    app = Flask(__name__)
    app.register_blueprint(report_controller)
    app.config['TESTING'] = True
    return app


@pytest.fixture
def client(app):
    """Create a test client for the Flask app."""
    with app.test_client() as client:
        yield client


@patch("app.controllers.report_controller.ReportService")
@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "false")
def test_get_report_success(mock_report_service, client):
    """Test successful report generation."""
    # Setup
    mock_report_service.fetch_master_enrolments_data.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31',
            'isFullReportRequired': True,
            'required_columns': ['col1', 'col2']
        }
    )
    
    # Verify
    assert response.status_code == 200
    assert response.mimetype == 'text/csv'
    assert 'attachment; filename="report_org123.csv"' in response.headers['Content-Disposition']
    
    # Check that the service was called with correct parameters
    mock_report_service.fetch_master_enrolments_data.assert_called_once()
    args, kwargs = mock_report_service.fetch_master_enrolments_data.call_args
    assert args[2] == 'org123'  # org_id
    assert args[3] is True  # is_full_report_required
    assert kwargs.get('required_columns') == ['col1', 'col2']  # required_columns


@patch("app.controllers.report_controller.ReportService")
@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "true")
@patch("app.controllers.report_controller.AccessTokenValidator")
def test_get_report_with_auth_success(mock_validator, mock_report_service, client):
    """Test report generation with authentication."""
    # Setup
    mock_validator.verify_user_token_get_org.return_value = "org123"
    mock_report_service.fetch_master_enrolments_data.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        },
        headers={'x-authenticated-user-token': 'valid-token'}
    )
    
    # Verify
    assert response.status_code == 200
    mock_validator.verify_user_token_get_org.assert_called_once_with('valid-token', True)


@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "true")
def test_get_report_missing_auth_token(client):
    """Test report generation with missing auth token."""
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 401
    data = response.get_json()
    assert 'Authentication token is required' in data['error']


@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "true")
@patch("app.controllers.report_controller.AccessTokenValidator")
def test_get_report_invalid_auth_token(mock_validator, client):
    """Test report generation with invalid auth token."""
    # Setup
    mock_validator.verify_user_token_get_org.return_value = ""
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        },
        headers={'x-authenticated-user-token': 'invalid-token'}
    )
    
    # Verify
    assert response.status_code == 401
    data = response.get_json()
    assert 'Invalid or expired authentication token' in data['error']


@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "true")
@patch("app.controllers.report_controller.AccessTokenValidator")
def test_get_report_unauthorized_org(mock_validator, client):
    """Test report generation with unauthorized organization."""
    # Setup
    mock_validator.verify_user_token_get_org.return_value = "org456"
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        },
        headers={'x-authenticated-user-token': 'valid-token'}
    )
    
    # Verify
    assert response.status_code == 403
    data = response.get_json()
    assert 'Access denied for the specified organization ID' in data['error']


def test_get_report_missing_dates(client):
    """Test report generation with missing date parameters."""
    # Execute
    response = client.post('/report/org/enrolment/org123', json={})
    
    # Verify
    assert response.status_code == 400
    data = response.get_json()
    assert 'Invalid input' in data['error']


def test_get_report_invalid_date_format(client):
    """Test report generation with invalid date format."""
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '01-01-2023',  # Wrong format
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 400
    data = response.get_json()
    assert 'Invalid date format' in data['error']


def test_get_report_date_range_too_long(client):
    """Test report generation with date range exceeding 1 year."""
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2022-01-01',
            'end_date': '2023-02-01'  # More than 1 year
        }
    )
    
    # Verify
    assert response.status_code == 400
    data = response.get_json()
    assert 'Date range cannot exceed 1 year' in data['error']


@patch("app.controllers.report_controller.ReportService")
@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "false")
def test_get_report_no_data(mock_report_service, client):
    """Test report generation with no data found."""
    # Setup
    mock_report_service.fetch_master_enrolments_data.return_value = None
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 404
    data = response.get_json()
    assert 'No data found' in data['error']


@patch("app.controllers.report_controller.ReportService")
@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "false")
def test_get_report_service_error(mock_report_service, client):
    """Test report generation with service error."""
    # Setup
    mock_report_service.fetch_master_enrolments_data.side_effect = Exception("Service error")
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 500
    data = response.get_json()
    assert 'Failed to generate the report' in data['error']


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_success(mock_report_service, client):
    """Test successful user report generation."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userEmail': 'test@example.com',
            'start_date': '2023-01-01',
            'end_date': '2023-01-31',
            'required_columns': ['col1', 'col2']
        }
    )
    
    # Verify
    assert response.status_code == 200
    assert response.mimetype == 'text/csv'
    assert 'attachment; filename="user-report.csv"' in response.headers['Content-Disposition']
    
    # Check that the service was called with correct parameters
    mock_report_service.fetch_user_cumulative_report.assert_called_once()
    args, kwargs = mock_report_service.fetch_user_cumulative_report.call_args
    assert args[0] == 'test@example.com'  # email
    assert args[5] == 'org123'  # orgId


def test_get_user_report_missing_user_identifiers(client):
    """Test user report generation with missing user identifiers."""
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 400
    data = response.get_json()
    assert 'At least one of' in data['error']


def test_get_user_report_missing_body(client):
    """Test user report generation with missing request body."""
    # Execute
    response = client.post('/report/user/sync/org123', json=None)
    
    # Verify
    assert response.status_code == 500


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_invalid_date_format(mock_report_service, client):
    """Test user report generation with invalid date format."""
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userEmail': 'test@example.com',
            'start_date': '01-01-2023',  # Wrong format
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 400
    data = response.get_json()
    assert 'Invalid date format' in data['error']


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_no_data(mock_report_service, client):
    """Test user report generation with no data found."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.return_value = None
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userEmail': 'test@example.com'
        }
    )
    
    # Verify
    assert response.status_code == 404
    data = response.get_json()
    assert 'No data found' in data['error']


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_with_phone(mock_report_service, client):
    """Test user report generation with phone number."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userPhone': '1234567890',
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 200
    mock_report_service.fetch_user_cumulative_report.assert_called_once()
    args, kwargs = mock_report_service.fetch_user_cumulative_report.call_args
    assert args[1] == '1234567890'  # phone


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_with_ehrms_id(mock_report_service, client):
    """Test user report generation with EHRMS ID."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'ehrmsId': 'EMP123',
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 200
    mock_report_service.fetch_user_cumulative_report.assert_called_once()
    args, kwargs = mock_report_service.fetch_user_cumulative_report.call_args
    assert args[2] == 'EMP123'  # ehrmsId


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_with_whitespace(mock_report_service, client):
    """Test user report generation with whitespace in identifiers."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userEmail': ' test@example.com ',
            'userPhone': ' 1234567890 ',
            'ehrmsId': ' EMP123 '
        }
    )
    
    # Verify
    assert response.status_code == 200
    mock_report_service.fetch_user_cumulative_report.assert_called_once()
    args, kwargs = mock_report_service.fetch_user_cumulative_report.call_args
    assert args[0] == 'test@example.com'  # email (trimmed)
    assert args[1] == '1234567890'  # phone (trimmed)
    assert args[2] == 'EMP123'  # ehrmsId (trimmed)


@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_service_error(mock_report_service, client):
    """Test user report generation with service error."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.side_effect = Exception("Service error")
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userEmail': 'test@example.com'
        }
    )
    
    # Verify
    assert response.status_code == 500
    data = response.get_json()
    assert 'Failed to generate the report' in data['error']


@patch("app.controllers.report_controller.ReportService")
def test_get_org_user_report_success(mock_report_service, client):
    """Test successful organization user report generation."""
    # Setup
    mock_report_service.fetch_master_user_data.return_value = iter(["header\n", "data1\n", "data2\n"])
    
    # Execute
    response = client.post(
        '/report/org/user/org123',
        json={
            'user_creation_start_date': '2023-01-01',
            'user_creation_end_date': '2023-01-31',
            'isFullReportRequired': True,
            'required_columns': ['col1', 'col2']
        }
    )
    
    # Verify
    assert response.status_code == 200
    assert response.mimetype == 'text/csv'
    assert 'attachment; filename="user-report.csv"' in response.headers['Content-Disposition']
    
    # Check that the service was called with correct parameters
    mock_report_service.fetch_master_user_data.assert_called_once()
    args, kwargs = mock_report_service.fetch_master_user_data.call_args
    assert args[0] == 'org123'  # orgId
    assert args[1] is True  # is_full_report_required
    assert kwargs['required_columns'] == ['col1', 'col2']
    assert isinstance(kwargs['user_creation_start_date'], datetime)
    assert isinstance(kwargs['user_creation_end_date'], datetime)


@patch("app.controllers.report_controller.ReportService")
def test_get_org_user_report_no_data(mock_report_service, client):
    """Test organization user report generation with no data found."""
    # Setup
    mock_report_service.fetch_master_user_data.return_value = None
    
    # Execute
    response = client.post(
        '/report/org/user/org123',
        json={"dummy": "data"}  # Add some data to avoid missing body error
    )
    
    # Verify
    assert response.status_code == 404
    data = response.get_json()
    assert 'No data found' in data['error']


@patch("app.controllers.report_controller.ReportService")
def test_get_org_user_report_service_error(mock_report_service, client):
    """Test organization user report generation with service error."""
    # Setup
    mock_report_service.fetch_master_user_data.side_effect = Exception("Service error")
    
    # Execute
    response = client.post(
        '/report/org/user/org123',
        json={"dummy": "data"}  # Add some data to avoid missing body error
    )
    
    # Verify
    assert response.status_code == 500
    data = response.get_json()
    assert 'Failed to generate the report' in data['error']


def test_get_org_user_report_missing_body(client):
    """Test organization user report generation with missing request body."""
    # Execute
    response = client.post('/report/org/user/org123', json=None)
    
    # Verify
    assert response.status_code == 500


@patch("app.controllers.report_controller.ReportService")
def test_get_org_user_report_invalid_date_format(mock_report_service, client):
    """Test organization user report generation with invalid date format."""
    # Execute
    response = client.post(
        '/report/org/user/org123',
        json={
            'user_creation_start_date': '01-01-2023',  # Wrong format
            'user_creation_end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 400
    data = response.get_json()
    assert 'Invalid date format' in data['error']


@patch("app.controllers.report_controller.GcsToBigQuerySyncService")
def test_sync_gcs_to_bq_success(mock_sync_service, client):
    """Test successful GCS to BigQuery sync."""
    # Setup
    mock_instance = MagicMock()
    mock_sync_service.return_value = mock_instance
    
    # Execute
    response = client.get('/gcs-to-bq/sync')
    
    # Verify
    assert response.status_code == 200
    data = response.get_json()
    assert data == {"status": "success", "message": "All tables synced successfully"}
    mock_instance.sync_all_tables.assert_called_once()


@patch("app.controllers.report_controller.GcsToBigQuerySyncService")
def test_sync_gcs_to_bq_error(mock_sync_service, client):
    """Test GCS to BigQuery sync with error."""
    # Setup
    mock_instance = MagicMock()
    mock_sync_service.return_value = mock_instance
    mock_instance.sync_all_tables.side_effect = Exception("Sync error")
    
    # Execute
    response = client.get('/gcs-to-bq/sync')
    
    # Verify
    assert response.status_code == 500
    data = response.get_json()
    assert data == {"status": "error", "message": "Sync error"}
    mock_instance.sync_all_tables.assert_called_once()


@patch("app.controllers.report_controller.ctypes.CDLL")
@patch("app.controllers.report_controller.ReportService")
@patch("app.controllers.report_controller.IS_VALIDATION_ENABLED", "false")
def test_get_report_with_malloc_trim(mock_report_service, mock_cdll, client):
    """Test report generation with malloc_trim call."""
    # Setup
    mock_report_service.fetch_master_enrolments_data.return_value = iter(["header\n", "data1\n", "data2\n"])
    mock_libc = MagicMock()
    mock_cdll.return_value = mock_libc
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify
    assert response.status_code == 200
    mock_cdll.assert_called_with("libc.so.6")
    mock_libc.malloc_trim.assert_called_with(0)


@patch("app.controllers.report_controller.ctypes.CDLL")
@patch("app.controllers.report_controller.ReportService")
def test_get_user_report_with_malloc_trim(mock_report_service, mock_cdll, client):
    """Test user report generation with malloc_trim call."""
    # Setup
    mock_report_service.fetch_user_cumulative_report.return_value = iter(["header\n", "data1\n", "data2\n"])
    mock_libc = MagicMock()
    mock_cdll.return_value = mock_libc
    
    # Execute
    response = client.post(
        '/report/user/sync/org123',
        json={
            'userEmail': 'test@example.com'
        }
    )
    
    # Verify
    assert response.status_code == 200
    mock_cdll.assert_called_with("libc.so.6")
    mock_libc.malloc_trim.assert_called_with(0)


@patch("app.controllers.report_controller.ctypes.CDLL")
@patch("app.controllers.report_controller.ReportService")
def test_get_org_user_report_with_malloc_trim(mock_report_service, mock_cdll, client):
    """Test organization user report generation with malloc_trim call."""
    # Setup
    mock_report_service.fetch_master_user_data.return_value = iter(["header\n", "data1\n", "data2\n"])
    mock_libc = MagicMock()
    mock_cdll.return_value = mock_libc
    
    # Execute
    response = client.post(
        '/report/org/user/org123',
        json={"dummy": "data"}  # Add some data to avoid missing body error
    )
    
    # Verify
    assert response.status_code == 200
    mock_cdll.assert_called_with("libc.so.6")
    mock_libc.malloc_trim.assert_called_with(0)


@patch("app.controllers.report_controller.ctypes.CDLL")
@patch("app.controllers.report_controller.ReportService")
def test_malloc_trim_exception(mock_report_service, mock_cdll, client):
    """Test handling of malloc_trim exception."""
    # Setup
    mock_report_service.fetch_master_enrolments_data.return_value = iter(["header\n", "data1\n", "data2\n"])
    mock_cdll.side_effect = Exception("malloc_trim error")
    
    # Execute
    response = client.post(
        '/report/org/enrolment/org123',
        json={
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
    )
    
    # Verify - should still return 200 as the malloc_trim exception is caught
    assert response.status_code == 200