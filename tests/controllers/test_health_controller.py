import pytest
from unittest.mock import patch, MagicMock
from flask import Flask
from app.controllers.health_controller import health_controller


@pytest.fixture
def app():
    """Create a Flask test app with the health_controller blueprint registered."""
    app = Flask(__name__)
    app.register_blueprint(health_controller)
    app.config['TESTING'] = True
    return app


@pytest.fixture
def client(app):
    """Create a test client for the Flask app."""
    with app.test_client() as client:
        yield client


@patch("app.controllers.health_controller.BigQueryService")
def test_health_check_success(mock_bigquery_service, client):
    """Test successful health check endpoint."""
    # Setup
    mock_instance = MagicMock()
    mock_bigquery_service.return_value = mock_instance
    mock_instance.test_connection.return_value = True
    
    # Execute
    response = client.get('/health')
    
    # Verify
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "True"
    assert data["BigQueryDB"]["status"] == "Connected"
    mock_instance.test_connection.assert_called_once()


@patch("app.controllers.health_controller.BigQueryService")
def test_health_check_failure(mock_bigquery_service, client):
    """Test health check endpoint when BigQuery connection fails."""
    # Setup
    mock_instance = MagicMock()
    mock_bigquery_service.return_value = mock_instance
    mock_instance.test_connection.side_effect = Exception("Connection failed")
    
    # Execute
    response = client.get('/health')
    
    # Verify
    assert response.status_code == 500
    data = response.get_json()
    assert data["status"] == "False"
    assert data["BiqQueryDB"]["status"] == "down"
    mock_instance.test_connection.assert_called_once()


def test_liveness_check(client):
    """Test liveness check endpoint."""
    # Execute
    response = client.get('/liveness')
    
    # Verify
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "OK"