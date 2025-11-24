import json
from unittest import mock
from flask import Flask
import pytest
from flask_wtf.csrf import CSRFProtect

from app.controllers.ehrms_report_controller import ehrms_report_controller


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['TESTING'] = True
    # Disable CSRF for tests
    app.config['WTF_CSRF_ENABLED'] = False
    csrf = CSRFProtect()
    csrf.init_app(app)
    app.register_blueprint(ehrms_report_controller)
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def make_stream_generator(rows, cols):
    def gen():
        yield '|'.join(cols) + '\n'
        for r in rows:
            yield '|'.join(map(str, r)) + '\n'
    return gen()


@mock.patch('app.controllers.ehrms_report_controller.EhrmsReportService')
def test_get_user_enrolment_report_success(mock_report_service, client):
    # Prepare input
    payload = {
        "enrolment_start_date": "2024-01-01",
        "enrolment_end_date": "2024-01-10",
        "required_columns": ["user_id", "enrolled_on"]
    }

    # Mock service to return a simple CSV generator
    mock_gen = make_stream_generator([["u1", "2024-01-02"], ["u2", "2024-01-03"]], ["user_id", "enrolled_on"])
    mock_report_service.fetch_master_enrolments_data.return_value = mock_gen

    resp = client.post('/report/ehrms/org/enrolment', data=json.dumps(payload), content_type='application/json')

    assert resp.status_code == 200
    assert resp.mimetype == 'text/csv'
    body = resp.get_data(as_text=True)
    assert 'user_id|enrolled_on' in body
    assert 'u1|2024-01-02' in body


@mock.patch('app.controllers.ehrms_report_controller.EhrmsReportService')
def test_get_user_enrolment_report_missing_dates(mock_report_service, client):
    payload = { }
    resp = client.post('/report/ehrms/org/enrolment', data=json.dumps(payload), content_type='application/json')
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Invalid input' in data['error']


@mock.patch('app.controllers.ehrms_report_controller.EhrmsReportService')
def test_get_user_report_success(mock_report_service, client):
    payload = {
        "userEmail": "user@example.com",
        "required_columns": ["user_id", "enrolled_on"]
    }

    mock_gen = make_stream_generator([["u1", "2024-01-02"]], ["user_id", "enrolled_on"])
    mock_report_service.fetch_user_cumulative_report.return_value = mock_gen

    resp = client.post('/report/ehrms/user/sync', data=json.dumps(payload), content_type='application/json')

    assert resp.status_code == 200
    assert resp.mimetype == 'text/csv'
    assert 'user_id|enrolled_on' in resp.get_data(as_text=True)


@mock.patch('app.controllers.ehrms_report_controller.EhrmsReportService')
def test_get_user_report_missing_identifiers(mock_report_service, client):
    payload = {"required_columns": []}
    resp = client.post('/report/ehrms/user/sync', data=json.dumps(payload), content_type='application/json')
    assert resp.status_code == 400 or resp.status_code == 500


@mock.patch('app.controllers.ehrms_report_controller.EhrmsReportService')
def test_get_org_user_report_missing_body(mock_report_service, client):
    resp = client.post('/report/ehrms/org/user', data=json.dumps(None), content_type='application/json')
    assert resp.status_code == 400


@mock.patch('app.controllers.ehrms_report_controller.EhrmsReportService')
def test_get_org_user_report_success(mock_report_service, client):
    # Provide creation date range
    payload = {
        "user_creation_start_date": "2024-01-01",
        "user_creation_end_date": "2024-01-10",
        "required_columns": ["user_id", "email"]
    }
    mock_gen = make_stream_generator([["u1", "e1@example.com"]], ["user_id", "email"])
    mock_report_service.fetch_master_user_data.return_value = mock_gen

    resp = client.post('/report/ehrms/org/user', data=json.dumps(payload), content_type='application/json')
    assert resp.status_code == 200
    assert resp.mimetype == 'text/csv'
    assert 'user_id|email' in resp.get_data(as_text=True)
