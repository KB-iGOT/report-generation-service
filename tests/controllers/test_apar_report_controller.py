import pytest
from flask import Flask
from datetime import datetime, timedelta
from app.controllers import apar_report_controller

def create_app():
    app = Flask(__name__)
    app.register_blueprint(apar_report_controller)
    return app

def test_successful_report_generation(monkeypatch):
    app = create_app()
    client = app.test_client()

    # Provide a small CSV generator
    def fake_fetch(start, end, filters, cols):
        yield "col1,col2\n"
        yield "a,b\n"

    # Patch service method used by controller
    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_fetch
    )

    # Ensure allowed filters contain 'f1'
    apar_report_controller.APAR_FILTER_KEY = 'f1,f2'

    payload = {
        "assigned_on_start_date": (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d'),
        "assigned_on_end_date": datetime.utcnow().strftime('%Y-%m-%d'),
        "filters": {"f1": "value"},
        "required_columns": []
    }

    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 200
    assert resp.headers.get('Content-Type', '').startswith('text/csv')
    assert b'col1,col2' in resp.get_data()

def test_missing_dates_returns_400():
    app = create_app()
    client = app.test_client()
    resp = client.post('/report/apar/assigned/courses', json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Missing' in data.get('details') or 'Missing' in data.get('error')

def test_invalid_date_format_returns_400():
    app = create_app()
    client = app.test_client()

    apar_report_controller.APAR_FILTER_KEY = 'f1'
    payload = {
        "assigned_on_start_date": "2021/01/01",
        "assigned_on_end_date": "2021-01-02",
        "filters": {"f1": "v"}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Invalid date format' in data.get('error')

def test_invalid_filter_key_returns_400():
    app = create_app()
    client = app.test_client()

    # Allowed keys do not include 'bad'
    apar_report_controller.APAR_FILTER_KEY = 'good1,good2'
    payload = {
        "assigned_on_start_date": "2022-01-01",
        "assigned_on_end_date": "2022-01-02",
        "filters": {"bad": "x"}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Invalid filter key' in data.get('details') or 'Invalid filter key' in data.get('error')

def test_empty_filter_values_return_400():
    app = create_app()
    client = app.test_client()

    apar_report_controller.APAR_FILTER_KEY = 'a,b'
    payload = {
        "assigned_on_start_date": "2022-01-01",
        "assigned_on_end_date": "2022-01-02",
        "filters": {"a": "", "b": None}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'At least one' in data.get('details') or 'At least one' in data.get('error')

def test_date_range_exceeds_one_year_returns_400():
    app = create_app()
    client = app.test_client()

    apar_report_controller.APAR_FILTER_KEY = 'f1'
    start = (datetime.utcnow() - timedelta(days=366)).strftime('%Y-%m-%d')
    end = datetime.utcnow().strftime('%Y-%m-%d')
    payload = {
        "assigned_on_start_date": start,
        "assigned_on_end_date": end,
        "filters": {"f1": "v"}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Date range cannot exceed' in data.get('details') or 'Date range cannot exceed' in data.get('error')

def test_service_returns_no_data_results_in_500(monkeypatch):
    app = create_app()
    client = app.test_client()

    # Service returns empty -> controller should raise FileNotFoundError path -> 500
    def fake_fetch_empty(start, end, filters, cols):
        return []

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_fetch_empty
    )

    apar_report_controller.APAR_FILTER_KEY = 'f1'
    payload = {
        "assigned_on_start_date": "2022-01-01",
        "assigned_on_end_date": "2022-01-02",
        "filters": {"f1": "v"}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 500
    data = resp.get_json()
    assert 'Report file could not be generated' in data.get('error')

def test_unexpected_service_exception_returns_500(monkeypatch):
    app = create_app()
    client = app.test_client()

    def fake_fetch_raise(start, end, filters, cols):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_fetch_raise
    )

    apar_report_controller.APAR_FILTER_KEY = 'f1'
    payload = {
        "assigned_on_start_date": "2022-01-01",
        "assigned_on_end_date": "2022-01-02",
        "filters": {"f1": "v"}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 500
    data = resp.get_json()
    assert 'An unexpected error occurred' in data.get('error')