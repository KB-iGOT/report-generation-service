import pytest
from flask import Flask
from datetime import datetime, timedelta
from flask_wtf.csrf import CSRFProtect

from app.controllers import apar_report_controller as controller


def create_app():
    app = Flask(__name__)
    app.config['TESTING'] = True
    # Disable CSRF for tests
    app.config['WTF_CSRF_ENABLED'] = False
    csrf = CSRFProtect()
    csrf.init_app(app)
    app.register_blueprint(controller.apar_report_controller)
    return app


def test_validate_request_data_missing_raises_keyerror():
    with pytest.raises(KeyError):
        controller.validate_request_data(None)


def test_validate_date_range_normalizes_and_validates():
    start = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d')
    end = datetime.utcnow().strftime('%Y-%m-%d')
    s_dt, e_dt = controller.validate_date_range(start, end)
    assert s_dt.time().hour == 0 and s_dt.time().minute == 0
    assert e_dt.time().hour == 23 and e_dt.time().minute == 59
    assert (e_dt - s_dt).days <= 365


def test_validate_date_range_bad_format_raises_valueerror():
    with pytest.raises(ValueError):
        controller.validate_date_range('2021/01/01', '2021-01-02')


def test_validate_date_range_exceeds_one_year_raises_valueerror():
    start = (datetime.utcnow() - timedelta(days=366)).strftime('%Y-%m-%d')
    end = datetime.utcnow().strftime('%Y-%m-%d')
    with pytest.raises(ValueError):
        controller.validate_date_range(start, end)


def test_validate_filters_invalid_key_raises():
    # Temporarily set allowed keys on controller module
    controller.APAR_FILTER_KEY = 'a,b'
    with pytest.raises(ValueError):
        controller.validate_filters({'bad': 'x'})


def test_validate_filters_empty_values_raises():
    controller.APAR_FILTER_KEY = 'a,b'
    with pytest.raises(ValueError):
        controller.validate_filters({'a': '', 'b': None})


def test_generate_report_returns_file_not_found_when_empty(monkeypatch):
    # Make the service return empty list
    def fake_empty(start, end, filters, cols):
        return []

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_empty,
    )

    with pytest.raises(FileNotFoundError):
        controller.generate_report(None, None, {}, [])


def test_generate_report_propagates_exceptions(monkeypatch):
    def fake_raise(start, end, filters, cols):
        raise RuntimeError('boom')

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_raise,
    )

    with pytest.raises(RuntimeError):
        controller.generate_report(None, None, {}, [])


def test_route_success_streams_csv(monkeypatch):
    app = create_app()
    client = app.test_client()

    def fake_fetch(start, end, filters, cols):
        # generator to mimic streaming CSV
        yield 'c1,c2\n'
        yield '1,2\n'

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_fetch,
    )

    controller.APAR_FILTER_KEY = 'f1'
    payload = {
        'assigned_on_start_date': (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'assigned_on_end_date': datetime.utcnow().strftime('%Y-%m-%d'),
        'filters': {'f1': 'v'},
        'required_columns': []
    }

    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 200
    assert resp.headers.get('Content-Type', '').startswith('text/csv')
    data = resp.get_data()
    assert b'c1,c2' in data


def test_route_missing_dates_returns_400():
    app = create_app()
    client = app.test_client()
    resp = client.post('/report/apar/assigned/courses', json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Missing' in data.get('details') or 'Missing' in data.get('error')


def test_route_invalid_date_format_returns_400():
    app = create_app()
    client = app.test_client()
    controller.APAR_FILTER_KEY = 'f1'
    payload = {
        'assigned_on_start_date': '2021/01/01',
        'assigned_on_end_date': '2021-01-02',
        'filters': {'f1': 'v'}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Invalid date format' in data.get('error')


def test_route_invalid_filter_key_returns_400():
    app = create_app()
    client = app.test_client()
    controller.APAR_FILTER_KEY = 'good1,good2'
    payload = {
        'assigned_on_start_date': '2022-01-01',
        'assigned_on_end_date': '2022-01-02',
        'filters': {'bad': 'x'}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Invalid filter key' in data.get('details') or 'Invalid filter key' in data.get('error')


def test_route_date_range_exceeds_one_year_returns_400():
    app = create_app()
    client = app.test_client()
    controller.APAR_FILTER_KEY = 'f1'
    start = (datetime.utcnow() - timedelta(days=366)).strftime('%Y-%m-%d')
    end = datetime.utcnow().strftime('%Y-%m-%d')
    payload = {
        'assigned_on_start_date': start,
        'assigned_on_end_date': end,
        'filters': {'f1': 'v'}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'Date range cannot exceed' in data.get('details') or 'Date range cannot exceed' in data.get('error')


def test_route_service_no_data_returns_500(monkeypatch):
    app = create_app()
    client = app.test_client()

    def fake_empty(start, end, filters, cols):
        return []

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_empty,
    )

    controller.APAR_FILTER_KEY = 'f1'
    payload = {
        'assigned_on_start_date': '2022-01-01',
        'assigned_on_end_date': '2022-01-02',
        'filters': {'f1': 'v'}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 500
    data = resp.get_json()
    assert 'Report file could not be generated' in data.get('error')


def test_route_unexpected_service_exception_returns_500(monkeypatch):
    app = create_app()
    client = app.test_client()

    def fake_raise(start, end, filters, cols):
        raise RuntimeError('boom')

    monkeypatch.setattr(
        'app.controllers.apar_report_controller.AparReportService.fetch_apar_assigned_courses_report',
        fake_raise,
    )

    controller.APAR_FILTER_KEY = 'f1'
    payload = {
        'assigned_on_start_date': '2022-01-01',
        'assigned_on_end_date': '2022-01-02',
        'filters': {'f1': 'v'}
    }
    resp = client.post('/report/apar/assigned/courses', json=payload)
    assert resp.status_code == 500
    data = resp.get_json()
    assert 'An unexpected error occurred' in data.get('error')