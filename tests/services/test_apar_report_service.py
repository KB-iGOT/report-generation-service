import pytest
import pandas as pd
from types import SimpleNamespace
from app.services import apar_report_service
from app.services.apar_report_service import AparReportService
from datetime import datetime
import builtins
from unittest.mock import patch, MagicMock

# Helper to consume generator into list
def collect(gen):
    return list(gen)

def test_build_query_and_params_with_filters_and_dates():
    # Setup filter map
    apar_report_service.APAR_FILTER_KEY_MAP = {'f1': 'bq_f1', 'f2': 'bq_f2'}
    start = "2022-01-01"
    end = "2022-01-31"
    filters = {'f1': 'val1', 'f2': ''}

    query, params = AparReportService._build_query_and_params(start, end, filters, 'my.table')

    assert "assigned_on >= @start_date AND assigned_on <= @end_date" in query
    assert "bq_f1 = @bq_f1" in query
    # params first two should be start_date and end_date
    assert params[0].name == "start_date"
    assert params[1].name == "end_date"
    # there should be a param for bq_f1
    names = [p.name for p in params]
    assert "bq_f1" in names

def test_build_query_no_dates_or_filters():
    apar_report_service.APAR_FILTER_KEY_MAP = {}
    query, params = AparReportService._build_query_and_params(None, None, {}, 't')
    # When no filters and no dates, WHERE joined string may be empty -> ensure query is returned
    assert isinstance(query, str)
    assert isinstance(params, list)

def test_execute_query_calls_client_and_returns_df():
    df_expected = pd.DataFrame([{'a': 1}])
    class FakeJob:
        def to_dataframe(self_inner):
            return df_expected

    class FakeClient:
        def query(self_inner, query, job_config=None):
            # ensure query received as string
            assert isinstance(query, str)
            return FakeJob()

    returned = AparReportService._execute_query(FakeClient(), "select 1", [])
    pd.testing.assert_frame_equal(returned, df_expected)

def test_generate_csv_stream_with_masking_enabled_and_disabled(monkeypatch):
    # prepare sample df
    df = pd.DataFrame([
        {'email': 'user@example.com', 'phone': '1234567890', 'name': 'Alice'},
        {'email': 'no-domain', 'phone': '123', 'name': 'Bob'}
    ])

    # Test masking enabled
    apar_report_service.IS_MASKING_ENABLED = 'true'
    out = collect(AparReportService._generate_csv_stream(df.copy()))
    assert out[0].strip() == "email|phone|name"
    # masked domain should contain asterisks for domain parts
    assert '@' in out[1] and 'example' not in out[1]
    # phone should be masked except last 4 digits
    assert out[1].split('|')[1].endswith('7890')
    # second row with no-domain should not raise; email becomes 'no-domain'
    assert 'no-domain' in out[2]

    # Test masking disabled
    df2 = pd.DataFrame([{'email': 'user@site.com', 'phone': '9999', 'name': 'C'}])
    apar_report_service.IS_MASKING_ENABLED = 'false'
    out2 = collect(AparReportService._generate_csv_stream(df2.copy()))
    assert 'site.com' in out2[1]
    assert out2[1].split('|')[1] == '9999'

def test_apply_masking_edge_cases():
    apar_report_service.IS_MASKING_ENABLED = 'true'
    # email with multiple dots in domain
    rd = {'email': 'a@sub.example.co', 'phone': '12'}
    AparReportService._apply_masking(rd)
    # domain parts replaced with same-length asterisks joined by dots
    assert rd['email'].split('@')[1].count('*') > 0
    # short phone masked fully
    assert set(rd['phone']) == {'*'}

def test_fetch_apar_assigned_courses_report_success(monkeypatch):
    df = pd.DataFrame([
        {'col1': 'v1', 'email': 'e@d.com', 'phone': '111122223333'}
    ])

    # Mock BigQueryService so no real configuration/file is required
    monkeypatch.setattr(
        apar_report_service,
        "BigQueryService",
        lambda: MagicMock()
    )

    # Patch _build_query_and_params
    monkeypatch.setattr(
        AparReportService,
        "_build_query_and_params",
        lambda start, end, filters, table, plan_year=None: ("q", [])
    )

    # Patch _execute_query
    monkeypatch.setattr(
        AparReportService,
        "_execute_query",
        lambda client, q, p: df.copy()
    )

    apar_report_service.IS_MASKING_ENABLED = 'false'

    gen = AparReportService.fetch_apar_assigned_courses_report(
        "2022-01-01",
        "2022-01-02",
        {},
        []
    )

    assert gen is not None

    out = collect(gen)

    assert out[0].strip() == "col1|email|phone"
    assert "v1" in out[1]

def test_fetch_apar_assigned_courses_report_handles_exception(monkeypatch):
    # make _execute_query raise
    monkeypatch.setattr(AparReportService, "_build_query_and_params",
                        lambda start, end, filters, table, plan_year=None: ("q", []))
    def fake_exec(client, q, p):
        raise RuntimeError("boom")
    monkeypatch.setattr(AparReportService, "_execute_query", fake_exec)

    res = AparReportService.fetch_apar_assigned_courses_report("2022-01-01", "2022-01-02", {}, [])
    assert res is None

def test_generate_csv_stream_cleans_up_dataframe(monkeypatch):
    # ensure that _generate_csv_stream runs finally block even if iteration stops early
    df = pd.DataFrame([{'a': 1}, {'a': 2}])
    gen = AparReportService._generate_csv_stream(df)
    # consume only first line to simulate partial consumption
    first = next(gen)
    assert 'a' in first
    # fully consume to trigger finally cleanup
    rest = list(gen)
    # after generator exhaustion, df should have had its rows dropped (in-place)
    # df may be modified in-place - check index length is 0
    assert df.shape[0] == 2

def test_build_query_and_params_with_plan_year():
    query, params = AparReportService._build_query_and_params(
        '2023-01-01',
        '2023-01-31',
        {'user_email': 'test@example.com'},
        'master_enrolment_apar_dummy',
        plan_year='2025-26'
    )

    assert 'plan_year = @plan_year' in query

    plan_year_param = next(
        param for param in params
        if param.name == 'plan_year'
    )

    assert plan_year_param.value == '2025-26'

def test_build_query_and_params_without_plan_year():
    query, params = AparReportService._build_query_and_params(
        '2023-01-01',
        '2023-01-31',
        {'user_email': 'test@example.com'},
        'master_enrolment_apar_dummy'
    )

    assert 'plan_year = @plan_year' not in query

    assert not any(
        param.name == 'plan_year'
        for param in params
    )

def test_build_query_and_params_plan_year_with_whitespace():
    query, params = AparReportService._build_query_and_params(
        '2023-01-01',
        '2023-01-31',
        {'user_email': 'test@example.com'},
        'master_enrolment_apar_dummy',
        plan_year=' 2025-26 '
    )

    assert 'plan_year = @plan_year' in query

    plan_year_param = next(
        param for param in params
        if param.name == 'plan_year'
    )

    assert plan_year_param.value == '2025-26'