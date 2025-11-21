import pytest
from unittest import mock
import pandas as pd

from app.services.ehrms_report_service import EhrmsReportService


@mock.patch('app.services.ehrms_report_service.BigQueryService')
def test_get_user_ids_no_filters(mock_bq):
    svc = mock_bq.return_value
    res = EhrmsReportService._get_user_ids(svc, None, None, None)
    assert res is None


@mock.patch('app.services.ehrms_report_service.BigQueryService')
def test_get_user_ids_success(mock_bq):
    svc = mock_bq.return_value
    df = pd.DataFrame({'user_id': ['u1', 'u2'], 'mdo_id': ['m1', 'm1']})
    svc.run_query.return_value = df

    res = EhrmsReportService._get_user_ids(svc, 'a@b.com', None, None)
    assert res == ['u1', 'u2']
    svc.run_query.assert_called_once()


@mock.patch('app.services.ehrms_report_service.BigQueryService')
def test_fetch_user_cumulative_report_no_user_found(mock_bq):
    svc = mock_bq.return_value
    # user query returns empty
    svc.run_query.return_value = pd.DataFrame()

    res = EhrmsReportService.fetch_user_cumulative_report(email='x@y.com')
    assert res is None


@mock.patch('app.services.ehrms_report_service.BigQueryService')
def test_fetch_user_cumulative_report_success(mock_bq):
    svc = mock_bq.return_value
    # First call: user df
    user_df = pd.DataFrame({'user_id': ['u1'], 'mdo_id': ['m1']})
    # Second call: enrollment df
    enrol_df = pd.DataFrame({'user_id': ['u1'], 'enrolled_on': ['2024-01-02'], 'content': ['c1']})
    svc.run_query.side_effect = [user_df, enrol_df]

    gen = EhrmsReportService.fetch_user_cumulative_report(email='a@b.com', required_columns=['user_id', 'enrolled_on'])
    assert gen is not None
    out = ''.join(list(gen))
    assert 'user_id|enrolled_on' in out
    assert 'u1|2024-01-02' in out


@mock.patch('app.services.ehrms_report_service.BigQueryService')
def test_fetch_master_enrolments_data_returns_csv(mock_bq):
    svc = mock_bq.return_value
    # Return some rows from bigquery
    df = pd.DataFrame({'mdo_id': ['m1'], 'user_id': ['u1'], 'enrolled_on': ['2024-01-02']})
    svc.run_query.return_value = df

    gen = EhrmsReportService.fetch_master_enrolments_data('2024-01-01', '2024-01-10', required_columns=['mdo_id', 'user_id'])
    assert gen is not None
    s = ''.join(list(gen))
    assert 'mdo_id|user_id' in s
    assert 'm1|u1' in s


@mock.patch('app.services.ehrms_report_service.BigQueryService')
@mock.patch('app.services.ehrms_report_service.IS_MASKING_ENABLED', 'true')
@mock.patch('app.services.ehrms_report_service.DOPT_EHRMS_EXTERNAL_SYSTEM_NAME', 'DoPT eHRMS')
def test_fetch_master_user_data_masking(mock_bq):
    svc = mock_bq.return_value
    df = pd.DataFrame({'user_id': ['u1'], 'email': ['john.doe@example.com'], 'phone_number': ['9876543210'], 'external_system': ['DoPT eHRMS']})
    svc.run_query.return_value = df

    gen = EhrmsReportService.fetch_master_user_data(user_creation_start_date='2024-01-01', user_creation_end_date='2024-01-10', required_columns=['user_id', 'email', 'phone_number'])
    assert gen is not None
    out = ''.join(list(gen))
    # email domain should be masked
    assert 'john.doe@' in out
    assert '*' in out
    # phone number masked to show last 4 digits
    assert out.count('*') >= 1

