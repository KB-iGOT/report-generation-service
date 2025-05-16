import os
import json

DEFAULT_TABLE_NAME = os.environ.get('DEFAULT_TABLE_NAME', 'wf_status')
USER_DETAILS_TABLE = os.environ.get('USER_DETAILS_TABLE', 'user_detail')
CONTENT_TABLE = os.environ.get('CONTENT_TABLE', 'content')
USER_ENROLMENTS_TABLE = os.environ.get('USER_ENROLMENTS_TABLE', 'user_enrolment')
REQUIRED_COLUMNS_FOR_ENROLLMENTS = ["user_id", "full_name", "content_id","content_name","content_type","content_type","certificate_id","enrolled_on","certificate_generated","first_completed_on","last_completed_on","content_duration","content_progress_percentage"]
SUNBIRD_SSO_URL = os.environ.get('SUNBIRD_SSO_URL', 'https://sso.example.com')
SUNBIRD_SSO_REALM = os.environ.get('SUNBIRD_SSO_REALM', 'https://sso.example.com')
ACCESS_TOKEN_PUBLICKEY_BASEPATH = os.environ.get('accesstoken_publickey_basepath')
IS_VALIDATION_ENABLED = os.environ.get('IS_VALIDATION_ENABLED', 'false')
X_AUTHENTICATED_USER_TOKEN = 'x-authenticated-user-token'
GCP_PROJECT = os.environ.get('GCP_PROJECT', 'table')
GCP_DB_NAME = os.environ.get('GCP_DB_NAME', 'data')
GCP_ENROLMENT_TABLE_NAME = os.environ.get('GCP_ENROLMENT_TABLE_NAME', 'table')
MASTER_ENROLMENTS_TABLE = f"{GCP_PROJECT}.{GCP_DB_NAME}.{GCP_ENROLMENT_TABLE_NAME}"
GCP_ORG_HIERARCHY_TABLE_NAME = os.environ.get('GCP_ORG_HIERARCHY_TABLE_NAME', 'table')
MASTER_ORG_HIERARCHY_TABLE = f"{GCP_PROJECT}.{GCP_DB_NAME}.{GCP_ORG_HIERARCHY_TABLE_NAME}"
GCP_ORG_USER_TABLE_NAME = os.environ.get('GCP_ORG_USER_TABLE_NAME', 'table')
MASTER_USER_TABLE = f"{GCP_PROJECT}.{GCP_DB_NAME}.{GCP_ORG_USER_TABLE_NAME}"
GCP_CREDENTIALS_PATH = os.environ.get('GCP_CREDENTIALS_PATH', 'test')
IS_MASKING_ENABLED = os.environ.get('IS_MASKING_ENABLED', 'False')
SYNC_TABLES=os.environ.get("SYNC_TABLES", "table")
GCS_URI_MASTER_USER_DETAILS=os.environ.get("GCS_URI_MASTER_USER_DETAILS","file")
GCS_URI_MASTER_USER_ENROLMENTS=os.environ.get("GCS_URI_MASTER_USER_ENROLMENTS","file")
GCS_URI_MASTER_ORG_HIERARCHY_DATA=os.environ.get("GCS_URI_MASTER_ORG_HIERARCHY_DATA","file")
DATASET=os.environ.get("GCS_URI_MASTER_ORG_HIERARCHY_DATA","data")
MERGE_KEYS_MASTER_USER_DETAILS=os.environ.get("MERGE_KEYS_MASTER_USER_DETAILS","id")
MERGE_KEYS_MASTER_USER_ENROLMENTS=os.environ.get("MERGE_KEYS_MASTER_USER_ENROLMENTS","table")
MERGE_KEYS_MASTER_ORG_HIERARCHY_DATA=os.environ.get("MERGE_KEYS_MASTER_ORG_HIERARCHY_DATA","table")
X_ORG_ID = 'x_org_id'
MAX_ORG_CACHE_SIZE = os.environ.get("MAX_ORG_CACHE_SIZE", 1000)
MAX_ORG_CACHE_AGE = os.environ.get("MAX_ORG_CACHE_AGE", 14400)
ENROLMENT_FILTER_CONFIG = json.loads(os.environ.get("ENROLMENT_FILTER_CONFIG", """
{
    "content_id": {"type": "list"},
    "mdo_id_list": {"type": "list"},
    "user_id": {"type": "list"},
    "content_progress_percentage": {"type": "comparison", "valid_operators": [">=", "=<", ">", "<", "="]},
    "certificate_generated": {"type": "string"}
}
"""))

USER_FILTER_CONFIG = json.loads(os.environ.get("USER_FILTER_CONFIG", """
{
    "mdo_id_list": {"type": "list"},
    "status": {"type": "string", "values": {"Active": 1, "Inactive": 0}},
    "user_registration_date": {"type": "comparison", "valid_operators": [">=", "<=", ">", "<", "="]},
    "is_verified_karmayogi": {"type": "boolean", "values": {"True": true, "False": false, "true": true, "false": false}},
    "groups": {"type": "list"},
    "user_id": {"type": "list"},
    "designation": {"type": "list"}
}
"""))

USER_REPORT_FILTER_CONFIG = json.loads(os.environ.get("USER_REPORT_FILTER_CONFIG", """
{
    "content_id": {"type": "list"},
    "mdo_id_list": {"type": "list"},
    "content_progress_percentage": {"type": "comparison", "valid_operators": [">", "<", ">=", "<=", "="]},
    "certificate_generated": {"type": "string"}
}
"""))
 
