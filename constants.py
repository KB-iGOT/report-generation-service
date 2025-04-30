import os

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
GCP_PROJECT = os.environ.get('GCP_PROJECT', 'prj-kb-nprd-uat-gcp-1006')
GCP_DB_NAME = os.environ.get('GCP_DB_NAME', 'cumulative_master_data')
GCP_ENROLMENT_TABLE_NAME = os.environ.get('GCP_ENROLMENT_TABLE_NAME', 'master_user_enrolments')
MASTER_ENROLMENTS_TABLE = f"{GCP_PROJECT}.{GCP_DB_NAME}.{GCP_ENROLMENT_TABLE_NAME}"
GCP_ORG_HIERARCHY_TABLE_NAME = os.environ.get('GCP_ORG_HIERARCHY_TABLE_NAME', 'master_org_hierarchy_data')
MASTER_ORG_HIERARCHY_TABLE = f"{GCP_PROJECT}.{GCP_DB_NAME}.{GCP_ORG_HIERARCHY_TABLE_NAME}"
GCP_ORG_USER_TABLE_NAME = os.environ.get('GCP_ORG_USER_TABLE_NAME', 'master_user_details')
MASTER_USER_TABLE = f"{GCP_PROJECT}.{GCP_DB_NAME}.{GCP_ORG_USER_TABLE_NAME}"
GCP_CREDENTIALS_PATH = os.environ.get('GCP_CREDENTIALS_PATH', '/home/sahilchaudhary/Downloads/prj-kb-nprd-uat-gcp-1006-c691f3c2615b.json')
IS_MASKING_ENABLED = os.environ.get('IS_MASKING_ENABLED', 'False')
#SYNC_TABLES=os.environ.get("SYNC_TABLES", "master_user_enrolments, master_org_hierarchy_data, master_user_details_test")
SYNC_TABLES=os.environ.get("SYNC_TABLES", "master_user_details_test")
GCS_URI_MASTER_USER_DETAILS_TEST=os.environ.get("GCS_URI_MASTER_USER_DETAILS","gs://igotuatdp/unifiedReports/unified_user_details.parquet")
GCS_URI_MASTER_USER_ENROLMENTS=os.environ.get("GCS_URI_MASTER_USER_ENROLMENTS","gs://igotuatdp/unifiedReports/unified_enrolments_latest.parquet")
GCS_URI_MASTER_ORG_HIERARCHY_DATA=os.environ.get("GCS_URI_MASTER_ORG_HIERARCHY_DATA","gs://igotuatdp/unifiedReports/unified_org_details.parquet")
DATASET=os.environ.get("GCS_URI_MASTER_ORG_HIERARCHY_DATA","cumulative_master_data")
MERGE_KEYS_MASTER_USER_DETAILS_TEST=os.environ.get("MERGE_KEYS_MASTER_USER_DETAILS","user_id, mdo_id")
MERGE_KEYS_MASTER_USER_ENROLMENTS=os.environ.get("MERGE_KEYS_MASTER_USER_ENROLMENTS","user_id, content_id, mdo_id")
MERGE_KEYS_MASTER_ORG_HIERARCHY_DATA=os.environ.get("MERGE_KEYS_MASTER_ORG_HIERARCHY_DATA","mdo_id")

