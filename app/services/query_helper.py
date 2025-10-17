"""Helper functions for SQL query generation"""

from typing import List, Optional, Dict, Any
from google.cloud import bigquery

def build_date_filter(start_date: Optional[str], end_date: Optional[str], column: str = "enrolled_on") -> str:
    """Build date range filter"""
    if start_date and end_date:
        return f" AND {column} BETWEEN '{start_date}' AND '{end_date}'"
    return ""

def build_mdo_id_filter(mdo_ids: List[str]) -> str:
    """Build MDO ID filter"""
    mdo_id_list = [f"'{mid}'" for mid in mdo_ids]
    return f"mdo_id IN ({', '.join(mdo_id_list)})"

def build_user_filter(email: Optional[str], phone: Optional[str], ehrms_id: Optional[str]) -> List[str]:
    """Build user filter conditions"""
    filters = []
    if email:
        filters.append(f"email = '{email}'")
    if phone:
        filters.append(f"phone_number = '{phone}'")
    if ehrms_id:
        filters.append(f"external_system_id = '{ehrms_id}'")
    return filters

def build_apar_query(table: str, filters: Dict[str, Any], filter_map: Dict[str, str], 
                    start_date: Optional[str] = None, end_date: Optional[str] = None) -> tuple[str, List[bigquery.ScalarQueryParameter]]:
    """Build APAR query with parameters"""
    filter_clauses = []
    params = []
    
    # Add date filters
    if start_date and end_date:
        filter_clauses.insert(0, "enrolled_on >= @start_date AND enrolled_on <= @end_date")
        params.extend([
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date)
        ])

    # Add other filters
    for key, value in filters.items():
        if value and key in filter_map:
            bq_col = filter_map[key]
            filter_clauses.append(f"{bq_col} = @{bq_col}")
            params.append(bigquery.ScalarQueryParameter(bq_col, "STRING", value.strip()))

    # Build final query
    if filter_clauses:
        query = f"""
            SELECT *
            FROM `{table}`
            WHERE {" AND ".join(filter_clauses)}
        """
    else:
        query = f"SELECT * FROM `{table}`"

    return query, params