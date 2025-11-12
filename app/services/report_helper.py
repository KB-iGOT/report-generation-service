"""Helper functions for report generation service"""

import logging
import gc
from typing import List, Dict, Any, Generator, Optional
import pandas as pd

logger = logging.getLogger(__name__)

def build_date_filter(start_date: str, end_date: str, column_name: str = "enrolled_on") -> Optional[str]:
    """Build date range filter"""
    if start_date and end_date:
        return f"{column_name} BETWEEN '{start_date}' AND '{end_date}'"
    return None

def build_enrolments_query(master_table: str, where_clause: str) -> str:
    """Build the enrolments query"""
    return f"""
        SELECT * 
        FROM `{master_table}`
        WHERE {where_clause}
    """

def build_user_query(master_table: str, where_clause: str) -> str:
    """Build the user query"""
    return f"""
        SELECT user_id, mdo_id
        FROM `{master_table}`
        WHERE {where_clause}
    """

def generate_csv_stream(df: 'pd.DataFrame', cols: List[str], separator: str = '|') -> Generator[str, None, None]:
    """Generate a CSV stream from a DataFrame with proper cleanup"""
    try:
        yield separator.join(cols) + '\n'
        for row in df.itertuples(index=False, name=None):
            yield separator.join(map(str, row)) + '\n'
    finally:
        df.drop(df.index, inplace=True)
        del df
        gc.collect()
        logger.info("Cleaned up DataFrame after streaming.")

def filter_required_columns(df: 'pd.DataFrame', required_columns: List[str]) -> 'pd.DataFrame':
    """Filter DataFrame to include only required columns"""
    if not required_columns:
        return df
        
    existing_columns = [col for col in required_columns if col in df.columns]
    missing_columns = list(set(required_columns) - set(existing_columns))
    if missing_columns:
        logger.info(f"Warning: Missing columns skipped: {missing_columns}")
    return df[existing_columns]