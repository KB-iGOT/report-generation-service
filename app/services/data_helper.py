"""Helper functions for data masking and stream generation"""

import logging
import gc
from typing import Dict, List, Generator, Any
import pandas as pd

logger = logging.getLogger(__name__)

def mask_email(email: str) -> str:
    """Mask email address while preserving username"""
    if not email:
        return email
    
    parts = email.split('@')
    if len(parts) == 2:
        domain_parts = parts[1].split('.')
        masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
        return f"{parts[0]}@{masked_domain}"
    return parts[0]

def mask_phone(phone: str) -> str:
    """Mask phone number while preserving last 4 digits"""
    if not phone:
        return phone
        
    phone = str(phone)
    if len(phone) >= 4:
        return '*' * (len(phone) - 4) + phone[-4:]
    return '*' * len(phone)

def mask_sensitive_data(row_dict: Dict[str, Any], masking_enabled: bool) -> Dict[str, Any]:
    """Apply masking to sensitive data fields"""
    if not masking_enabled:
        return row_dict
        
    masked = row_dict.copy()
    if 'email' in masked and masked['email']:
        masked['email'] = mask_email(masked['email'])
    if any(key in masked for key in ['phone_number', 'phone']) and masked.get('phone_number') or masked.get('phone'):
        phone_key = 'phone_number' if 'phone_number' in masked else 'phone'
        masked[phone_key] = mask_phone(masked[phone_key])
    return masked

def filter_required_columns(df: 'pd.DataFrame', required_columns: List[str]) -> 'pd.DataFrame':
    """Filter DataFrame to include only required columns"""
    if not required_columns:
        return df
        
    existing_columns = [col for col in required_columns if col in df.columns]
    missing_columns = list(set(required_columns) - set(existing_columns))
    if missing_columns:
        logger.info(f"Warning: Missing columns skipped: {missing_columns}")
    return df[existing_columns]

def generate_csv_stream(df: 'pd.DataFrame', cols: List[str], masking_enabled: bool = False, separator: str = '|') -> Generator[str, None, None]:
    """Generate a CSV stream from a DataFrame with optional masking"""
    try:
        yield separator.join(cols) + '\n'
        for row in df.itertuples(index=False, name=None):
            row_dict = dict(zip(cols, row))
            if masking_enabled:
                row_dict = mask_sensitive_data(row_dict, masking_enabled)
            yield separator.join(str(row_dict.get(col, '')) for col in cols) + '\n'
    finally:
        df.drop(df.index, inplace=True)
        del df
        gc.collect()
        logger.info("Cleaned up DataFrame after streaming.")