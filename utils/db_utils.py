import duckdb
import os
import re

DB_PATH = "data/minerals_etl.duckdb"

def get_connection():
    os.makedirs("data", exist_ok=True)
    return duckdb.connect(DB_PATH)

def create_schema(conn, schema_name):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

def standardise_phone(phone):
    if phone is None:
        return None
    
    # Strip Unicode control characters and non-breaking spaces
    phone = re.sub(r'[\u202a\u202c\u200f\u200e\u00a0]', '', phone)
    
    # Strip ERROR values
    if '#ERROR' in phone:
        return None
    
    # Remove spaces, hyphens, brackets
    phone = re.sub(r'[\s\-\(\)]', '', phone)
    
    # Remove +234(0) pattern
    phone = phone.replace('+234(0)', '+234')
    
    # Normalise to +234 format
    if phone.startswith('0') and len(phone) == 11:
        phone = '+234' + phone[1:]
    elif phone.startswith('234') and not phone.startswith('+234'):
        phone = '+' + phone
    
    # Validate final format — must be +234 followed by 10 digits
    if re.match(r'^\+234\d{10}$', phone):
        return phone
    
    return None