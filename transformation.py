"""
GlobalFin Customer 360 Platform - Data Transformation Layer
Simulates ESB/iPaaS transformation logic
"""

import sqlite3
import re
from datetime import datetime

def clean_email(email):
    """Standardize email format"""
    return email.lower().strip()

def standardize_name(name):
    """Title case names"""
    return name.strip().title()

def validate_phone(phone):
    """Basic phone validation"""
    digits = re.sub(r'\D', '', phone)
    return len(digits) >= 10

def transform_source_to_mdm():
    print("=" * 60)
    print("GlobalFin Customer 360 - Data Transformation Layer")
    print("=" * 60)
    print()
    print("Starting ETL process: Source Systems → MDM...")
    print()
    
    # Read from source
    source_conn = sqlite3.connect('source-systems.db')
    source_cursor = source_conn.cursor()
    source_cursor.execute("SELECT * FROM crm_customers")
    customers = source_cursor.fetchall()
    source_conn.close()
    
    print(f"[1/3] Extracted {len(customers)} records from CRM")
    
    # Transform and load to MDM
    mdm_conn = sqlite3.connect('mdm.db')
    mdm_cursor = mdm_conn.cursor()
    
    transformed_count = 0
    skipped_count = 0
    
    print("[2/3] Transforming data...")
    
    for customer in customers:
        customer_id, first_name, last_name, email, phone, age, address, city, country, created_at, updated_at = customer
        
        # Data transformations
        email_clean = clean_email(email)
        first_name_clean = standardize_name(first_name)
        last_name_clean = standardize_name(last_name)
        
        # Data quality scoring
        quality_score = 100
        if not phone or not validate_phone(phone):
            quality_score -= 10
        if not address:
            quality_score -= 10
        if not city:
            quality_score -= 10
        
        # Confidence score (simulated deduplication confidence)
        confidence_score = round(random.uniform(0.85, 0.99), 3)
        
        try:
            mdm_cursor.execute('''INSERT OR IGNORE INTO golden_records 
                                  (first_name, last_name, email, phone, age, address, city, country,
                                   source_system, source_id, confidence_score, data_quality_score) 
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                               (first_name_clean, last_name_clean, email_clean, phone, age, 
                                address, city, country, 'CRM_Salesforce', customer_id, 
                                confidence_score, quality_score))
            transformed_count += 1
        except sqlite3.IntegrityError:
            skipped_count += 1
            continue
    
    mdm_conn.commit()
    mdm_conn.close()
    
    print(f"[3/3] Loaded {transformed_count} golden records to MDM")
    if skipped_count > 0:
        print(f"      ⚠ Skipped {skipped_count} duplicates")
    
    print()
    print("=" * 60)
    print("✅ Transformation complete")
    print("=" * 60)
    print()
    print(f"Statistics:")
    print(f"  • Records Extracted: {len(customers)}")
    print(f"  • Records Transformed: {transformed_count}")
    print(f"  • Duplicates Skipped: {skipped_count}")
    print(f"  • Success Rate: {(transformed_count/len(customers)*100):.1f}%")
    print()

import random

if __name__ == "__main__":
    transform_source_to_mdm()
