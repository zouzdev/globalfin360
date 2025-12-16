"""
GlobalFin Customer 360 Platform - Database Setup
Creates all required database schemas for the demo
"""

import sqlite3
from datetime import datetime

def create_databases():
    print("=" * 60)
    print("GlobalFin Customer 360 Platform - Database Initialization")
    print("=" * 60)
    print()
    
    # 1. Source Systems Database
    print("[1/5] Creating Source Systems Database...")
    conn = sqlite3.connect('source-systems.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS crm_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        age INTEGER,
        address TEXT,
        city TEXT,
        country TEXT DEFAULT 'Netherlands',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS source_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT,
        last_sync TIMESTAMP,
        record_count INTEGER
    )''')
    conn.commit()
    conn.close()
    print("   ✓ Source Systems DB created")
    
    # 2. MDM Database
    print("[2/5] Creating Master Data Management Database...")
    conn = sqlite3.connect('mdm.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS golden_records (
        golden_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        age INTEGER,
        address TEXT,
        city TEXT,
        country TEXT,
        source_system TEXT,
        source_id INTEGER,
        confidence_score REAL DEFAULT 1.0,
        data_quality_score INTEGER DEFAULT 100,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS match_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        golden_id INTEGER,
        match_type TEXT,
        match_score REAL,
        matched_records TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (golden_id) REFERENCES golden_records(golden_id)
    )''')
    conn.commit()
    conn.close()
    print("   ✓ MDM DB created")
    
    # 3. CDP Database
    print("[3/5] Creating Customer Data Platform Database...")
    conn = sqlite3.connect('cdp.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS customer_profiles (
        customer_id INTEGER PRIMARY KEY,
        golden_id INTEGER UNIQUE,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        age INTEGER,
        segment TEXT,
        lifecycle_stage TEXT,
        lifetime_value REAL,
        risk_score INTEGER,
        propensity_score REAL,
        preferred_channel TEXT,
        product_holdings TEXT,
        last_interaction_date TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (golden_id) REFERENCES mdm.golden_records(golden_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS customer_segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        segment_name TEXT UNIQUE,
        segment_criteria TEXT,
        customer_count INTEGER DEFAULT 0
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS customer_interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        interaction_type TEXT,
        channel TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customer_profiles(customer_id)
    )''')
    conn.commit()
    conn.close()
    print("   ✓ CDP DB created")
    
    # 4. Data Warehouse Database
    print("[4/5] Creating Data Warehouse Database...")
    conn = sqlite3.connect('datawarehouse.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS customer_analytics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        metric_name TEXT,
        metric_value REAL,
        metric_date DATE,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS campaign_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_name TEXT,
        segment TEXT,
        sent_count INTEGER,
        open_count INTEGER,
        click_count INTEGER,
        conversion_count INTEGER,
        revenue REAL,
        campaign_date DATE
    )''')
    conn.commit()
    conn.close()
    print("   ✓ Data Warehouse DB created")
    
    # 5. CJOP Database
    print("[5/5] Creating Customer Journey Orchestration Platform Database...")
    conn = sqlite3.connect('cjop.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        customer_age INTEGER,
        customer_segment TEXT,
        channel TEXT,
        campaign_name TEXT,
        personalized_message TEXT,
        ai_model_used TEXT,
        decision_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS journey_states (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        journey_name TEXT,
        current_step TEXT,
        next_action TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS ab_tests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        test_name TEXT,
        variant TEXT,
        customer_id INTEGER,
        result TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    conn.close()
    print("   ✓ CJOP DB created")
    
    print()
    print("=" * 60)
    print("✅ All databases successfully created!")
    print("=" * 60)
    print()
    print("Database Files:")
    print("  • source-systems.db")
    print("  • mdm.db")
    print("  • cdp.db")
    print("  • datawarehouse.db")
    print("  • cjop.db")
    print()

if __name__ == "__main__":
    create_databases()
