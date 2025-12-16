"""
GlobalFin Customer 360 Platform - CDP Data Synchronization
Syncs golden records from MDM to CDP with enrichment
"""

import sqlite3
import random

def calculate_segment(age):
    """Determine customer segment based on age"""
    if age < 30:
        return "Young Professional"
    elif age < 50:
        return "Mid-Career Wealth Builder"
    else:
        return "Senior Wealth Management"

def calculate_lifecycle_stage(age):
    """Determine lifecycle stage"""
    if age < 25:
        return "Acquisition"
    elif age < 40:
        return "Growth"
    elif age < 60:
        return "Retention"
    else:
        return "Loyalty"

def calculate_ltv(age, segment):
    """Calculate estimated lifetime value"""
    base_ltv = age * 125
    segment_multiplier = {
        "Young Professional": 1.2,
        "Mid-Career Wealth Builder": 1.5,
        "Senior Wealth Management": 1.8
    }
    return round(base_ltv * segment_multiplier.get(segment, 1.0) + random.randint(0, 5000), 2)

def sync_mdm_to_cdp():
    print("=" * 60)
    print("GlobalFin Customer 360 - CDP Data Synchronization")
    print("=" * 60)
    print()
    print("Syncing golden records from MDM to CDP...")
    print()
    
    # Read from MDM
    mdm_conn = sqlite3.connect('mdm.db')
    mdm_cursor = mdm_conn.cursor()
    mdm_cursor.execute("SELECT golden_id, first_name, last_name, email, age FROM golden_records WHERE is_active = 1")
    golden_records = mdm_cursor.fetchall()
    mdm_conn.close()
    
    print(f"[1/3] Retrieved {len(golden_records)} golden records from MDM")
    
    # Write to CDP with enrichment
    cdp_conn = sqlite3.connect('cdp.db')
    cdp_cursor = cdp_conn.cursor()
    
    synced_count = 0
    
    print("[2/3] Enriching and loading to CDP...")
    
    for record in golden_records:
        golden_id, first_name, last_name, email, age = record
        
        # Enrichment logic
        segment = calculate_segment(age)
        lifecycle_stage = calculate_lifecycle_stage(age)
        ltv = calculate_ltv(age, segment)
        risk_score = random.randint(70, 99)
        propensity_score = round(random.uniform(0.3, 0.9), 2)
        
        # Preferred channel (based on age)
        if age < 35:
            preferred_channel = "Mobile App"
        elif age < 55:
            preferred_channel = "Email"
        else:
            preferred_channel = "Branch"
        
        # Product holdings (mock)
        products = random.choice([
            "Checking Account",
            "Checking Account, Savings Account",
            "Checking Account, Savings Account, Credit Card",
            "Checking Account, Mortgage"
        ])
        
        cdp_cursor.execute('''INSERT OR REPLACE INTO customer_profiles 
                              (customer_id, golden_id, first_name, last_name, email, age, 
                               segment, lifecycle_stage, lifetime_value, risk_score, 
                               propensity_score, preferred_channel, product_holdings) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (golden_id, golden_id, first_name, last_name, email, age,
                            segment, lifecycle_stage, ltv, risk_score, propensity_score,
                            preferred_channel, products))
        synced_count += 1
    
    # Update segment counts
    print("[3/3] Updating segment statistics...")
    for segment_name in ["Young Professional", "Mid-Career Wealth Builder", "Senior Wealth Management"]:
        cdp_cursor.execute("SELECT COUNT(*) FROM customer_profiles WHERE segment = ?", (segment_name,))
        count = cdp_cursor.fetchone()[0]
        cdp_cursor.execute('''INSERT OR REPLACE INTO customer_segments (segment_name, customer_count)
                              VALUES (?, ?)''', (segment_name, count))
    
    cdp_conn.commit()
    cdp_conn.close()
    
    print()
    print("=" * 60)
    print(f"✅ Successfully synced {synced_count} customer profiles to CDP")
    print("=" * 60)
    print()
    
    # Show segment distribution
    cdp_conn = sqlite3.connect('cdp.db')
    cdp_cursor = cdp_conn.cursor()
    cdp_cursor.execute("SELECT segment, COUNT(*) FROM customer_profiles GROUP BY segment")
    segments = cdp_cursor.fetchall()
    cdp_conn.close()
    
    print("Segment Distribution:")
    for seg in segments:
        print(f"  • {seg[0]}: {seg[1]} customers")
    print()

if __name__ == "__main__":
    sync_mdm_to_cdp()
