"""
GlobalFin Customer 360 Platform - Data Activation Script
Simulates customer data generation from source systems (CRM)
"""

import sqlite3
import random
from datetime import datetime, timedelta

# Install: pip install faker
try:
    from faker import Faker
except ImportError:
    print("❌ Error: 'faker' library not installed")
    print("   Install: pip install faker")
    exit(1)

fake = Faker('nl_NL')  # Dutch locale for realistic data

def generate_customer_data(num_customers=100):
    print("=" * 60)
    print("GlobalFin Customer 360 - Data Activation")
    print("=" * 60)
    print()
    print(f"Generating {num_customers} synthetic customer records...")
    print()
    
    conn = sqlite3.connect('source-systems.db')
    c = conn.cursor()
    
    generated_emails = set()
    successful_inserts = 0
    
    for i in range(num_customers):
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # Generate unique email
        email_base = f"{first_name.lower()}.{last_name.lower()}@example.com"
        email = email_base
        counter = 1
        while email in generated_emails:
            email = f"{first_name.lower()}.{last_name.lower()}{counter}@example.com"
            counter += 1
        generated_emails.add(email)
        
        age = random.randint(18, 75)
        phone = fake.phone_number()
        address = fake.street_address()
        city = fake.city()
        
        try:
            c.execute('''INSERT INTO crm_customers 
                         (first_name, last_name, email, phone, age, address, city) 
                         VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                      (first_name, last_name, email, phone, age, address, city))
            successful_inserts += 1
            
            if (i + 1) % 20 == 0:
                print(f"   ✓ Generated {i + 1}/{num_customers} customers...")
                
        except sqlite3.IntegrityError:
            print(f"   ⚠ Skipped duplicate: {email}")
            continue
    
    # Update metadata
    c.execute('''INSERT INTO source_metadata (source_name, last_sync, record_count) 
                 VALUES (?, ?, ?)''', 
              ('CRM_Salesforce', datetime.now(), successful_inserts))
    
    conn.commit()
    conn.close()
    
    print()
    print("=" * 60)
    print(f"✅ Successfully generated {successful_inserts} customer records")
    print("=" * 60)
    print()
    print("Sample Data:")
    
    # Show sample
    conn = sqlite3.connect('source-systems.db')
    c = conn.cursor()
    c.execute("SELECT first_name, last_name, email, age FROM crm_customers LIMIT 5")
    samples = c.fetchall()
    conn.close()
    
    for sample in samples:
        print(f"  • {sample[0]} {sample[1]} ({sample[2]}) - Age: {sample[3]}")
    print()

if __name__ == "__main__":
    import sys
    
    num = 100
    if len(sys.argv) > 1:
        try:
            num = int(sys.argv[1])
        except:
            print("Usage: python activate.py [number_of_customers]")
            sys.exit(1)
    
    generate_customer_data(num)
