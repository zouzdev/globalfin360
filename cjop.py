"""
GlobalFin Customer 360 Platform - CJOP Orchestration
Customer Journey Orchestration Platform with AI integration
"""

import sqlite3
import requests
import json
from datetime import datetime

GEMINI_API_KEY = 'AIzaSyDNEchnzTrA9Ej8nko7LX2poS2NKHulyac'
GEMINI_API_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent'

def get_customer_profile(age):
    """Retrieve customer profile from CDP based on age"""
    conn = sqlite3.connect('cdp.db')
    c = conn.cursor()
    c.execute('''SELECT customer_id, first_name, last_name, email, segment, 
                        lifetime_value, risk_score 
                 FROM customer_profiles 
                 WHERE age = ? 
                 LIMIT 1''', (age,))
    profile = c.fetchone()
    conn.close()
    
    if profile:
        return {
            'customer_id': profile[0],
            'first_name': profile[1],
            'last_name': profile[2],
            'email': profile[3],
            'segment': profile[4],
            'ltv': profile[5],
            'risk_score': profile[6]
        }
    return None

def generate_ai_message(first_name, age, segment, ltv, risk_score):
    """Generate personalized message using Gemini API"""
    prompt = f"""You are a professional banking relationship manager at GlobalFin, a premium financial institution.

Customer Profile:
- Name: {first_name}
- Age: {age} years
- Segment: {segment}
- Estimated Lifetime Value: €{ltv}
- Risk Score: {risk_score}/100 (Low Risk)

Write a personalized, professional welcome message for this customer. The message should:
1. Be warm but professional (corporate banking tone)
2. Reference their specific segment and financial profile
3. Suggest 2-3 relevant products/services based on their age and segment
4. Be between 80-120 words
5. End with a clear call-to-action

Do not use generic phrases. Make it feel personally crafted for {first_name}."""

    try:
        response = requests.post(
            f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
            headers={'Content-Type': 'application/json'},
            json={
                'contents': [{
                    'parts': [{'text': prompt}]
                }],
                'generationConfig': {
                    'temperature': 0.7,
                    'maxOutputTokens': 250,
                    'topP': 0.8,
                    'topK': 40
                }
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            message = data['candidates'][0]['content']['parts'][0]['text']
            return message, True
        else:
            print(f"   ⚠ API Error: {response.status_code}")
            return generate_fallback_message(first_name, segment), False
            
    except Exception as e:
        print(f"   ⚠ API Request Failed: {str(e)}")
        return generate_fallback_message(first_name, segment), False

def generate_fallback_message(first_name, segment):
    """Fallback message if API fails"""
    messages = {
        'Young Professional': f"Dear {first_name}, welcome to GlobalFin. As a young professional, we've tailored exclusive benefits for you: high-yield savings account with 3.8% APY, mobile-first banking, and investment tools. Start your financial journey with us today.",
        'Mid-Career Wealth Builder': f"Welcome {first_name}! Based on your profile, we recommend our premium wealth management package: personalized investment portfolio, mortgage refinancing options, and family protection plans. Let's build your financial future together.",
        'Senior Wealth Management': f"Hello {first_name}! We value your experience and trust. Enjoy our senior benefits: premium retirement planning, estate management services, and exclusive rates. Your financial security is our priority."
    }
    return messages.get(segment, f"Welcome to GlobalFin, {first_name}!")

def orchestrate_customer_journey(age):
    """Main CJOP orchestration logic"""
    print("=" * 60)
    print("GlobalFin Customer 360 - CJOP Orchestration")
    print("=" * 60)
    print()
    print(f"Orchestrating journey for customer (age: {age})...")
    print()
    
    # Step 1: Retrieve customer profile from CDP
    print("[1/4] Querying CDP for customer profile...")
    profile = get_customer_profile(age)
    
    if not profile:
        print("   ❌ No customer profile found for this age")
        return
    
    print(f"   ✓ Profile found: {profile['first_name']} {profile['last_name']}")
    print(f"      Segment: {profile['segment']}")
    print(f"      LTV: €{profile['ltv']}")
    
    # Step 2: Determine channel and campaign
    print()
    print("[2/4] Determining optimal channel and campaign...")
    channel = "Mobile App" if age < 40 else "Email"
    campaign = f"{profile['segment']} Welcome Journey"
    print(f"   ✓ Channel: {channel}")
    print(f"   ✓ Campaign: {campaign}")
    
    # Step 3: Generate personalized message with AI
    print()
    print("[3/4] Generating personalized message with Gemini AI...")
    message, api_success = generate_ai_message(
        profile['first_name'],
        age,
        profile['segment'],
        profile['ltv'],
        profile['risk_score']
    )
    
    if api_success:
        print("   ✓ AI-generated message created")
    else:
        print("   ⚠ Using fallback message (API unavailable)")
    
    # Step 4: Log interaction to CJOP database
    print()
    print("[4/4] Logging interaction to CJOP database...")
    conn = sqlite3.connect('cjop.db')
    c = conn.cursor()
    c.execute('''INSERT INTO interactions 
                 (customer_id, customer_age, customer_segment, channel, 
                  campaign_name, personalized_message, ai_model_used)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (profile['customer_id'], age, profile['segment'], channel,
               campaign, message, 'Gemini Pro' if api_success else 'Fallback'))
    conn.commit()
    conn.close()
    print("   ✓ Interaction logged")
    
    print()
    print("=" * 60)
    print("✅ Customer journey orchestration complete")
    print("=" * 60)
    print()
    print("Personalized Message:")
    print("-" * 60)
    print(message)
    print("-" * 60)
    print()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python cjop.py <customer_age>")
        print("Example: python cjop.py 35")
        sys.exit(1)
    
    try:
        age = int(sys.argv[1])
        if age < 18 or age > 100:
            print("Age must be between 18 and 100")
            sys.exit(1)
        orchestrate_customer_journey(age)
    except ValueError:
        print("Invalid age. Please provide a number.")
        sys.exit(1)
