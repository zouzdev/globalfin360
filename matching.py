"""
GlobalFin Customer 360 Platform - MDM Matching Engine
Simulates duplicate detection and identity resolution
"""

import sqlite3
from difflib import SequenceMatcher

def similarity(a, b):
    """Calculate string similarity (0-1)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def match_duplicates():
    print("=" * 60)
    print("GlobalFin Customer 360 - MDM Matching Engine")
    print("=" * 60)
    print()
    print("Running identity resolution and duplicate detection...")
    print()
    
    conn = sqlite3.connect('mdm.db')
    c = conn.cursor()
    
    # Find exact email duplicates
    print("[1/3] Checking for exact email matches...")
    c.execute('''SELECT email, COUNT(*) as count FROM golden_records 
                 GROUP BY email HAVING count > 1''')
    email_duplicates = c.fetchall()
    
    if email_duplicates:
        print(f"   ⚠ Found {len(email_duplicates)} duplicate emails:")
        for dup in email_duplicates[:5]:  # Show first 5
            print(f"      • {dup[0]}: {dup[1]} records")
    else:
        print("   ✓ No exact email duplicates found")
    
    # Find fuzzy name matches
    print()
    print("[2/3] Checking for fuzzy name matches...")
    c.execute("SELECT golden_id, first_name, last_name, email FROM golden_records")
    all_records = c.fetchall()
    
    potential_matches = []
    checked_pairs = set()
    
    for i, record1 in enumerate(all_records):
        for record2 in all_records[i+1:]:
            pair_key = tuple(sorted([record1[0], record2[0]]))
            if pair_key in checked_pairs:
                continue
            checked_pairs.add(pair_key)
            
            name_sim = (similarity(record1[1], record2[1]) + similarity(record1[2], record2[2])) / 2
            
            if name_sim > 0.8 and record1[0] != record2[0]:
                potential_matches.append({
                    'id1': record1[0],
                    'id2': record2[0],
                    'name1': f"{record1[1]} {record1[2]}",
                    'name2': f"{record2[1]} {record2[2]}",
                    'similarity': name_sim
                })
    
    if potential_matches:
        print(f"   ⚠ Found {len(potential_matches)} potential fuzzy matches:")
        for match in potential_matches[:5]:
            print(f"      • {match['name1']} ≈ {match['name2']} ({match['similarity']:.2%} match)")
            
            # Log to match_history
            c.execute('''INSERT INTO match_history (golden_id, match_type, match_score, matched_records)
                         VALUES (?, ?, ?, ?)''',
                      (match['id1'], 'fuzzy_name', match['similarity'], 
                       f"{match['id1']},{match['id2']}"))
    else:
        print("   ✓ No fuzzy name matches found")
    
    # Data quality analysis
    print()
    print("[3/3] Analyzing data quality...")
    c.execute("SELECT AVG(data_quality_score), MIN(data_quality_score), MAX(data_quality_score) FROM golden_records")
    avg_score, min_score, max_score = c.fetchone()
    
    c.execute("SELECT COUNT(*) FROM golden_records WHERE data_quality_score < 80")
    low_quality_count = c.fetchone()[0]
    
    conn.commit()
    conn.close()
    
    print(f"   • Average Quality Score: {avg_score:.1f}/100")
    print(f"   • Quality Range: {min_score}-{max_score}")
    print(f"   • Low Quality Records: {low_quality_count}")
    
    print()
    print("=" * 60)
    print("✅ Matching analysis complete")
    print("=" * 60)
    print()

if __name__ == "__main__":
    match_duplicates()
