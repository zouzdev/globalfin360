"""
GlobalFin Customer 360 Platform - Complete Demo Runner
Executes all pipeline steps in sequence
"""

import subprocess
import sys
import time

def run_script(script_name, args=[]):
    """Run a Python script and capture output"""
    cmd = [sys.executable, script_name] + args
    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode == 0

def main():
    print("=" * 70)
    print("   GlobalFin Customer 360 Platform - Complete Pipeline Demo")
    print("=" * 70)
    print()
    
    scripts = [
        ("setup_databases.py", [], "Database Setup"),
        ("activate.py", ["50"], "Data Activation (50 customers)"),
        ("transformation.py", [], "Data Transformation"),
        ("matching.py", [], "MDM Matching"),
        ("safecdpdata.py", [], "CDP Synchronization"),
        ("cjop.py", ["35"], "CJOP Orchestration (age 35)")
    ]
    
    for i, (script, args, description) in enumerate(scripts, 1):
        print(f"\n{'='*70}")
        print(f"Step {i}/{len(scripts)}: {description}")
        print(f"{'='*70}\n")
        
        success = run_script(script, args)
        
        if not success:
            print(f"\n❌ Error in {script}. Stopping pipeline.")
            sys.exit(1)
        
        if i < len(scripts):
            print("\nPress Enter to continue to next step...")
            input()
    
    print("\n" + "="*70)
    print("   ✅ Complete pipeline execution finished successfully!")
    print("="*70)
    print()
    print("All databases populated. You can now:")
    print("  • Run cjop.py <age> to test different customer ages")
    print("  • Inspect databases with SQLite browser")
    print("  • Use the HTML frontend for interactive demo")
    print()

if __name__ == "__main__":
    main()
