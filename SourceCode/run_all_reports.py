# -*- coding: utf-8 -*-
"""
Main runner script to generate and upload both reports.

INSTRUCTIONS:
1. Make sure this file (`run_all_reports.py`) is in the same directory as:
   - BaocaoTonghop_formatted.py
   - BaocaoXepHangASM_formatted.py
   - config.py
2. Ensure you have updated the two report scripts as per the instructions.
3. Run this script from your terminal: python run_all_reports.py
"""

import time
import sys

# Try to import the necessary report modules
try:
    import BaocaoTonghop_formatted as summary_reporter
    import BaocaoXepHangASM_formatted as ranking_reporter
except ImportError as e:
    print(f"ERROR: Could not import a report module. {e}")
    print("Please ensure all script files are in the same directory.")
    sys.exit(1)  # Exit if modules can't be found


def run_reports():
    """
    Main function to orchestrate the generation of both reports.
    """
    start_time = time.time()

    print("==============================================")
    print("== STARTING AUTOMATED REPORT GENERATION ==")
    print("==============================================")

    try:
        # --- 1. Run Summary Report ---
        print("\n[1/2] Generating Summary Report...")
        summary_reporter.generate_summary_report()
        print("✅ [1/2] Summary Report completed.")

        print("\n" + "-" * 45 + "\n")

        # --- 2. Run Ranking Report ---
        print("[2/2] Generating ASM Ranking Report...")
        ranking_reporter.generate_ranking_report()
        print("✅ [2/2] ASM Ranking Report completed.")

    except AttributeError as e:
        print(f"\nERROR: {e}")
        print(
            "It seems the 'generate_summary_report()' or 'generate_ranking_report()' function is missing."
        )
        print(
            "Please ensure you have fully replaced the code in the two report scripts as instructed."
        )
    except Exception as e:
        print(f"\n[CRITICAL ERROR] An unexpected error occurred: {e}")
    finally:
        end_time = time.time()
        print("\n==============================================")
        print("== REPORT GENERATION PROCESS FINISHED ==")
        print(f"== Total execution time: {end_time - start_time:.2f} seconds ==")
        print("==============================================")


if __name__ == "__main__":
    run_reports()
