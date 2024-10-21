import pandas as pd
import os
import sys
from colorama import Fore, Style, init

init(autoreset=True)

EXCEL_PATH = 'data/FARM_ITEM_MASTE_FILE.xlsx'
CSV_PATH = 'data/ml_transactions.csv'
OUTPUT_DIR = 'out'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'missing_products.txt')

def find_missing_products():
    print(Fore.CYAN + "Starting comparison...")
    
    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    try:
        # Read the Excel and CSV files, treating barcodes and product IDs as strings
        df_excel = pd.read_excel(EXCEL_PATH, dtype={'BARCODE': str})
        df_csv = pd.read_csv(CSV_PATH, dtype={'product_id': str})
    except Exception as e:
        print(Fore.RED + f"Error reading files: {e}")
        sys.exit(1)
    
    # Get sets of barcodes and product IDs
    excel_barcodes = set(df_excel['BARCODE'].dropna().str.strip())
    csv_product_ids = set(df_csv['product_id'].dropna().str.strip())
    
    # Find missing product IDs in the CSV that are not in the Excel file
    missing = csv_product_ids - excel_barcodes
    
    if missing:
        # Write missing product IDs to the output file, quoted and with the column name
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write('"product_id"\n')  # Write the column header
            for pid in sorted(missing):
                f.write(f'"{pid}"\n')  # Write each product ID, quoted
        print(Fore.YELLOW + f"Missing products found: {len(missing)}. See {OUTPUT_FILE}")
    else:
        print(Fore.GREEN + "No missing products found.")
    
    print(Fore.CYAN + "Comparison completed.")

if __name__ == "__main__":
    find_missing_products()
