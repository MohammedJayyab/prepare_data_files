
import pandas as pd
import sys
from colorama import Fore, Style, init
from tqdm import tqdm
import os

init(autoreset=True)

FILE_PATH = 'data/FARM_ITEM_MASTE_FILE.xlsx'
LOG_PATH = os.path.join('log', 'errors.txt')

def log_error(message):
    print(Fore.RED + message)
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(message + '\n')

def log_warning(message):
    print(Fore.YELLOW + message)
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(message + '\n')

def check_unique(df, column, display_name):
    duplicates = df[df.duplicated(column, keep=False)]
    if not duplicates.empty:
        for _, row in tqdm(duplicates.iterrows(), total=duplicates.shape[0], desc=f'Checking duplicates in {display_name}'):
            line = row.name + 2
            context = f"Line {line}: {column}='{row[column]}' | ITEM_NUMBER={row['ITEM_NUMBER']} | BRAND={row['BRAND']}"
            log_error(f"Duplicate {display_name}: {context}")

def check_null_empty(df, column, display_name):
    condition = df[column].isnull() | df[column].astype(str).str.strip().eq('')
    filtered = df[condition]
    for idx in tqdm(filtered.index, desc=f'Checking null/empty in {display_name}'):
        line = idx + 2
        log_error(f"{display_name} is null/empty: Line {line}, ITEM_NUMBER={df.at[idx, 'ITEM_NUMBER']}")

def check_whitespace(df, column, display_name):
    condition = df[column].astype(str).str.startswith(' ') | df[column].astype(str).str.endswith(' ')
    filtered = df[condition]
    for idx in tqdm(filtered.index, desc=f'Checking whitespace in {display_name}'):
        line = idx + 2
        log_warning(f"{display_name} has leading/trailing whitespace: Line {line}, ITEM_NUMBER={df.at[idx, 'ITEM_NUMBER']}")

def check_fields(df):
    check_unique(df, 'AR_FULL_DESCRIPTION', 'AR_FULL_DESCRIPTION')
    check_null_empty(df, 'AR_FULL_DESCRIPTION', 'AR_FULL_DESCRIPTION')
    
    check_unique(df, 'BARCODE', 'BARCODE')
    check_null_empty(df, 'BARCODE', 'BARCODE')
    #check_whitespace(df, 'BARCODE', 'BARCODE')

    check_unique(df, 'EN_FULL_DESCRIPTION', 'EN_FULL_DESCRIPTION')
    check_null_empty(df, 'EN_FULL_DESCRIPTION', 'EN_FULL_DESCRIPTION')
    
    #check_whitespace(df, 'EN_FULL_DESCRIPTION', 'EN_FULL_DESCRIPTION')

    check_unique(df, 'EN_SHORT_DESC', 'EN_SHORT_DESC')
    check_null_empty(df, 'EN_SHORT_DESC', 'EN_SHORT_DESC')
    #check_whitespace(df, 'EN_SHORT_DESC', 'EN_SHORT_DESC')

    check_unique(df, 'AR_SHORT_DESC', 'AR_SHORT_DESC')
    check_null_empty(df, 'AR_SHORT_DESC', 'AR_SHORT_DESC')
    #check_whitespace(df, 'AR_SHORT_DESC', 'AR_SHORT_DESC')

    fields = ['BRAND', 'CATEGORY_LEVEL1', 'CATEGORY_LEVEL2', 'CATEGORY_LEVEL3', 'CATEGORY_LEVEL4']
    for field in fields:
        condition = df[field].isnull() | df[field].astype(str).str.strip().eq('')
        filtered = df[condition]
        for idx in tqdm(filtered.index, desc=f'Checking null/empty in {field}'):
            line = idx + 2
            log_error(f"{field} is null/empty: Line {line}, ITEM_NUMBER={df.at[idx, 'ITEM_NUMBER']}")

def main():
    print(Fore.CYAN + "Starting validation...")
    if not os.path.exists('log'):
        os.makedirs('log')
    open(LOG_PATH, 'w').close()
    try:
        df = pd.read_excel(FILE_PATH, dtype={'BARCODE': str})
    except Exception as e:
        log_error(f"Failed to read Excel file: {e}")
        sys.exit(1)
    check_fields(df)
    print(Fore.GREEN + "Validation completed. Check log/errors.txt for details.")

if __name__ == "__main__":
    main()
