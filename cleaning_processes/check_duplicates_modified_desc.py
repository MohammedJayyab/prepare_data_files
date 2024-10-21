import pandas as pd
from tqdm import tqdm  # For progress bar
from colorama import Fore, Style, init  # For colored output
import csv  # For proper quoting

# Initialize colorama
init(autoreset=True)

def check_duplicates_modified_desc(df):
    print(f"{Fore.YELLOW}Starting duplication check on MODIFIED_SHORT_DESC...\n")

    # Create a new column for max barcode of duplicates
    df['MAX_BARCODE_FOR_DUPLICATES'] = ''

    # Find duplicated MODIFIED_SHORT_DESC
    duplicated_groups = df[df.duplicated('MODIFIED_SHORT_DESC', keep=False)].groupby('MODIFIED_SHORT_DESC')

    # Count total duplicates
    total_duplicates = len(duplicated_groups)
    print(f"{Fore.CYAN}Total duplicated MODIFIED_SHORT_DESC entries found: {total_duplicates}\n")

    # Progress bar for processing
    for name, group in tqdm(duplicated_groups, desc=f"{Fore.YELLOW}Processing duplicates (MODIFIED_SHORT_DESC)", ncols=100):
        max_barcode = group['BARCODE'].max()  # Find the lexicographically largest barcode in the group
        df.loc[df['MODIFIED_SHORT_DESC'] == name, 'MAX_BARCODE_FOR_DUPLICATES'] = max_barcode  # Assign max barcode to all duplicates

    print(f"{Fore.GREEN}Duplication check for MODIFIED_SHORT_DESC completed.\n")
    return df


def check_duplicates_ar_short_desc(df):
    print(f"{Fore.YELLOW}Starting duplication check on AR_SHORT_DESC...\n")

    # Create a new column for max barcode of AR_SHORT_DESC duplicates
    df['MAX_BARCODE_FOR_DUPLICATED_AR'] = ''

    # Find duplicated AR_SHORT_DESC
    duplicated_ar_groups = df[df.duplicated('AR_SHORT_DESC', keep=False)].groupby('AR_SHORT_DESC')

    # Count total duplicates for AR_SHORT_DESC
    total_ar_duplicates = len(duplicated_ar_groups)
    print(f"{Fore.CYAN}Total duplicated AR_SHORT_DESC entries found: {total_ar_duplicates}\n")

    # Progress bar for processing AR_SHORT_DESC duplicates
    for name, group in tqdm(duplicated_ar_groups, desc=f"{Fore.YELLOW}Processing duplicates (AR_SHORT_DESC)", ncols=100):
        max_barcode = group['BARCODE'].max()  # Find the lexicographically largest barcode in the group
        # Assign max barcode only if MAX_BARCODE_FOR_DUPLICATES column is empty
        df.loc[(df['AR_SHORT_DESC'] == name) & (df['MAX_BARCODE_FOR_DUPLICATES'] == ''), 'MAX_BARCODE_FOR_DUPLICATED_AR'] = max_barcode

    print(f"{Fore.GREEN}Duplication check for AR_SHORT_DESC completed.\n")
    return df


def combine_max_barcode(df):
    print(f"{Fore.YELLOW}Combining results into MAX_BARCODE column...\n")

    # Create the new MAX_BARCODE column based on the priority of the conditions
    df['MAX_BARCODE'] = df['MAX_BARCODE_FOR_DUPLICATES'].where(df['MAX_BARCODE_FOR_DUPLICATES'] != '', df['MAX_BARCODE_FOR_DUPLICATED_AR'])
    df['MAX_BARCODE'] = df['MAX_BARCODE'].where(df['MAX_BARCODE'] != '', df['BARCODE'])  # If both are empty, take BARCODE

    print(f"{Fore.GREEN}MAX_BARCODE column created.\n")
    return df


def run_duplication_checks(input_file_path, output_file_path):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_file_path, encoding='utf-8-sig', dtype=str, low_memory=False)

    # Step 1: Check for duplicates in MODIFIED_SHORT_DESC
    df = check_duplicates_modified_desc(df)

    # Step 2: Check for duplicates in AR_SHORT_DESC
    df = check_duplicates_ar_short_desc(df)

    # Step 3: Combine results into MAX_BARCODE column
    df = combine_max_barcode(df)

    # Save the DataFrame with the new columns to a CSV file
    df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"{Fore.GREEN}Results saved to {output_file_path}")

# Example usage:
# run_duplication_checks('input_file.csv', 'output_file.csv')
