import pandas as pd
from tqdm import tqdm  # For progress bar
from colorama import Fore, Style, init  # For colored output
import csv  # For proper quoting

# Initialize colorama
init(autoreset=True)

def check_duplicates_modified_desc(df):
    print(f"{Fore.YELLOW}Starting duplication check on MODIFIED_SHORT_DESC...\n")

    # Create a new column for max barcode of duplicates
    df['MAX_barcode_FOR_DUPLICATES'] = ''

    # Find duplicated MODIFIED_SHORT_DESC
    duplicated_groups = df[df.duplicated('MODIFIED_SHORT_DESC', keep=False)].groupby('MODIFIED_SHORT_DESC')

    # Count total duplicates
    total_duplicates = len(duplicated_groups)
    print(f"{Fore.CYAN}Total duplicated MODIFIED_SHORT_DESC entries found: {total_duplicates}\n")

    # Progress bar for processing
    for name, group in tqdm(duplicated_groups, desc=f"{Fore.YELLOW}Processing duplicates (MODIFIED_SHORT_DESC)", ncols=100):
        # Get max barcode as a string, removing any decimal points
        max_barcode = str(group['barcode'].max()).split('.')[0]
        df.loc[df['MODIFIED_SHORT_DESC'] == name, 'MAX_barcode_FOR_DUPLICATES'] = max_barcode  # Assign max barcode to all duplicates

    print(f"{Fore.GREEN}Duplication check for MODIFIED_SHORT_DESC completed.\n")
    return df


def check_duplicates_ar_short_desc(df):
    print(f"{Fore.YELLOW}Starting duplication check on ar_short_desc...\n")

    # Create a new column for max barcode of ar_short_desc duplicates
    df['MAX_barcode_FOR_DUPLICATED_AR'] = ''

    # Find duplicated ar_short_desc
    duplicated_ar_groups = df[df.duplicated('ar_short_desc', keep=False)].groupby('ar_short_desc')

    # Count total duplicates for ar_short_desc
    total_ar_duplicates = len(duplicated_ar_groups)
    print(f"{Fore.CYAN}Total duplicated ar_short_desc entries found: {total_ar_duplicates}\n")

    # Progress bar for processing ar_short_desc duplicates
    for name, group in tqdm(duplicated_ar_groups, desc=f"{Fore.YELLOW}Processing duplicates (ar_short_desc)", ncols=100):
        # Get max barcode as a string, removing any decimal points
        max_barcode = str(group['barcode'].max()).split('.')[0]
        df.loc[(df['ar_short_desc'] == name) & (df['MAX_barcode_FOR_DUPLICATES'] == ''), 'MAX_barcode_FOR_DUPLICATED_AR'] = max_barcode

    print(f"{Fore.GREEN}Duplication check for ar_short_desc completed.\n")
    return df


def combine_max_barcode(df):
    print(f"{Fore.YELLOW} Combining results into MAX_barcode column...\n")

    # Create the new MAX_barcode column based on the priority of the conditions
    df['MAX_barcode'] = df['MAX_barcode_FOR_DUPLICATES'].where(df['MAX_barcode_FOR_DUPLICATES'] != '', df['MAX_barcode_FOR_DUPLICATED_AR'])
    df['MAX_barcode'] = df['MAX_barcode'].where(df['MAX_barcode'] != '', df['barcode'])  # If both are empty, take barcode

    # Ensure all barcodes in MAX_barcode are treated as strings, removing decimals if any
    df['MAX_barcode'] = df['MAX_barcode'].apply(lambda x: str(x).split('.')[0])

    print(f"{Fore.GREEN}MAX_barcode column created.\n")
    return df


def run_duplication_checks(input_file_path, output_file_path):
    print(f"{Fore.YELLOW}Starting duplication checks...\n")
    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_file_path, encoding='utf-8-sig', dtype=str, low_memory=False)

    # Step 1: Check for duplicates in MODIFIED_SHORT_DESC
    df = check_duplicates_modified_desc(df)

    # Step 2: Check for duplicates in ar_short_desc
    df = check_duplicates_ar_short_desc(df)

    # Step 3: Combine results into MAX_barcode column
    df = combine_max_barcode(df)

    # Save the DataFrame with the new columns to a CSV file, ensuring all fields are quoted and saved as strings
    df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"{Fore.GREEN}Results saved to {output_file_path}")

# Example usage:
# run_duplication_checks('input_file.csv', 'output_file.csv')
