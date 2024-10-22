import pandas as pd
import csv
import unidecode
import arabic_reshaper
from bidi.algorithm import get_display
from colorama import Fore, Style, init
from tqdm import tqdm

# Initialize colorama
init(autoreset=True)

# Method to count duplicate barcodes and print correct stats
def count_duplicates_before_cleaning(df):
    # Identify duplicated barcodes (excluding the first occurrence)
    duplicated_barcodes = df[df.duplicated(subset=['barcode'], keep=False)]
    
    # Identify how many unique barcodes are duplicated
    unique_duplicated_barcodes_count = duplicated_barcodes['barcode'].nunique()
    
    # Count how many total records have duplicated barcodes (including first occurrences)
    total_duplicated_records = duplicated_barcodes.shape[0]
    
    # Print the correct counts
    print(f"{Fore.YELLOW}Number of unique duplicated barcodes: {unique_duplicated_barcodes_count}")
    print(f"{Fore.YELLOW}Total records involved in duplication: {total_duplicated_records}")
    
    return unique_duplicated_barcodes_count, total_duplicated_records

# Method to remove duplicates by keeping the first occurrence of each unique barcode
def remove_duplicate_barcodes(df):
    df_cleaned = df.drop_duplicates(subset=['barcode'], keep='first')
    return df_cleaned

# Method to print the results after cleaning (number of unique barcodes and total records)
def print_uniqueness_after_cleaning(df_cleaned):
    unique_barcodes_count = df_cleaned['barcode'].nunique()
    total_records_after_cleaning = df_cleaned.shape[0]
    print(f"{Fore.GREEN}Number of unique barcodes after processing: {unique_barcodes_count}")
    print(f"{Fore.CYAN}Total records after cleaning: {total_records_after_cleaning}")

# Method to remove rows with missing or empty barcodes
def remove_null_empty_barcodes(df):
    # Count missing and empty barcodes
    missing_barcodes_count = df['barcode'].isnull().sum()
    empty_barcodes_count = df['barcode'].eq('').sum()  # Count empty strings as well
    print(f"{Fore.RED}Number of missing (null) barcodes: {missing_barcodes_count}")
    print(f"{Fore.RED}Number of empty barcodes: {empty_barcodes_count}")

    # Remove rows where 'barcode' is null or empty
    df_cleaned = df.dropna(subset=['barcode'])
    df_cleaned = df_cleaned[df_cleaned['barcode'].str.strip() != '']

    return df_cleaned

# Method to reshape and bidirectional correct Arabic text
def reshape_arabic_text(text):
    reshaped_text = arabic_reshaper.reshape(text)
    bidi_text = get_display(reshaped_text)
    return bidi_text

# Method to save CSV with quotes for non-numeric fields
def save_csv_with_quotes(df_cleaned, output_file_path):
    print(f"{Fore.BLUE}Saving data to CSV with progress:")
    with tqdm(total=len(df_cleaned), desc="Saving CSV", bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} {elapsed} elapsed") as pbar:
        df_cleaned.to_csv(output_file_path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8-sig')
        pbar.update(len(df_cleaned))
    
    print(f"{Fore.GREEN}Cleaned CSV data saved to: {output_file_path}")

# Method to display a pretty summary of the data using colorama and unicode decoding
def display_pretty_summary(df_cleaned):
    print(f"\n{Style.BRIGHT}{Fore.MAGENTA}Displaying a sample summary:")
    
    for index, row in df_cleaned.head(5).iterrows():
        item_number = row['item_number']
        en_desc = unidecode.unidecode(row['en_full_description'])
        ar_desc = reshape_arabic_text(row['ar_full_description'])
        barcode = row['barcode']
        
        # Print in different colors
        print(f"{Fore.CYAN}item_number: {Fore.YELLOW}{item_number}")
        print(f"{Fore.CYAN}en_full_description: {Fore.YELLOW}{en_desc}")
        print(f"{Fore.CYAN}ar_full_description: {Fore.YELLOW}{ar_desc}")
        print(f"{Fore.CYAN}barcode: {Fore.YELLOW}{barcode}")
        print(f"{Fore.MAGENTA}{'-' * 40}")

# Main method for cleaning barcodes
def clean_barcode(input_file_path, output_file_path):
    print(f"{Fore.YELLOW}Starting the barcode cleaning process...{Style.RESET_ALL}")

    # Step 1: Load the data from the provided input file, forcing 'barcode' to be a string
    df = pd.read_csv(input_file_path, dtype={'barcode': str})

    # Step 2: Remove rows with missing or empty barcodes
    df = remove_null_empty_barcodes(df)

    # Step 3: Count duplicates before cleaning and print detailed stats
    #unique_duplicated_barcodes_count, total_duplicated_records = count_duplicates_before_cleaning(df)

    # Step 4: Clean the data by removing duplicate barcodes (keep first occurrence)
    df_cleaned = remove_duplicate_barcodes(df)

    # Step 5: Print the uniqueness and total records after cleaning
    print_uniqueness_after_cleaning(df_cleaned)

    # Step 6: Display a pretty summary of the cleaned data
    #display_pretty_summary(df_cleaned)

    # Step 7: Save the cleaned data to CSV with quotes around non-numeric fields
    save_csv_with_quotes(df_cleaned, output_file_path)

    print(f"{Fore.YELLOW}barcode cleaning process completed.{Style.RESET_ALL}")
