import pandas as pd
import csv
from colorama import Fore, Style, init
import re  # Import regex module to handle multi-space replacements

# Initialize colorama for colored output
init(autoreset=True)

# Function to trim leading/trailing spaces, remove commas, semicolons, newlines, and ensure no multi-spaces
def trim_spaces_commas(input_file_path, output_file_path):
    print(f"{Fore.BLUE}Starting the trimming process for specified columns...{Style.RESET_ALL}")

    # Step 1: Load the data from the provided input file
    try:
        df = pd.read_excel(input_file_path)
        print(f"{Fore.CYAN}Data successfully loaded from: {input_file_path}{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}Error loading file: {e}{Style.RESET_ALL}")
        return

    # Step 2: Define the columns for which leading and trailing spaces should be trimmed
    columns_to_trim = [
        "EN_FULL_DESCRIPTION", "AR_FULL_DESCRIPTION", "BARCODE", "UOM", 
        "PACKING", "EN_SHORT_DESC", "AR_SHORT_DESC", "BRAND", "VENDOR_NAME"
        "CATEGORY_LEVEL1", "CATEGORY_LEVEL2", "CATEGORY_LEVEL3", "CATEGORY_LEVEL4"
    ]

    # Step 3: Process each specified column to trim spaces, remove commas, semicolons, newlines, and handle multi-spaces
    for column in columns_to_trim:
        if column in df.columns:
            print(f"{Fore.CYAN}Processing column: {column}...{Style.RESET_ALL}")
            # Clean column: remove non-breaking spaces, commas, semicolons, newlines, trim, and handle multiple spaces
            df[column] = (df[column].astype(str)
                          .str.replace('\u00A0', ' ')  # Replace non-breaking spaces with normal spaces                        
                          .str.replace('\n', ' ').str.replace('\r', ' ')  # Remove newlines
                          .str.replace('`', '').str.replace('´', '')  # Remove commas and semicolons
                          .str.strip()  # Remove leading/trailing spaces
                          .str.replace(r'\s+', ' ', regex=True))  # Replace multiple spaces with a single space

    # Step 4: Save the cleaned DataFrame to the output CSV file
    try:
        print(f"{Fore.CYAN}Saving the cleaned data to: {output_file_path}{Style.RESET_ALL}")
        df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
        print(f"{Fore.GREEN}Trimming is finished. Cleaned data saved to: {output_file_path}{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}Error saving file: {e}{Style.RESET_ALL}")
