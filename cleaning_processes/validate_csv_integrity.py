import os
import pandas as pd
import csv
from colorama import Fore, Style, init

# Initialize colorama for colored output
init(autoreset=True)

# Create the Logs folder if it doesn't exist
os.makedirs('Logs', exist_ok=True)

# Function to check and validate the integrity of the CSV file
def check_validate_integrity(input_file_path, required_columns):
    print(f"{Fore.YELLOW}Starting validation for file: {input_file_path}{Style.RESET_ALL}")
    
    # Step 1: Load the CSV with UTF-8-sig encoding
    try:
        df = pd.read_csv(input_file_path, encoding='utf-8-sig', dtype=str)  # Load all columns as strings to avoid dtype issues
        print(f"{Fore.GREEN}File loaded successfully.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}Error loading file: {e}{Style.RESET_ALL}")
        return

    # Step 2: Check for missing columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"{Fore.RED}Missing columns: {', '.join(missing_columns)}{Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}All required columns are present.{Style.RESET_ALL}")

    # Step 3: Check for empty columns
    empty_columns = [col for col in df.columns if df[col].isnull().all()]
    if empty_columns:
        print(f"{Fore.RED}Empty columns (no data): {', '.join(empty_columns)}{Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}No completely empty columns.{Style.RESET_ALL}")

    # Step 4: Check for rows with missing or empty values and log the missing columns for each row using .loc[]
    rows_with_missing_values = df[df.isnull().any(axis=1)].copy()  # Copy to avoid the warning
    if not rows_with_missing_values.empty:
        print(f"{Fore.RED}Rows with missing values: {rows_with_missing_values.shape[0]}{Style.RESET_ALL}")
        rows_with_missing_values.loc[:, 'Missing Columns'] = rows_with_missing_values.apply(
            lambda row: ', '.join([col for col in required_columns if pd.isnull(row[col])]), axis=1)
    else:
        print(f"{Fore.GREEN}No rows with missing values.{Style.RESET_ALL}")

    # Step 5: Check for misplaced commas only in specific columns
    columns_to_check_for_commas = [
        "barcode", "category_level1", 
        "category_level2", "category_level3", "category_level4"
    ]

    problematic_rows = df[df.apply(
        lambda row: any(',' in str(row[col]) for col in columns_to_check_for_commas if col in df.columns),
        axis=1
    )]

    if not problematic_rows.empty:
        print(f"{Fore.RED}Rows with misplaced commas: {problematic_rows.shape[0]}{Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}No rows with misplaced commas found.{Style.RESET_ALL}")

    # Step 6: Log issues to 'Logs/incorrect_data.csv'
    log_issues_to_csv(missing_columns, empty_columns, rows_with_missing_values, problematic_rows)

    # Step 7: Return validation results summary
    if missing_columns or not rows_with_missing_values.empty or not problematic_rows.empty:
        print(f"{Fore.RED}Validation completed with issues found. See Logs/incorrect_data.csv for details.{Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}Validation completed successfully with no issues.{Style.RESET_ALL}")

# Function to log issues to 'Logs/incorrect_data.csv' with issue description and missing columns
def log_issues_to_csv(missing_columns, empty_columns, rows_with_missing_values, problematic_rows):
    log_file_path = 'Logs/incorrect_data.csv'

    with open(log_file_path, mode='w', newline='', encoding='utf-8-sig') as log_file:
        writer = csv.writer(log_file)
        
        # Log missing columns
        if missing_columns:
            writer.writerow(["Issue Description", "Missing Columns"])
            for col in missing_columns:
                writer.writerow([f"Missing required column: {col}", col])
            writer.writerow([])  # Blank line for separation

        # Log empty columns
        if empty_columns:
            writer.writerow(["Issue Description", "Empty Columns (No Data)"])
            for col in empty_columns:
                writer.writerow([f"Empty column: {col}", col])
            writer.writerow([])

        # Log rows with missing values and which columns are missing
        if not rows_with_missing_values.empty:
            writer.writerow(["Issue Description", "Row Data", "Missing Columns"])
            rows_with_missing_values.to_csv(log_file, columns=list(rows_with_missing_values.columns) + ['Missing Columns'], index=False, quoting=csv.QUOTE_NONNUMERIC, header=False)
            writer.writerow([])

        # Log rows with misplaced commas
        if not problematic_rows.empty:
            writer.writerow(["Issue Description", "Row Data with Misplaced Commas"])
            problematic_rows.to_csv(log_file, index=False, quoting=csv.QUOTE_NONNUMERIC, header=False)
            writer.writerow([])

    print(f"{Fore.GREEN}Issues logged to: {log_file_path}{Style.RESET_ALL}")

# Main method to run the validation process
def main():
    input_file_path = 'output/Cleaned_ml_items_master.csv'  # Updated to correct path
    
    # Define the required columns for validation
    required_columns = [
        "item_number", "en_full_description", "ar_full_description", 
        "barcode", "UOM", "packing", "en_short_desc", "ar_short_desc", 
        "brand", "category_level1", "category_level2", "category_level3", "category_level4"
    ]

    check_validate_integrity(input_file_path, required_columns)

# Entry point for the script
if __name__ == "__main__":
    main()
