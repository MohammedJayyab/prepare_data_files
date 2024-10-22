import pandas as pd
import csv
from colorama import Fore, Style, init  # For colored output
import os

# Initialize colorama for colored output
init(autoreset=True)

def correct_category_levels(input_file_path, output_file_path, log_file_path):
    print(f"{Fore.YELLOW}Correcting category levels...{Style.RESET_ALL}")
    # Load the data into a DataFrame
    df = pd.read_csv(input_file_path, dtype=str, low_memory=False)

    
    # Create logs folder if it doesn't exist
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    # Track rows with missing or corrected categories
    missing_or_corrected_rows = []

    # Check each row and fix category levels
    for index, row in df.iterrows():
        corrected = False

        # Check for missing or "NULL" values in category_level1
        if pd.isnull(row['category_level1']) or row['category_level1'].strip() == '' or row['category_level1'].strip().upper() == 'NULL':
            print(f"{Fore.RED}Warning: category_level1 is missing for record {index}.")
            corrected = True

        # Check for missing or "NULL" values in category_level2 and fix
        if pd.isnull(row['category_level2']) or row['category_level2'].strip() == '' or row['category_level2'].strip().upper() == 'NULL':
            row['category_level2'] = row['category_level1'].strip()  # Take trimmed value from category_level1
            corrected = True

        # Check for missing or "NULL" values in category_level3 and fix
        if pd.isnull(row['category_level3']) or row['category_level3'].strip() == '' or row['category_level3'].strip().upper() == 'NULL':
            row['category_level3'] = row['category_level2'].strip()  # Take trimmed value from category_level2
            corrected = True

        # Check for missing or "NULL" values in category_level4 and fix
        if pd.isnull(row['category_level4']) or row['category_level4'].strip() == '' or row['category_level4'].strip().upper() == 'NULL':
            row['category_level4'] = row['category_level3'].strip()  # Take trimmed value from category_level3
            corrected = True

        # Create new column 'category_name' based on the trimmed value of category_level4
        df.at[index, 'category_name'] = row['category_level4'].strip()

        # Log missing or corrected rows
        if corrected:
            missing_or_corrected_rows.append(row)

    # Save the corrected DataFrame to the output file
    df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8-sig')

    # Write missing or corrected rows to a log file
    if missing_or_corrected_rows:
        print(f"{Fore.CYAN}Writing missing or corrected records to log file: {log_file_path}{Style.RESET_ALL}")
        pd.DataFrame(missing_or_corrected_rows).to_csv(log_file_path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8-sig')

    print(f"{Fore.GREEN}Category level correction process completed. Corrected data saved to: {output_file_path}{Style.RESET_ALL}")

# Example usage
# correct_category_levels('input_file.csv', 'output_file.csv', 'logs/missing_or_corrected_categories.csv')
