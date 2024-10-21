import pandas as pd
from colorama import Fore, Style, init
from tabulate import tabulate  # Import tabulate to create a beautiful table
import csv

# Initialize colorama for colored output
init(autoreset=True)

# Function to clean descriptions by combining full and short descriptions, and removing empty rows
def clean_descriptions(input_file_path, output_file_path):
    # Load the data from the input file
    df = pd.read_csv(input_file_path, dtype={'BARCODE': str})

    # Step 1: Check and remove rows where all description columns are null or empty
    empty_description_mask = (
        df['EN_FULL_DESCRIPTION'].isnull() | df['EN_FULL_DESCRIPTION'].str.strip().eq('')) & \
        (df['EN_SHORT_DESC'].isnull() | df['EN_SHORT_DESC'].str.strip().eq('')) & \
        (df['AR_FULL_DESCRIPTION'].isnull() | df['AR_FULL_DESCRIPTION'].str.strip().eq('')) & \
        (df['AR_SHORT_DESC'].isnull() | df['AR_SHORT_DESC'].str.strip().eq(''))

    # Count the number of rows with all empty description fields
    empty_count = df[empty_description_mask].shape[0]
    
    # Print the count of such rows in red
    if empty_count > 0:
        print(f"{Fore.RED}Number of rows with all empty description fields: {empty_count}{Style.RESET_ALL}")

    # Remove rows where all description fields are empty
    df = df[~empty_description_mask]

    # Step 2: Create new columns for combined English and Arabic descriptions
    df['EN_COMBINED_DESC'] = df.apply(lambda row: row['EN_FULL_DESCRIPTION'] if pd.notnull(row['EN_FULL_DESCRIPTION']) and row['EN_FULL_DESCRIPTION'].strip() != ''
                                      else row['EN_SHORT_DESC'] if pd.notnull(row['EN_SHORT_DESC']) and row['EN_SHORT_DESC'].strip() != ''
                                      else None, axis=1)
    
    df['AR_COMBINED_DESC'] = df.apply(lambda row: row['AR_FULL_DESCRIPTION'] if pd.notnull(row['AR_FULL_DESCRIPTION']) and row['AR_FULL_DESCRIPTION'].strip() != ''
                                      else row['AR_SHORT_DESC'] if pd.notnull(row['AR_SHORT_DESC']) and row['AR_SHORT_DESC'].strip() != ''
                                      else None, axis=1)

    # Step 3: Create a final combined description column that includes both English and Arabic combined descriptions
    df['FINAL_COMBINED_DESC'] = df['EN_COMBINED_DESC'].fillna('') + ' | ' + df['AR_COMBINED_DESC'].fillna('')

    # Step 4: Show duplication statistics before saving
    display_duplication_statistics(df)

    # Step 5: Save the cleaned and combined descriptions to the output file
    df.to_csv(output_file_path, index=False,quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"Cleaned data saved to: {output_file_path}")

    # Return the number of removed rows
    return empty_count

# Function to display duplication statistics
def display_duplication_statistics(df):
    # Calculate duplication statistics
    total_duplicated_en_full = df.duplicated(subset=['EN_FULL_DESCRIPTION'], keep=False).sum()
    total_duplicated_en_short = df.duplicated(subset=['EN_SHORT_DESC'], keep=False).sum()
    total_duplicated_both_en = df.duplicated(subset=['EN_FULL_DESCRIPTION', 'EN_SHORT_DESC'], keep=False).sum()
    total_duplicated_ar_full = df.duplicated(subset=['AR_FULL_DESCRIPTION'], keep=False).sum()
    total_duplicated_ar_short = df.duplicated(subset=['AR_SHORT_DESC'], keep=False).sum()
    total_duplicated_both_ar = df.duplicated(subset=['AR_FULL_DESCRIPTION', 'AR_SHORT_DESC'], keep=False).sum()
    total_duplicated_all = df.duplicated(subset=['EN_FULL_DESCRIPTION', 'EN_SHORT_DESC', 'AR_FULL_DESCRIPTION', 'AR_SHORT_DESC'], keep=False).sum()

    # Create a table of statistics
    table = [
        ["EN_FULL_DESCRIPTION", total_duplicated_en_full],
        ["EN_SHORT_DESC", total_duplicated_en_short],
        ["Both EN_FULL_DESCRIPTION and EN_SHORT_DESC", total_duplicated_both_en],
        ["AR_FULL_DESCRIPTION", total_duplicated_ar_full],
        ["AR_SHORT_DESC", total_duplicated_ar_short],
        ["Both AR_FULL_DESCRIPTION and AR_SHORT_DESC", total_duplicated_both_ar],
        ["All EN and AR Descriptions", total_duplicated_all]
    ]

    # Convert the list into a DataFrame for saving to CSV
    df_table = pd.DataFrame(table, columns=["Description Field(s)", "Total Duplicates"])

    # Save the table as a CSV file
    df_table.to_csv('Output/duplication_products_by_desc.csv', index=False)

    # Display the table
    print(f"\n{Style.BRIGHT}{Fore.GREEN}Duplication Statistics:{Style.RESET_ALL}")
    print(tabulate(table, headers=["Description Field(s)", "Total Duplicates"], tablefmt="fancy_grid"))


