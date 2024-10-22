import pandas as pd
from colorama import Fore, Style, init
from tabulate import tabulate  # Import tabulate to create a beautiful table
import csv

# Initialize colorama for colored output
init(autoreset=True)

# Function to clean descriptions by combining full and short descriptions, and removing empty rows
def clean_descriptions(input_file_path, output_file_path):
    # Load the data from the input file
    df = pd.read_csv(input_file_path, dtype={'barcode': str})

    # Step 1: Check and remove rows where all description columns are null or empty
    empty_description_mask = (
        df['en_full_description'].isnull() | df['en_full_description'].str.strip().eq('')) & \
        (df['en_short_desc'].isnull() | df['en_short_desc'].str.strip().eq('')) & \
        (df['ar_full_description'].isnull() | df['ar_full_description'].str.strip().eq('')) & \
        (df['ar_short_desc'].isnull() | df['ar_short_desc'].str.strip().eq(''))

    # Count the number of rows with all empty description fields
    empty_count = df[empty_description_mask].shape[0]
    
    # Print the count of such rows in red
    if empty_count > 0:
        print(f"{Fore.RED}Number of rows with all empty description fields: {empty_count}{Style.RESET_ALL}")

    # Remove rows where all description fields are empty
    df = df[~empty_description_mask]

    # Step 2: Create new columns for combined English and Arabic descriptions
    df['EN_COMBINED_DESC'] = df.apply(lambda row: row['en_full_description'] if pd.notnull(row['en_full_description']) and row['en_full_description'].strip() != ''
                                      else row['en_short_desc'] if pd.notnull(row['en_short_desc']) and row['en_short_desc'].strip() != ''
                                      else None, axis=1)
    
    df['AR_COMBINED_DESC'] = df.apply(lambda row: row['ar_full_description'] if pd.notnull(row['ar_full_description']) and row['ar_full_description'].strip() != ''
                                      else row['ar_short_desc'] if pd.notnull(row['ar_short_desc']) and row['ar_short_desc'].strip() != ''
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
    total_duplicated_en_full = df.duplicated(subset=['en_full_description'], keep=False).sum()
    total_duplicated_en_short = df.duplicated(subset=['en_short_desc'], keep=False).sum()
    total_duplicated_both_en = df.duplicated(subset=['en_full_description', 'en_short_desc'], keep=False).sum()
    total_duplicated_ar_full = df.duplicated(subset=['ar_full_description'], keep=False).sum()
    total_duplicated_ar_short = df.duplicated(subset=['ar_short_desc'], keep=False).sum()
    total_duplicated_both_ar = df.duplicated(subset=['ar_full_description', 'ar_short_desc'], keep=False).sum()
    total_duplicated_all = df.duplicated(subset=['en_full_description', 'en_short_desc', 'ar_full_description', 'ar_short_desc'], keep=False).sum()

    # Create a table of statistics
    table = [
        ["en_full_description", total_duplicated_en_full],
        ["en_short_desc", total_duplicated_en_short],
        ["Both en_full_description and en_short_desc", total_duplicated_both_en],
        ["ar_full_description", total_duplicated_ar_full],
        ["ar_short_desc", total_duplicated_ar_short],
        ["Both ar_full_description and ar_short_desc", total_duplicated_both_ar],
        ["All EN and AR Descriptions", total_duplicated_all]
    ]

    # Convert the list into a DataFrame for saving to CSV
    df_table = pd.DataFrame(table, columns=["Description Field(s)", "Total Duplicates"])

    # Save the table as a CSV file
    df_table.to_csv('Output/duplication_products_by_desc.csv', index=False)

    # Display the table
    print(f"\n{Style.BRIGHT}{Fore.GREEN}Duplication Statistics:{Style.RESET_ALL}")
    print(tabulate(table, headers=["Description Field(s)", "Total Duplicates"], tablefmt="fancy_grid"))


