import pandas as pd
import os
import csv
from colorama import Fore, Style

def delete_incorrect_products(input_file_path, output_file_path):
    # Load the CSV file into a DataFrame, treating all columns as strings
    df = pd.read_csv(input_file_path, encoding='utf-8-sig', dtype=str, low_memory=False)

    # Create the Logs folder if it doesn't exist
    os.makedirs('Logs', exist_ok=True)

    # Open the log file to write the barcodes of deleted items
    log_file_path = 'Logs/Items_to_be_deleted.txt'
    with open(log_file_path, 'w', encoding='utf-8-sig') as log_file:
        log_file.write('"TEMP / DELETED" ITEMS:\n\n')

        # Identify rows where EN_FULL_DESCRIPTION is 'TEMP ITEMS TO BE DELETED'
        temp_rows = df[df['EN_FULL_DESCRIPTION'] == 'TEMP ITEMS TO BE DELETED']

        # Write the barcode of each deleted row into the log file
        for barcode in temp_rows['BARCODE']:
            log_file.write(f'"{barcode}"\n')

        # Count of the rows to be deleted
        total_deleted = temp_rows.shape[0]

        # Remove rows where EN_FULL_DESCRIPTION is 'TEMP ITEMS TO BE DELETED'
        df_cleaned = df[df['EN_FULL_DESCRIPTION'] != 'TEMP ITEMS TO BE DELETED']

    # Save the cleaned DataFrame to a new TXT file
    df_cleaned.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    # Print the total number of deleted products and save it to the log
    print(f"Total deleted products: {total_deleted}")
    #with open(log_file_path, 'a', encoding='utf-8-sig') as log_file:
     #   log_file.write(f"\nTotal deleted products: {total_deleted}")

    print(f"{Fore.RED}Removed incorrect products. Updated data saved to: {output_file_path}{Style.RESET_ALL}")

    print(f"Details of deleted items logged to: {log_file_path}")
