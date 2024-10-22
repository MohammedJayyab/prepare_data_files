import pandas as pd
from tqdm import tqdm
from colorama import Fore, Style, init
import csv
import time
import os

# Initialize colorama
init(autoreset=True)

def update_transactions(transactions_file,cleaned_with_max_barcode_file, items_file, output_updated_transactions_file_path):
    start_time = time.time()

    # Load the transactions and items data
    print(f"{Fore.YELLOW}Loading transactions and items files...\n")
    transactions_df = pd.read_csv(transactions_file, dtype=str, low_memory=False)
    items_df = pd.read_csv(cleaned_with_max_barcode_file, dtype=str, low_memory=False)

    
    # Step 1: Remove transactions that do not have a corresponding item_barcode in cleaned_with_max_barcode_file
    initial_transaction_count = transactions_df.shape[0]
    valid_barcodes = set(items_df['barcode'])

    # This line was incorrect because it used ~ to filter out matching barcodes, instead it should keep matching barcodes
    transactions_df = transactions_df[transactions_df['item_barcode'].isin(valid_barcodes)]
    removed_transaction_count = initial_transaction_count - transactions_df.shape[0]

    print(f"{Fore.RED}Total removed transactions that do not exist in items file: {removed_transaction_count}\n")



    # Step 2: Update the item_barcode in the transactions file based on the barcode from the items file
    # Prepare a log for not found items
    not_found_items = []

    # Create a dictionary for faster lookups from the items file
    barcode_to_max_barcode = pd.Series(items_df.MAX_barcode.values, index=items_df.barcode).to_dict()
    print(f"{Fore.YELLOW}Updating transactions file with MAX_barcode...\n")
    total_affected = 0

    for index, row in tqdm(transactions_df.iterrows(), total=transactions_df.shape[0], desc="Processing transactions", ncols=100):
        item_barcode = row['item_barcode']
        # Check if item_barcode exists in barcode_to_max_barcode dictionary
        if item_barcode in barcode_to_max_barcode:
            transactions_df.at[index, 'item_barcode'] = barcode_to_max_barcode[item_barcode]
            total_affected += 1
        # If the item_barcode is not found in cleaned_with_max_barcode_file, log it
        elif item_barcode not in items_df['barcode'].values:
            not_found_items.append(item_barcode)

    # Step 3: Write not found items to a log
    if not_found_items:
        print(f"{Fore.RED}Writing not found items to logs/not_found_items.csv...\n")
        with open('Logs/not_found_items.csv', 'w', newline='', encoding='utf-8-sig') as log_file:
            writer = csv.writer(log_file)
            writer.writerow(['item_barcode'])  # Column header
            for item in set(not_found_items):  # Using set to remove duplicates before writing to file
                writer.writerow([item])

    # Step 4: Save the updated transactions file
    print(f"{Fore.GREEN}Saving the updated transactions file...\n")
    transactions_df.to_csv(output_updated_transactions_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"{Fore.CYAN}Total affected records in transactions: {total_affected}\n")

    # Deduplicate cleaned_with_max_barcode_file keeping the max barcode when equal to MAX_barcode
    print(f"{Fore.YELLOW}Deduplicating items file...\n")
    deduplicated_items = items_df.sort_values(by='barcode', ascending=False).drop_duplicates(subset='MAX_barcode', keep='first')
    deduplicated_items['barcode'] = deduplicated_items['MAX_barcode'].astype(str)


    # Find the deleted records
    deleted_records = items_df[~items_df.index.isin(deduplicated_items.index)]

    # Log the deleted records
    if not deleted_records.empty:
        print(f"{Fore.RED}Writing deleted records to logs/deleted_items.csv...\n")
        deleted_records.to_csv('logs/deleted_items.csv', index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    # Save the deduplicated items file
    print(f"{Fore.GREEN} Saving deduplicated items/products file to {items_file}...\n")
    deduplicated_items.to_csv(items_file, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8-sig')

    # Calculate the time taken
    end_time = time.time()
    total_time = end_time - start_time
    print(f"{Fore.GREEN}Process completed in {total_time:.2f} seconds.")

# Example usage:
# update_transactions('data/transactions_file.csv', 'data/items_file.csv', 'output/updated_transactions.csv')
