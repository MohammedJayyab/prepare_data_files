import pandas as pd
from tqdm import tqdm
from colorama import Fore, Style, init
import csv
import time
import os

# Initialize colorama
init(autoreset=True)

def update_transactions(transactions_file, items_file, output_file_path):
    start_time = time.time()

    # Load the transactions and items data
    print(f"{Fore.YELLOW}Loading transactions and items files...\n")
    transactions_df = pd.read_csv(transactions_file, dtype=str, low_memory=False)
    items_df = pd.read_csv(items_file, dtype=str, low_memory=False)

    # Create logs folder if it doesn't exist
    os.makedirs('logs', exist_ok=True)

    # Prepare a log for not found items
    not_found_items = []

    # Create a dictionary for faster lookups from the items file
    barcode_to_max_barcode = pd.Series(items_df.MAX_BARCODE.values, index=items_df.BARCODE).to_dict()

    # Step 1: Remove transactions that do not have a corresponding item_barcode in items_file
    initial_transaction_count = transactions_df.shape[0]
    transactions_df = transactions_df[transactions_df['item_barcode'].isin(items_df['BARCODE'])]
    removed_transaction_count = initial_transaction_count - transactions_df.shape[0]

    print(f"{Fore.RED}Total removed transactions that do not exist in items file: {removed_transaction_count}\n")

    # Step 2: Update the item_barcode in the transactions file based on the barcode from the items file
    print(f"{Fore.YELLOW}Updating transactions file with MAX_BARCODE...\n")
    total_affected = 0

    for index, row in tqdm(transactions_df.iterrows(), total=transactions_df.shape[0], desc="Processing transactions", ncols=100):
        item_barcode = row['item_barcode']
        if item_barcode in barcode_to_max_barcode:
            transactions_df.at[index, 'item_barcode'] = barcode_to_max_barcode[item_barcode]
            total_affected += 1
        else:
            not_found_items.append(item_barcode)

    # Step 3: Write not found items to a log
    if not_found_items:
        print(f"{Fore.RED}Writing not found items to logs/not_found_items.csv...\n")
        with open('logs/not_found_items.csv', 'w', newline='', encoding='utf-8-sig') as log_file:
            writer = csv.writer(log_file)
            writer.writerow(['item_barcode'])  # Column header
            for item in not_found_items:
                writer.writerow([item])

    # Step 4: Save the updated transactions file
    print(f"{Fore.GREEN}Saving the updated transactions file...\n")
    transactions_df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"{Fore.CYAN}Total affected records in transactions: {total_affected}\n")

    # Deduplicate items_file keeping the max BARCODE when equal to MAX_BARCODE
    print(f"{Fore.YELLOW}Deduplicating items file...\n")
    deduplicated_items = items_df.sort_values(by='BARCODE', ascending=False).drop_duplicates(subset='MAX_BARCODE', keep='first')

    # Find the deleted records
    deleted_records = items_df[~items_df.index.isin(deduplicated_items.index)]

    # Log the deleted records
    if not deleted_records.empty:
        print(f"{Fore.RED}Writing deleted records to logs/deleted_items.csv...\n")
        deleted_records.to_csv('logs/deleted_items.csv', index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    # Save the deduplicated items file
    print(f"{Fore.GREEN}Saving deduplicated items file...\n")
    deduplicated_items.to_csv(items_file, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    # Calculate the time taken
    end_time = time.time()
    total_time = end_time - start_time
    print(f"{Fore.GREEN}Process completed in {total_time:.2f} seconds.")

# Example usage:
# update_transactions('data/transactions_file.csv', 'data/items_file.csv', 'output/updated_transactions.csv')
