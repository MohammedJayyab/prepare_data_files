import os
import pandas as pd
import csv
from tqdm import tqdm

def update_and_clean_transactions(current_transaction_file, current_items_file, new_transaction_file, new_items_file):
    if not os.path.exists(current_transaction_file):
        print(f"Error: '{current_transaction_file}' does not exist.")
        return
    
    if not os.path.exists(current_items_file):
        print(f"Error: '{current_items_file}' does not exist.")
        return

    print("Loading transaction data...")
    transaction_df = pd.read_csv(current_transaction_file, dtype=str)
    print("Loading items data...")
    items_df = pd.read_csv(current_items_file, dtype=str)

    print("Merging transaction data with item numbers...")
    total_rows = len(transaction_df)
    progress = tqdm(total=total_rows, desc="Merging", unit="rows", ncols=80)

    # Merging only item_number based on item_barcode
    merged_df = pd.merge(transaction_df, items_df[['item_number', 'barcode']], left_on='item_barcode', right_on='barcode', how='left')
    progress.update(total_rows)
    progress.close()

    # Dropping unwanted columns and rearranging
    merged_df = merged_df.drop(columns=['item_id', 'barcode'])

    column_order = ['customer_barcode', 'invoice_id', 'item_number', 'item_barcode', 'quantity', 'unit_price', 'amount', 'interaction_type', 'timestamp']
    merged_df = merged_df[column_order]

    if os.path.exists(new_transaction_file):
        os.remove(new_transaction_file)

    print(f"Saving merged transactions to '{new_transaction_file}'...")
    merged_df.to_csv(new_transaction_file, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"Loading merged transactions from '{new_transaction_file}'...")
    updated_transaction_df = pd.read_csv(new_transaction_file, dtype=str)

    initial_count = len(updated_transaction_df)
    print(f"Total transactions before removing duplicates: {initial_count}")

    unique_transactions_df = updated_transaction_df.drop_duplicates(subset='item_number', keep='first')

    final_count = len(unique_transactions_df)
    print(f"Total transactions after removing duplicates: {final_count}")

    if os.path.exists(new_items_file):
        os.remove(new_items_file)

    print("Removing unwanted columns from items data...")
    columns_to_remove = ["category_level1","category_level2","category_level3","category_level4", "department",
                         "vendor_code","vendor_name","uom", "packing", "en_short_desc", "ar_short_desc", "MAX_barcode_FOR_DUPLICATES", 
                         "MAX_barcode_FOR_DUPLICATED_AR", "MAX_barcode"]
    cleaned_items_df = items_df.drop(columns=columns_to_remove, errors='ignore')
    cleaned_items_df = cleaned_items_df.drop_duplicates(subset='item_number', keep='first')
    cleaned_items_df.columns = [col.lower() for col in cleaned_items_df.columns]


    print(f"Saving cleaned items data to '{new_items_file}'...")
    cleaned_items_df.to_csv(new_items_file, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')

    print(f"Total duplicate items removed: {initial_count - final_count}")

def validate_transactions_item_number(new_transaction_file, new_items_file):
    print("Loading new transactions data...")
    transaction_df = pd.read_csv(new_transaction_file, dtype=str)

    print("Loading new items data...")
    items_df = pd.read_csv(new_items_file, dtype=str)

    print("Validating if all item_numbers in transactions exist in the items file...")
    transaction_item_numbers = set(transaction_df['item_number'])
    items_item_numbers = set(items_df['item_number'])

    missing_item_numbers = transaction_item_numbers - items_item_numbers

    if missing_item_numbers:
        raise ValueError(f"Error: {len(missing_item_numbers)} item_numbers in transactions do not exist in the items file. Missing item_numbers: {missing_item_numbers}")
    else:
        print("Validation successful: All item_numbers in the transactions file exist in the items file.")


# Example usage with your file names
current_transaction_file = "Output/Cleaned_ml_transactions_outbox.csv"
current_items_file = "Output/Cleaned_ml_items.csv"
new_transaction_file = "Output/an_ml_transactions_outbox.csv"
new_items_file = "Output/an_ml_items.csv"

update_and_clean_transactions(current_transaction_file, current_items_file, new_transaction_file, new_items_file)
validate_transactions_item_number(new_transaction_file, new_items_file)
