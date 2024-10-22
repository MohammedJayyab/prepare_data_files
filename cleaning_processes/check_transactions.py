import pandas as pd
from colorama import Fore, Style, init

# Initialize colorama for colored output
init(autoreset=True)

def check_transaction_items(transactions_file, items_file):
    # Load transactions and items data
    print(f"{Fore.YELLOW}Loading transactions and items files...\n")
    transactions_df = pd.read_csv(transactions_file, dtype=str, low_memory=False)
    items_df = pd.read_csv(items_file, dtype=str, low_memory=False)

    # Get set of valid barcodes from items file
    valid_barcodes = set(items_df['barcode'])

    # Check for item_barcodes in transactions that do not exist in items_file
    not_found_barcodes = transactions_df[~transactions_df['item_barcode'].isin(valid_barcodes)]['item_barcode'].unique()
    df = pd.DataFrame(not_found_barcodes, columns=['item_barcode'])

    # Save not found barcodes to CSV
    df.to_csv('Logs/not_found_barcodes_in_items.csv', index=False)

    # Check for unique barcodes in transactions
    unique_barcodes_in_transactions = transactions_df['item_barcode'].unique()

    # Save unique barcodes to CSV
    unique_df = pd.DataFrame(unique_barcodes_in_transactions, columns=['unique_item_barcode'])
    unique_df.to_csv('Logs/unique_barcodes_in_transactions.csv', index=False)

    # Output results
    if len(not_found_barcodes) > 0:
        print(f"{Fore.RED}item_barcodes from transactions do not exist in the items file, Total records: {len(not_found_barcodes)}")
        return False
    else:
        print(f"{Fore.GREEN}All item_barcodes from transactions exist in the items file. Unique Item barcodes in transactions: {len(unique_barcodes_in_transactions)}")
        return True
    
# Example usage
def check_items_not_in_transactions(transactions_file, items_file):
    # Load transactions and items data
    print(f"{Fore.YELLOW}Loading transactions and items files...\n")
    transactions_df = pd.read_csv(transactions_file, dtype=str, low_memory=False)
    items_df = pd.read_csv(items_file, dtype=str, low_memory=False)

    # Get set of barcodes in the transactions file
    transaction_barcodes = set(transactions_df['item_barcode'])

    # Check for item barcodes in the items file that are not in the transactions file
    not_found_barcodes_in_transactions = items_df[~items_df['barcode'].isin(transaction_barcodes)]['barcode'].unique()
    df = pd.DataFrame(not_found_barcodes_in_transactions, columns=['item_barcode'])

    # Save to CSV
    #df.to_csv('Logs/not_found_barcodes_in_transactions.csv', index=False)

    # Output result
    if len(not_found_barcodes_in_transactions) > 0:
        print(f"{Fore.YELLOW}Item barcodes from the items file that do not exist in the transactions, Total records: {len(not_found_barcodes_in_transactions)}")
        
    else:
        print(f"{Fore.GREEN}All item barcodes from the items file are found in the transactions.")
        

if __name__ == "__main__":
    transactions_file = 'Output/Cleaned_ml_transactions_outbox.csv'  # Path to your transactions file
    items_file = 'Output/Cleaned_ml_items.csv'  # Path to your items file

    check_transaction_items(transactions_file, items_file)
    check_items_not_in_transactions(transactions_file, items_file)
