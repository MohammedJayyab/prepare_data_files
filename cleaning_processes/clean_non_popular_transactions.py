import pandas as pd

def clean_transactions(input_transactions_file, output_transaction_file, non_relevant_items_file):
    # Load the data from the CSV file
    df = pd.read_csv(input_transactions_file)

    # Step 1: Remove all transactions for customers who have <= 5 unique invoice_ids
    invoice_count_per_customer = df.groupby('customer_barcode')['invoice_id'].nunique()
    customers_with_few_invoices = invoice_count_per_customer[invoice_count_per_customer <= 5].index
    step1_removed = df[df['customer_barcode'].isin(customers_with_few_invoices)].shape[0]
    df = df[df['customer_barcode'].apply(lambda x: x not in customers_with_few_invoices)]
    print(f"Step 1 - Removed {step1_removed} transactions for customers with <= 5 invoice_ids")

    # Step 2: Remove all transactions for customers who have <= 10 total transactions
    transaction_count_per_customer = df['customer_barcode'].value_counts()
    customers_with_few_transactions = transaction_count_per_customer[transaction_count_per_customer <= 10].index
    step2_removed = df[df['customer_barcode'].isin(customers_with_few_transactions)].shape[0]
    df = df[df['customer_barcode'].apply(lambda x: x not in customers_with_few_transactions)]
    print(f"Step 2 - Removed {step2_removed} transactions for customers with <= 10 total transactions")
    
    # Step 3: Remove all transactions for customers who interact with <= 5 unique items
    unique_items_per_customer = df.groupby('customer_barcode')['item_barcode'].nunique()
    customers_with_few_unique_items = unique_items_per_customer[unique_items_per_customer <= 5].index
    step3_removed = df[df['customer_barcode'].isin(customers_with_few_unique_items)].shape[0]
    df = df[df['customer_barcode'].apply(lambda x: x not in customers_with_few_unique_items)]
    print(f"Step 3 - Removed {step3_removed} transactions for customers interacting with <= 5 unique items")
    
    # Step 4: Remove all barcodes that are used <= 5 times
    barcode_count = df['item_barcode'].value_counts()
    non_relevant_barcodes = barcode_count[barcode_count <= 5].index
    step4_removed = df[df['item_barcode'].isin(non_relevant_barcodes)].shape[0]
    
    # Save the non-relevant barcodes to a text file
    with open(non_relevant_items_file, 'w') as f:
        for barcode in non_relevant_barcodes:
            f.write(f"{barcode}\n")
    
    # Filter out non-relevant barcodes
    df = df[df['item_barcode'].apply(lambda x: x not in non_relevant_barcodes)]
    print(f"Step 4 - Removed {step4_removed} transactions for barcodes used <= 5 times")

    # Save the cleaned data to a new CSV file
    df.to_csv(output_transaction_file, index=False)
    
    # Show total removed records and summary after each step
    summary = {
        "Step 1 - Removed for <=5 invoice_ids": step1_removed,
        "Step 2 - Removed for <=10 transactions": step2_removed,
        "Step 3 - Removed for <=5 unique items": step3_removed,
        "Step 4 - Removed barcodes used <= 5 times": step4_removed,
        "Remaining records": df.shape[0]
    }

    return summary


if(__name__ == '__main__'):
    # Run the cleaning function
    input_file = 'Output/Cleaned_ml_transactions_outbox.csv'
    output_file = 'Output/Cleaned_ml_transactions_outbox_non_relevant.csv'
    non_relevant_items_file = 'Logs/non_relevant_barcodes.txt'
    summary = clean_transactions(input_file, output_file, non_relevant_items_file)

    # Print the summary of the cleanup process
    print("Summary of cleaning process:")
    for key, value in summary.items():
        print(f"{key}: {value}")

    print(f"Cleaned transactions saved to: {output_file}")
    print(f"Non-relevant items saved to: {non_relevant_items_file}")
    print("Cleaning process completed")

