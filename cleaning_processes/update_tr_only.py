from update_transactions_and_deduplicate_items import update_transactions  
def main():
    transactions_file = 'Data/ml_transactions_outbox.csv'  # Updated transactions file
    check_duplicates_modified_desc_file = 'Output/Duplicates_ml_items_master.csv'  # Duplicates file
    transactions_updated_file = 'Output/Cleaned_ml_transactions_outbox.csv'  # Updated transactions file


    update_transactions(transactions_file, check_duplicates_modified_desc_file, transactions_updated_file)

if __name__ == "__main__":
    main()