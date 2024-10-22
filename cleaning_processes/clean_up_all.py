import os
from delete_incorrect_products import delete_incorrect_products
from trim_spaces_commas import trim_spaces_commas
from colorama import Fore, Style, init
from correct_product_description import correct_product_description
from check_duplicates_modified_desc import run_duplication_checks
from clean_barcode import clean_barcode
from correct_category_levels import correct_category_levels
#from translate_missing_english_fields import translate_missing_english_fields  # Import translation function
from update_transactions_and_deduplicated_items import update_transactions  # Import transaction update function
from check_transactions import check_transaction_items  # Import transaction check function

    # Define file paths for input and output at each stage
input_file_path = 'Data/ml_items.csv'   # Original Excel file
trimmed_output_file = 'Output/Trimmed_ml_items.csv'  # CSV after trimming
#cleaned_descriptions_file = 'Output/cleaned_product_descriptions.csv'  # CSV after description cleanup
delete_incorrect_products_file = 'Output/Cleaned_ml_items_no_temp.csv'  # Log file for deleted products
correct_product_description_file = 'Output/Cleaned_Desc_ml_items.csv'  # Corrected product description
check_duplicates_modified_desc_file = 'Output/Cleaned_Duplicates_ml_items.csv'  # Duplicates file
transactions_file = 'Data/ml_transactions_outbox.csv'  # Updated transactions file
transactions_updated_file = 'Output/Cleaned_ml_transactions_outbox.csv'  # Updated transactions file    
correct_category_levels_file = 'Output/Cleaned_Category_ml_items.csv'  # Corrected category levels
#final_cleaned_empty_barcode_file = 'Output/Cleaned_Empty_Barcode_ml_items.csv'  # Final CSV after barcode cleaning
cleaned_with_max_barcode_file = 'Output/Cleaned_Max_Barcode_ml_items.csv'  # Final CSV after barcode cleaning
final_cleaned_output_file = 'Output/Cleaned_ml_items.csv'  # Final CSV after barcode cleaning

#Logs
log_file_path = 'Logs/missing_or_corrected_category_levels.csv'  # Log file for missing or corrected category levels

# create a function to delete intermediate files
def delete_intermediate_files():

    print(f"{Fore.YELLOW}Cleaning up intermediate files...")
    os.remove(trimmed_output_file)
    os.remove(delete_incorrect_products_file)
    os.remove(correct_product_description_file)
    os.remove(check_duplicates_modified_desc_file)    
    os.remove(correct_category_levels_file)
    

def delete_logs_files():
     print(f"{Fore.YELLOW}Deleting all files inside Logs folder...")
     for file in os.listdir('Logs'):
            os.remove(f'Logs/{file}')    

# Main function for orchestrating the cleanup process
def clean_update_all():
    
    print("Starting the full cleanup process...\n")
    os.makedirs('Logs', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    
    # step 0: delete all files Logs inside  Logs folder
    delete_logs_files()
            
    trim_spaces_commas(input_file_path, trimmed_output_file)
    delete_incorrect_products(trimmed_output_file, delete_incorrect_products_file)    
    correct_product_description(delete_incorrect_products_file, correct_product_description_file) 
    run_duplication_checks(correct_product_description_file,check_duplicates_modified_desc_file)    
    # correct category_levels
    correct_category_levels(check_duplicates_modified_desc_file, correct_category_levels_file, log_file_path)

    clean_barcode(correct_category_levels_file, cleaned_with_max_barcode_file) 
    update_transactions(transactions_file,cleaned_with_max_barcode_file, final_cleaned_output_file, transactions_updated_file)

    # make sure all barcodes in transactions exist in items file

    is_ok= check_transaction_items(transactions_updated_file, final_cleaned_output_file)
    # cleanup process and delete intermediate files
    

    if(is_ok):
        delete_intermediate_files()
        print(f"Final cleaned 'products' saved to: {final_cleaned_output_file}")        
        print(f"{Fore.GREEN}Full cleanup process completed.")
    else:
        print(f"{Fore.RED}Error: Some item_barcodes from transactions do not exist in the items file.")
        print(f"{Fore.RED}Full cleanup process aborted.")

#only update transactions  every three months
def update_transactions_only():
    print("Starting the transactions update process...\n")
    os.makedirs('Logs', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    delete_logs_files()
    
    update_transactions(transactions_file,cleaned_with_max_barcode_file, final_cleaned_output_file, transactions_updated_file)

    # make sure all barcodes in transactions exist in items file

    is_ok= check_transaction_items(transactions_updated_file, final_cleaned_output_file)
    
    if(is_ok):
        delete_intermediate_files()
        print(f"Final cleaned 'transactions' saved to: {transactions_updated_file}")
    else:
        print(f"{Fore.RED}Error: Some item_barcodes from transactions do not exist in the items file.")
        print(f"{Fore.RED}Transactions update process aborted.")


if __name__ == "__main__":
    # Initialize colorama
    init(autoreset=True)
    # menu
    print("Choose an option:")
    print("1. Full cleanup process (Products and Transactions)")
    print("2. Update transactions ONLY? (2)")
    print("3. or 'x' Exit")
    choice = input("Enter your choice (1/2): 3 for exit: ")
    while choice not in ['1', '2', '3', 'x', 'X']:
        print(f"{Fore.RED}Invalid choice. Please enter either 1 or 2.")
        choice = input("Enter your choice (1/2): 3 for exit: ")
    if choice == '1':
        clean_update_all()
    elif choice == '2':
        update_transactions_only()
    elif choice == 'x' or choice == 'X' or choice == '3':
        print(f"{Fore.YELLOW}Exiting the program...")
        exit()
    else:
        print(f"{Fore.RED}Invalid choice. Please enter either 1 or 2.")

    
    