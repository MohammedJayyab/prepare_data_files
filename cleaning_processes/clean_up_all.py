import os
from delete_incorrect_products import delete_incorrect_products
from trim_spaces_commas import trim_spaces_commas

from correct_product_description import correct_product_description
from check_duplicates_modified_desc import run_duplication_checks
from clean_barcode import clean_barcode
#from translate_missing_english_fields import translate_missing_english_fields  # Import translation function
from update_transactions_and_deduplicate_items import update_transactions  # Import transaction update function

# Main function for orchestrating the cleanup process
def main():
    
    print("Starting the full cleanup process...\n")
    os.makedirs('Logs', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    
    # Define file paths for input and output at each stage
    input_file_path = 'Data/FARM_ITEM_MASTE_FILE.xlsx'   # Original Excel file
    trimmed_output_file = 'Output/Trimmed_FARM_ITEM_MASTER_FILE.csv'  # CSV after trimming
    #cleaned_descriptions_file = 'Output/cleaned_product_descriptions.csv'  # CSV after description cleanup
    delete_incorrect_products_file = 'Output/Cleaned_FARM_ITEM_No_Temp_FILE.csv'  # Log file for deleted products
    correct_product_description_file = 'Output/Corrected_FARM_ITEM_MASTER_FILE.csv'  # Corrected product description
    check_duplicates_modified_desc_file = 'Output/Duplicates_FARM_ITEM_MASTER_FILE.csv'  # Duplicates file
    update_transactions_file = 'Output/Updated_Transactions.csv'  # Updated transactions file
    #translated_output_file = 'Output/translated_product_descriptions.csv'  # CSV after translation
    final_cleaned_output_file = 'Output/Cleaned_FARM_ITEM_MASTER_FILE.csv'  # Final CSV after barcode cleaning

    # Step 1: Call the trim_spaces_commas function to trim spaces from specified columns
    trim_spaces_commas(input_file_path, trimmed_output_file)

    delete_incorrect_products(trimmed_output_file, delete_incorrect_products_file)

    # Step 2: Call the correct_product_description  
    correct_product_description(delete_incorrect_products_file, correct_product_description_file)

    # Step 2: Call the clean_descriptions function to combine descriptions and remove empty ones
    #removed_descriptions_count = clean_descriptions(trimmed_output_file, cleaned_descriptions_file)

    # Step 3: Call the translation function to translate missing English fields
    #translate_missing_english_fields(cleaned_descriptions_file, translated_output_file)

    run_duplication_checks(correct_product_description_file,check_duplicates_modified_desc_file)
    update_transactions(check_duplicates_modified_desc_file, update_transactions_file)
    # Step 4: Call the clean_barcode function to clean duplicate barcodes and handle missing barcodes
    clean_barcode(check_duplicates_modified_desc_file, final_cleaned_output_file)

    

    # Display total removed records
    #print(f"\n{removed_descriptions_count} records were removed during the description cleaning process.")
    print("\nFull cleanup process completed.")

# Entry point for running the script
if __name__ == "__main__":
    main()
