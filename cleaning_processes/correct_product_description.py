import pandas as pd
import csv  # Correct import for quoting
import re  # For detecting product numbers and cleaning up text

def is_informative(short_desc, full_desc):
    # Define threshold to determine if short_desc lacks key details
    return len(short_desc.split()) > 2

def extract_product_number(full_desc):
    """Extract product number (e.g., #HL001) from the full description."""
    match = re.search(r'#\s*\w+', full_desc)
    return match.group(0) if match else ''

def clean_description(desc):
    """Clean up the description by removing leading/trailing punctuation and spaces."""
    # Remove leading/trailing dashes, colons, and extra spaces
    return re.sub(r'^[\s\-:]+|[\s\-:]+$', '', desc)

def correct_product_description(input_file_path, output_file_path):
    print(f"Correcting product descriptions ..............")
    # Load the CSV file into a DataFrame, treating all columns as strings
    df = pd.read_csv(input_file_path, encoding='utf-8-sig', dtype=str, low_memory=False)

    # Add a new column for the modified short description
    df['MODIFIED_SHORT_DESC'] = ''

    # Function to check if a string is numeric
    def is_numeric(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    # Iterate through the DataFrame to apply the correction rules
    for index, row in df.iterrows():
        en_short_desc = row['en_short_desc'] if pd.notnull(row['en_short_desc']) else ''
        en_full_desc = row['en_full_description'] if pd.notnull(row['en_full_description']) else ''

        # Extract the product number (e.g., #HL001) from en_full_description
        product_number = extract_product_number(en_full_desc)

        # Remove product number from full description for further processing
        en_full_desc_cleaned = en_full_desc.replace(product_number, '').strip()

        # Clean up leading/trailing punctuation (e.g., hyphens, colons)
        en_full_desc_cleaned = clean_description(en_full_desc_cleaned)

        modified_short_desc = en_short_desc.strip()  # Start with the original short description

        # Rule: If en_short_desc is null, empty, or numeric, set it to en_full_description
        if modified_short_desc == '' or is_numeric(modified_short_desc):
            modified_short_desc = en_full_desc_cleaned
        
        # Rule 1: If en_short_desc equals barcode value, set it to en_full_description
        elif modified_short_desc == row['barcode']:
            modified_short_desc = en_full_desc_cleaned

        # Rule 2: If en_short_desc starts with '*' and ends with '*', preserve it
        elif modified_short_desc.startswith('*') and modified_short_desc.endswith('*'):
            continue  # Skip appending details, preserving en_short_desc as is
        
        # Rule 3a: If en_short_desc starts with '#', check if it contains en_full_description, then set it to en_full_description
        elif modified_short_desc.startswith('#') and modified_short_desc in row['en_full_description']:
            modified_short_desc = en_full_desc_cleaned
        
        # Rule 3b: If en_short_desc starts with '#' and doesn't contain en_full_description, concatenate en_full_description and en_short_desc
        elif modified_short_desc.startswith('#') and modified_short_desc not in row['en_full_description']:
            modified_short_desc = en_full_desc_cleaned + ' ' + modified_short_desc

        # New Rule: Detect non-informative short description and avoid duplication
        if not is_informative(modified_short_desc, en_full_desc_cleaned):
            # If short description lacks details, append missing details from en_full_description
            missing_details = ' '.join([word for word in en_full_desc_cleaned.split() if word not in modified_short_desc])
            modified_short_desc = missing_details.strip() + ' ' + modified_short_desc.strip()

        # Ensure the product number is appended to the END of MODIFIED_SHORT_DESC (not at the beginning)
        if product_number and product_number not in modified_short_desc:
            modified_short_desc = modified_short_desc.strip() + ' ' + product_number

        # Ensure the product number stays at the end and doesn't have extra hyphen
        modified_short_desc = clean_description(modified_short_desc)

        # Convert both en_full_description and MODIFIED_SHORT_DESC to uppercase and strip spaces
        df.at[index, 'MODIFIED_SHORT_DESC'] = ' '.join(modified_short_desc.upper().split())

    # Save the corrected DataFrame to a new CSV file, using correct quoting for non-numeric fields
    df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
    print(f"Product descriptions corrected and saved to: {output_file_path}")

# Example usage:
# correct_product_description('input_file.csv', 'output_file.csv')
