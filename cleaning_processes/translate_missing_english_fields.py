from googletrans import Translator
import pandas as pd
import csv

def translate_missing_english_fields(input_file_path, output_file_path):
    # Load the CSV file into a DataFrame, treating all columns as strings to avoid DtypeWarning
    df = pd.read_csv(input_file_path, encoding='utf-8-sig', dtype=str, low_memory=False)

    # Create a Translator object
    translator = Translator()

    # Add a new column for marking translations
    df['Translation_Status'] = ''

    # Translate for each row where English descriptions are missing
    for index, row in df.iterrows():
        translated_full_desc = False  # Flag to track if a full description translation happened
        translated_short_desc = False  # Flag to track if a short description translation happened

        # If EN_FULL_DESCRIPTION is empty/null, translate from AR_FULL_DESCRIPTION
        if pd.isnull(row['EN_FULL_DESCRIPTION']) or row['EN_FULL_DESCRIPTION'].strip() == '':
            if not pd.isnull(row['AR_FULL_DESCRIPTION']) and row['AR_FULL_DESCRIPTION'].strip() != '':
                translated_text = translator.translate(row['AR_FULL_DESCRIPTION'], dest='en').text
                df.at[index, 'EN_FULL_DESCRIPTION'] = translated_text
                translated_full_desc = True

        # If EN_SHORT_DESC is empty/null, translate from AR_SHORT_DESC
        if pd.isnull(row['EN_SHORT_DESC']) or row['EN_SHORT_DESC'].strip() == '':
            if not pd.isnull(row['AR_SHORT_DESC']) and row['AR_SHORT_DESC'].strip() != '':
                translated_text = translator.translate(row['AR_SHORT_DESC'], dest='en').text
                df.at[index, 'EN_SHORT_DESC'] = translated_text
                translated_short_desc = True

        # If either translation occurred, set the Translation_Status column to 'Translated To English'
        if translated_full_desc or translated_short_desc:
            df.at[index, 'Translation_Status'] = 'Translated To English'

    # Save the updated DataFrame to a new CSV file with quotations for non-numeric fields
    df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
    print(f"Translation of missing English fields completed. Saved to: {output_file_path}")
