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

        # If en_full_description is empty/null, translate from ar_full_description
        if pd.isnull(row['en_full_description']) or row['en_full_description'].strip() == '':
            if not pd.isnull(row['ar_full_description']) and row['ar_full_description'].strip() != '':
                translated_text = translator.translate(row['ar_full_description'], dest='en').text
                df.at[index, 'en_full_description'] = translated_text
                translated_full_desc = True

        # If en_short_desc is empty/null, translate from ar_short_desc
        if pd.isnull(row['en_short_desc']) or row['en_short_desc'].strip() == '':
            if not pd.isnull(row['ar_short_desc']) and row['ar_short_desc'].strip() != '':
                translated_text = translator.translate(row['ar_short_desc'], dest='en').text
                df.at[index, 'en_short_desc'] = translated_text
                translated_short_desc = True

        # If either translation occurred, set the Translation_Status column to 'Translated To English'
        if translated_full_desc or translated_short_desc:
            df.at[index, 'Translation_Status'] = 'Translated To English'

    # Save the updated DataFrame to a new CSV file with quotations for non-numeric fields
    df.to_csv(output_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig')
    print(f"Translation of missing English fields completed. Saved to: {output_file_path}")
