import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# Function to load data
def load_data(file_path):
    print(f"Loading data from {file_path}...")
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

# Function to create the product feature table
def create_product_feature_table(products_df, transactions_df, output_file):
    print("Converting data types and calculating features...")

    # Convert necessary columns to appropriate types
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    transactions_df['unit_price'] = transactions_df['unit_price'].astype(float)
    transactions_df['amount'] = transactions_df['quantity'] * transactions_df['unit_price']
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    # Remove duplicate products before merging to avoid duplication issues
    products_df = products_df.drop_duplicates(subset=['item_number'])

    # Merge product data with transaction data on 'item_number'
    merged_df = pd.merge(transactions_df, products_df, on='item_number', how='left')

    # 1. Sales Performance Metrics
    print("Calculating Sales Performance Metrics...")
    product_stats = merged_df.groupby('item_number').agg(
        total_sales_units=('quantity', 'sum'),                  # Total units sold
        total_revenue=('amount', 'sum'),                        # Total revenue generated
        num_orders=('invoice_id', 'nunique'),                   # Number of unique orders containing this product
        avg_order_quantity=('quantity', 'mean'),                # Average quantity per order
        sales_per_day=('amount', lambda x: x.sum() / (merged_df['timestamp'].max() - merged_df['timestamp'].min()).days), # Sales rate per day
        return_rate=('quantity', lambda x: (x < 0).sum() / x.count())  # Return rate
    ).reset_index()

    # 2. Customer Engagement
    print("Calculating Customer Engagement Metrics...")
    customer_engagement = merged_df.groupby('item_number').agg(
        num_unique_buyers=('customer_barcode', 'nunique'),     # Number of unique buyers
        repeat_purchase_rate=('invoice_id', lambda x: (x.duplicated()).mean())  # Percentage of repeat purchases
    ).reset_index()

    # 3. Time-Based Performance
    print("Calculating Time-Based Performance Metrics...")
    time_based_performance = merged_df.groupby('item_number').agg(
        first_sale=('timestamp', 'min'),                      # Date of first sale
        last_sale=('timestamp', 'max'),                       # Date of last sale
        time_on_market=('timestamp', lambda x: (x.max() - x.min()).days),  # Time on market (days between first and last sale)
        days_since_last_sale=('timestamp', lambda x: (merged_df['timestamp'].max() - x.max()).days)  # Days since last sale
    ).reset_index()

    # 4. Market Share and Seasonality (Simplified Example)
    print("Calculating Market Share and Seasonality Metrics...")
    total_market_sales = merged_df['amount'].sum()
    market_share = product_stats['total_revenue'] / total_market_sales  # Market share per product

    # 5. Returns and Warranty (Return Rate included in Sales Performance)
    # Already calculated `return_rate` in Sales Performance Metrics.

    # 6. Trending Status (Simple approach based on time)
    print("Calculating Trending Status Metrics...")
    trending_status = merged_df.groupby(['item_number', merged_df['timestamp'].dt.to_period('M')]).agg(
        monthly_sales=('quantity', 'sum')
    ).reset_index()

    # Calculate the trend over time
    def calculate_trending_status(group):
        if len(group) > 1:
            return np.polyfit(group['timestamp'].astype(int), group['monthly_sales'], 1)[0]
        else:
            return 0

    trending_status_df = trending_status.groupby('item_number').apply(calculate_trending_status).reset_index(name='trending_status')

    # Merge all calculated features into the product feature table
    print("Merging features to create the final product feature table...")
    product_feature_table = pd.merge(product_stats, customer_engagement, on='item_number', how='left')
    product_feature_table = pd.merge(product_feature_table, time_based_performance, on='item_number', how='left')
    product_feature_table = pd.merge(product_feature_table, trending_status_df, on='item_number', how='left')

    # Add market share as a separate column
    product_feature_table['market_share'] = market_share

    # Include product information (modified_short_desc and category_name)
    product_feature_table = pd.merge(product_feature_table, products_df[['item_number', 'modified_short_desc', 'category_name']], on='item_number', how='left')

    # Round float values to 2 decimal places
    product_feature_table = product_feature_table.round(2)

    # Export the resulting dataframe to CSV
    print(f"Saving product feature table to {output_file}...")
    product_feature_table.to_csv(output_file, index=False)
    print(f"Product feature table saved to {output_file} successfully.")

# Main function to generate the product feature table
def main():
    products_file_path = os.path.join('output', 'an_ml_items.csv')
    transactions_file_path = os.path.join('output', 'an_ml_transactions_outbox.csv')
    output_file = os.path.join('reports', 'product_feature_table.csv')

    # Load the product and transaction data
    print("Loading product and transaction data...")
    products_df = load_data(products_file_path)
    transactions_df = load_data(transactions_file_path)

    # Generate the product feature table and save it as a CSV
    create_product_feature_table(products_df, transactions_df, output_file)

    print("Process completed. Please check the output file for results.")

if __name__ == "__main__":
    main()
