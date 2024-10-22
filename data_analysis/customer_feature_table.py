import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# Function to load data
def load_data(file_path):
    print(f"Loading data from {file_path}...")
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

# Function to calculate customer features and create a pivot table
def create_customer_feature_table(transactions_df, output_file):
    print("Converting data types and calculating features...")

    # Convert necessary columns to appropriate types
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    transactions_df['unit_price'] = transactions_df['unit_price'].astype(float)
    transactions_df['amount'] = transactions_df['quantity'] * transactions_df['unit_price']

    # Parse 'timestamp' with day-first format
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    # Adding time-related features for each transaction
    transactions_df['day_of_week'] = transactions_df['timestamp'].dt.day_name()
    transactions_df['time_of_day'] = transactions_df['timestamp'].dt.hour

    # Progress bar for groupby operations
    print("Calculating customer statistics...")
    customer_stats = transactions_df.groupby('customer_barcode').agg(
        total_orders=('invoice_id', 'nunique'),                # Total number of unique orders
        total_items=('quantity', 'sum'),                      # Total number of items purchased
        total_spent=('amount', 'sum'),                        # Total money spent
        first_purchase=('timestamp', 'min'),                  # Date of first purchase
        last_purchase=('timestamp', 'max'),                   # Date of last purchase
        most_frequent_day=('day_of_week', lambda x: x.mode()[0]),  # Most frequent day of the week
        most_frequent_time=('time_of_day', lambda x: x.mode()[0]), # Most frequent time of day
    ).reset_index()

    # Add derived features
    print("Calculating derived features (average values, RFM features, Z-scores)...")
    customer_stats['average_order_value'] = customer_stats['total_spent'] / customer_stats['total_orders']
    customer_stats['average_quantity_per_order'] = customer_stats['total_items'] / customer_stats['total_orders']
    customer_stats['average_spend_per_item'] = customer_stats['total_spent'] / customer_stats['total_items']

    # Calculate Recency, Frequency, and Monetary (RFM) Features
    customer_stats['recency_days'] = (transactions_df['timestamp'].max() - customer_stats['last_purchase']).dt.days
    customer_stats['frequency'] = customer_stats['total_orders']
    customer_stats['monetary'] = customer_stats['total_spent']

    # Z-score Normalization for Frequency and Monetary Values
    customer_stats['frequency_zscore'] = (customer_stats['frequency'] - customer_stats['frequency'].mean()) / customer_stats['frequency'].std()
    customer_stats['monetary_zscore'] = (customer_stats['monetary'] - customer_stats['monetary'].mean()) / customer_stats['monetary'].std()

    # Customer lifetime (days between first and last purchase)
    customer_stats['customer_lifetime'] = (customer_stats['last_purchase'] - customer_stats['first_purchase']).dt.days

    # Determine the favorite category (most purchased)
    if 'category_name' in transactions_df.columns:
        print("Calculating favorite category for each customer...")
        product_stats = transactions_df.groupby(['customer_barcode', 'category_name']).agg(
            total_category_purchases=('quantity', 'sum')
        ).reset_index()

        favorite_category = product_stats.loc[product_stats.groupby('customer_barcode')['total_category_purchases'].idxmax()]
        customer_stats = pd.merge(customer_stats, favorite_category[['customer_barcode', 'category_name']], on='customer_barcode', how='left')
        customer_stats.rename(columns={'category_name': 'favorite_category'}, inplace=True)
    else:
        print("Warning: 'category_name' column not found, skipping favorite category calculation.")
        customer_stats['favorite_category'] = np.nan

    # Calculate purchase trend over time (upward, downward, stable)
    print("Calculating purchase trend over time (weekly purchases)...")
    transactions_df['week'] = transactions_df['timestamp'].dt.isocalendar().week

    # Group weekly purchases for trend analysis
    weekly_purchase_trend = transactions_df.groupby(['customer_barcode', 'week']).size().reset_index(name='purchases_per_week')

    # Remove any rows with missing or non-numeric data in the trend analysis
    weekly_purchase_trend = weekly_purchase_trend.dropna(subset=['week', 'purchases_per_week'])
    
    # Ensure that week and purchases_per_week are numeric
    weekly_purchase_trend['week'] = pd.to_numeric(weekly_purchase_trend['week'], errors='coerce')
    weekly_purchase_trend['purchases_per_week'] = pd.to_numeric(weekly_purchase_trend['purchases_per_week'], errors='coerce')

    # Perform trend analysis using np.polyfit, handling missing data
    def calculate_trend(group):
        if len(group) > 1:
            return np.polyfit(group['week'], group['purchases_per_week'], 1)[0]
        else:
            return 0  # No trend if only one data point

    trend = weekly_purchase_trend.groupby('customer_barcode').apply(calculate_trend)
    customer_stats['purchase_trend'] = trend.apply(lambda x: 'Upward' if x > 0 else ('Downward' if x < 0 else 'Stable')).values

    # Calculate time between purchases for each customer
    print("Calculating time between purchases...")
    transactions_df = transactions_df.sort_values(by=['customer_barcode', 'timestamp'])
    transactions_df['time_diff'] = transactions_df.groupby('customer_barcode')['timestamp'].diff().dt.days

    avg_time_between_purchases = transactions_df.groupby('customer_barcode')['time_diff'].mean().reset_index()
    customer_stats = pd.merge(customer_stats, avg_time_between_purchases, on='customer_barcode', how='left')
    customer_stats.rename(columns={'time_diff': 'avg_time_between_purchases'}, inplace=True)

    # Segmentation (Based on Z-scores and important metrics)
    print("Performing segmentation on customers...")
    customer_stats['order_segment'] = pd.cut(customer_stats['total_orders'], bins=[0, 1, 3, 5, 10, float('inf')], labels=['One-time', 'Low', 'Medium', 'High', 'Very High'])
    customer_stats['frequency_segment'] = pd.cut(customer_stats['frequency_zscore'], bins=[-float('inf'), -1, 0, 1, float('inf')], labels=['Low', 'Average', 'High', 'Very High'])
    customer_stats['monetary_segment'] = pd.cut(customer_stats['monetary_zscore'], bins=[-float('inf'), -1, 0, 1, float('inf')], labels=['Low Spend', 'Average Spend', 'High Spend', 'Very High Spend'])

    # Assign 1 for relevant customers in each segment
    customer_stats['is_high_value_customer'] = np.where(customer_stats['monetary_zscore'] > 1, 1, 0)
    customer_stats['is_high_frequency_customer'] = np.where(customer_stats['frequency_zscore'] > 1, 1, 0)

    # Round the float columns to 2 decimal places
    customer_stats = customer_stats.round(2)

    # Export the resulting dataframe to CSV
    print(f"Saving customer feature table to {output_file}...")
    customer_stats.to_csv(output_file, index=False)
    print(f"Feature table saved to {output_file} successfully.")

# Main function to generate the customer feature table
def main():
    transactions_file_path = os.path.join('output', 'Cleaned_ml_transactions_outbox.csv')
    output_file = os.path.join('reports', 'customer_feature_table.csv')

    # Load the cleaned transaction data
    print("Starting process to generate customer feature table...")
    transactions_df = load_data(transactions_file_path)

    # Generate the customer feature table and save it as a CSV
    create_customer_feature_table(transactions_df, output_file)

    print("Process completed. Please check the output file for results.")

if __name__ == "__main__":
    main()
