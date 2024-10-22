import pandas as pd
import os

def load_data(file_path):
    # Load transactions data with appropriate column names
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

def load_products(file_path):
    # Load products data with appropriate column names
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

def write_to_csv(results, csv_file):
    # Convert the dictionary results to DataFrame and write to CSV
    df = pd.DataFrame([results])
    df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)

def repeat_purchase_rate(transactions_df):
    repeat_customers = transactions_df.groupby('customer_barcode')['invoice_id'].nunique().reset_index()
    repeat_rate = (repeat_customers[repeat_customers['invoice_id'] > 1].shape[0] / repeat_customers.shape[0]) * 100
    return repeat_rate

def customer_segmentation(transactions_df):
    segmentation = transactions_df.groupby('customer_barcode').agg(order_count=('invoice_id', 'nunique')).reset_index()
    segmentation['segment'] = pd.cut(segmentation['order_count'], bins=[0, 1, 5, 10, float('inf')], labels=['One-time', 'Low', 'Medium', 'High'])
    segment_distribution = segmentation['segment'].value_counts(normalize=True) * 100
    return segment_distribution

def cohort_analysis(transactions_df):
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    transactions_df['cohort'] = transactions_df.groupby('customer_barcode')['timestamp'].transform('min').dt.to_period('M')
    cohort_counts = transactions_df.groupby(['cohort']).agg(customers=('customer_barcode', 'nunique'))
    return cohort_counts

def average_order_value(transactions_df):
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    transactions_df['unit_price'] = transactions_df['unit_price'].astype(float)
    transactions_df['amount'] = transactions_df['quantity'] * transactions_df['unit_price']
    aov = transactions_df.groupby('invoice_id')['amount'].sum().mean()
    return aov

def customer_retention_rate(transactions_df):
    retained_customers = transactions_df.groupby('customer_barcode')['invoice_id'].nunique().reset_index()
    retention_rate = (retained_customers[retained_customers['invoice_id'] > 1].shape[0] / retained_customers.shape[0]) * 100
    return retention_rate

def customer_lifetime_value(transactions_df):
    transactions_df['amount'] = transactions_df['quantity'].astype(int) * transactions_df['unit_price'].astype(float)
    clv = transactions_df.groupby('customer_barcode')['amount'].sum().mean()
    return clv

def product_profitability(transactions_df):
    transactions_df['amount'] = transactions_df['quantity'].astype(int) * transactions_df['unit_price'].astype(float)
    top_products = transactions_df.groupby('item_barcode')['amount'].sum().sort_values(ascending=False).head(10).reset_index()
    return top_products

def sales_trend_forecasting(transactions_df):
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    transactions_df['date'] = transactions_df['timestamp'].dt.to_period('M')
    sales_trends = transactions_df.groupby('date')['amount'].sum()
    return sales_trends

def main():
    transactions_file_path = os.path.join('output', 'Cleaned_ml_transactions_outbox_non_relevant.csv')
    products_file_path = os.path.join('output', 'Cleaned_ml_items.csv')
    output_dir = 'Reports'
    csv_file = os.path.join(output_dir, 'ecommerce_analysis_report.csv')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    transactions_df = load_data(transactions_file_path)
    products_df = load_products(products_file_path)

    # Initialize a dictionary to hold the results for each metric
    results = {}

    # Calculate each metric and store in the results dictionary
    results['Repeat Purchase Rate (%)'] = repeat_purchase_rate(transactions_df)
    results['Average Order Value (AOV)'] = average_order_value(transactions_df)
    results['Customer Retention Rate (%)'] = customer_retention_rate(transactions_df)
    results['Customer Lifetime Value (CLV)'] = customer_lifetime_value(transactions_df)

    # Convert series to string summaries
    segmentation = customer_segmentation(transactions_df).to_dict()
    results['Customer Segmentation'] = str(segmentation)
    
    top_products = product_profitability(transactions_df)
    results['Top Performing Products'] = top_products['item_barcode'].tolist()
    
    cohort_counts = cohort_analysis(transactions_df)
    results['Cohort Analysis'] = str(cohort_counts.to_dict())

    sales_trends = sales_trend_forecasting(transactions_df)
    results['Sales Trends Forecast'] = str(sales_trends.to_dict())

    # Add summary
    results['Summary'] = (
        f"The repeat purchase rate is {results['Repeat Purchase Rate (%)']:.2f}%, "
        f"with an average order value of {results['Average Order Value (AOV)']:.2f} and "
        f"a customer retention rate of {results['Customer Retention Rate (%)']:.2f}%."
        f" The customer lifetime value is {results['Customer Lifetime Value (CLV)']:.2f}."
    )

    # Write results to CSV
    write_to_csv(results, csv_file)
    
    print("E-commerce analysis report generated successfully.")
    print(f"Report saved to: {csv_file}")

if __name__ == "__main__":
    main()
