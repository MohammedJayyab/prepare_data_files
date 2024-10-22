import pandas as pd
import os

def load_data(file_path):
    # Load transactions data with appropriate column names
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

def load_products(file_path):
    # Load products data with appropriate column names
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

def write_to_report(report_file, content):
    with open(report_file, 'a', encoding='utf-8') as f:
        f.write(content + '\n\n')

def repeat_purchase_rate(transactions_df, report_file):
    repeat_customers = transactions_df.groupby('customer_barcode')['invoice_id'].nunique().reset_index()
    
    if repeat_customers.shape[0] == 0:
        repeat_rate = 0.0
    else:
        repeat_rate = (repeat_customers[repeat_customers['invoice_id'] > 1].shape[0] / repeat_customers.shape[0]) * 100
    
    total_repeat_customers = repeat_customers[repeat_customers['invoice_id'] > 1].shape[0]
    explanation = "This measures the percentage of customers who made more than one order (RPR).\n"
    write_to_report(report_file, f"--- Repeat Purchase Rate (RPR) ---\nRepeat purchase rate: {repeat_rate:.2f}%\nTotal repeat customers: {total_repeat_customers}\n{explanation}")
 

def customer_segmentation(transactions_df, report_file):
    # Ensure 'timestamp' is parsed correctly with the right format (dd/MM/yyyy HH:mm:ss)
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    # Group by customer and calculate necessary metrics for segmentation
    customer_stats = transactions_df.groupby('customer_barcode').agg(
        order_count=('invoice_id', 'nunique'),
        total_spent=('amount', 'sum'),
        last_purchase=('timestamp', 'max'),
        first_purchase=('timestamp', 'min')
    ).reset_index()

    # Check if customer_stats has data
    if customer_stats.empty:
        print("No customer data found for segmentation.")
        write_to_report(report_file, "--- Customer Segmentation ---\nNo data available for segmentation.\n")
        return

    # Convert numeric columns to proper numeric types
    customer_stats['order_count'] = pd.to_numeric(customer_stats['order_count'], errors='coerce')
    customer_stats['total_spent'] = pd.to_numeric(customer_stats['total_spent'], errors='coerce')

    # Parse 'last_purchase' and 'first_purchase' to datetime
    customer_stats['last_purchase'] = pd.to_datetime(customer_stats['last_purchase'])
    customer_stats['first_purchase'] = pd.to_datetime(customer_stats['first_purchase'])

    # Adding more detailed customer segmentation based on order count
    customer_stats['order_segment'] = pd.cut(customer_stats['order_count'], 
                                             bins=[0, 1, 3, 5, 10, 20, float('inf')], 
                                             labels=['One-time', 'Very Low', 'Low', 'Medium', 'High', 'Very High'])

    # RFM Segmentation (Recency, Frequency, Monetary Value)
    # Recency: Days since the last purchase
    customer_stats['recency_days'] = (transactions_df['timestamp'].max() - customer_stats['last_purchase']).dt.days
    customer_stats['frequency'] = customer_stats['order_count']
    customer_stats['monetary'] = customer_stats['total_spent']

    # Convert recency_days, frequency, and monetary to numeric if needed
    customer_stats['recency_days'] = pd.to_numeric(customer_stats['recency_days'], errors='coerce')
    customer_stats['frequency'] = pd.to_numeric(customer_stats['frequency'], errors='coerce')
    customer_stats['monetary'] = pd.to_numeric(customer_stats['monetary'], errors='coerce')

    ### Z-score Normalization for Frequency and Monetary Values ###
    # Z-score for frequency and monetary
    customer_stats['frequency_zscore'] = (customer_stats['frequency'] - customer_stats['frequency'].mean()) / customer_stats['frequency'].std()
    customer_stats['monetary_zscore'] = (customer_stats['monetary'] - customer_stats['monetary'].mean()) / customer_stats['monetary'].std()

    # Frequency segmentation based on Z-scores
    customer_stats['frequency_segment'] = pd.cut(customer_stats['frequency_zscore'],
                                                 bins=[-float('inf'), -1, 0, 1, float('inf')],
                                                 labels=['Low', 'Average', 'High', 'Very High'])

    # Monetary segmentation based on Z-scores
    customer_stats['monetary_segment'] = pd.cut(customer_stats['monetary_zscore'],
                                                bins=[-float('inf'), -1, 0, 1, float('inf')],
                                                labels=['Low Spend', 'Average Spend', 'High Spend', 'Very High Spend'])

    # Recency segmentation (static based on days)
    recency_bins = [0, 30, 90, 180, 365, float('inf')]
    customer_stats['recency_segment'] = pd.cut(customer_stats['recency_days'],
                                               bins=recency_bins,
                                               labels=['Active', 'Warm', 'Cooling', 'Cold', 'Churned'])

    # Customer lifetime segmentation (based on time between first and last purchase)
    customer_stats['lifetime_days'] = (customer_stats['last_purchase'] - customer_stats['first_purchase']).dt.days
    lifetime_bins = [0, 30, 180, 365, float('inf')]  # Adjust bins based on data
    customer_stats['lifetime_segment'] = pd.cut(customer_stats['lifetime_days'],
                                                bins=lifetime_bins,
                                                labels=['New', 'Short-term', 'Medium-term', 'Long-term'])

    # Forecast segmentation using Z-scores for recency and frequency
    customer_stats['forecast_segment'] = pd.cut(customer_stats['frequency_zscore'] + customer_stats['recency_days'],
                                                bins=[-float('inf'), 100, 200, 300, float('inf')],
                                                labels=['Low Potential', 'Medium Potential', 'High Potential', 'Very High Potential'])

    # Prepare the segment distribution for reporting
    order_segment_dist = customer_stats['order_segment'].value_counts(normalize=True) * 100
    recency_segment_dist = customer_stats['recency_segment'].value_counts(normalize=True) * 100
    frequency_segment_dist = customer_stats['frequency_segment'].value_counts(normalize=True) * 100
    monetary_segment_dist = customer_stats['monetary_segment'].value_counts(normalize=True) * 100
    lifetime_segment_dist = customer_stats['lifetime_segment'].value_counts(normalize=True) * 100
    forecast_segment_dist = customer_stats['forecast_segment'].value_counts(normalize=True) * 100

    # Explanation of segmentation types
    explanation = (
        "Customer segmentation is based on multiple factors that help identify different types of customers.\n"
        "1. **Order Segmentation**: Groups customers based on the total number of orders placed.\n"
        "2. **RFM Segmentation**:\n"
        "   - **Recency**: Groups customers based on how recently they made a purchase.\n"
        "   - **Frequency**: Uses Z-scores to group customers based on how often they make purchases.\n"
        "   - **Monetary**: Uses Z-scores to group customers based on how much they spend.\n"
        "3. **Customer Lifetime Segmentation**: Groups customers based on how long they've been active from the first purchase to the last.\n"
        "4. **Forecast Segmentation**: Groups customers by potential future sales using frequency and recency Z-scores."
    )

    # Writing the results to the report
    write_to_report(report_file, "--- Customer Segmentation ---\n")
    write_to_report(report_file, f"--- Order Segmentation ---\n{order_segment_dist.to_string()}")
    write_to_report(report_file, f"--- Recency Segmentation ---\n{recency_segment_dist.to_string()}")
    write_to_report(report_file, f"--- Frequency Segmentation ---\n{frequency_segment_dist.to_string()}")
    write_to_report(report_file, f"--- Monetary Segmentation ---\n{monetary_segment_dist.to_string()}")
    write_to_report(report_file, f"--- Customer Lifetime Segmentation ---\n{lifetime_segment_dist.to_string()}")
    write_to_report(report_file, f"--- Forecast Segmentation ---\n{forecast_segment_dist.to_string()}")
    write_to_report(report_file, explanation)

def cohort_analysis(transactions_df, report_file):
 
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    transactions_df['cohort'] = transactions_df.groupby('customer_barcode')['timestamp'].transform('min').dt.to_period('M')
    cohort_counts = transactions_df.groupby(['cohort']).agg(customers=('customer_barcode', 'nunique')).to_string()
    explanation = "Cohort analysis groups customers by when they made their first purchase and tracks how many remain active (CA).\n"
    write_to_report(report_file, f"--- Cohort Analysis (CA) ---\n{cohort_counts}\n{explanation}")

def product_affinity(transactions_df, report_file):
    affinity = transactions_df.groupby('customer_barcode')['item_barcode'].apply(lambda x: x.unique()).reset_index()
    affinity['affinity_count'] = affinity['item_barcode'].apply(lambda x: len(x))
    product_affinity_stats = affinity['affinity_count'].value_counts().head(10).to_string()  # Limit to top 10
    top_affinity_counts = affinity['affinity_count'].value_counts().head(3).to_string()
    explanation = "This shows how many different products customers buy together (PA).\n"
    write_to_report(report_file, f"--- Product Affinity Analysis (PA) ---\n{product_affinity_stats}\nTop affinity counts: {top_affinity_counts}\n{explanation}")

def average_order_value(transactions_df, report_file):
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    transactions_df['unit_price'] = transactions_df['unit_price'].astype(float)
    transactions_df['amount'] = transactions_df['quantity'] * transactions_df['unit_price']
    aov = transactions_df.groupby('invoice_id')['amount'].sum().mean()
    total_revenue = transactions_df['amount'].sum()
    explanation = "Average Order Value (AOV) measures the average revenue per order.\n"
    write_to_report(report_file, f"--- Average Order Value (AOV) ---\nAverage order value: {aov:.2f}\nTotal revenue: {total_revenue:.2f}\n{explanation}")

def purchase_frequency(transactions_df, report_file):
    purchase_counts = transactions_df['customer_barcode'].value_counts().mean()
    median_purchase_frequency = transactions_df['customer_barcode'].value_counts().median()
    explanation = "Purchase frequency shows how often customers make a purchase (PF).\n"
    write_to_report(report_file, f"--- Purchase Frequency (PF) ---\nAverage purchase frequency: {purchase_counts:.2f}\nMedian purchase frequency: {median_purchase_frequency:.2f}\n{explanation}")

def customer_retention_rate(transactions_df, report_file):
    retained_customers = transactions_df.groupby('customer_barcode')['invoice_id'].nunique().reset_index()
    retention_rate = (retained_customers[retained_customers['invoice_id'] > 1].shape[0] / retained_customers.shape[0]) * 100
    total_retained_customers = retained_customers[retained_customers['invoice_id'] > 1].shape[0]
    explanation = "Customer Retention Rate (CRR) shows how many customers returned for another purchase.\n"
    write_to_report(report_file, f"--- Customer Retention Rate (CRR) ---\nRetention rate: {retention_rate:.2f}%\nTotal retained customers: {total_retained_customers}\n{explanation}")

def customer_lifetime_value(transactions_df, report_file):
    transactions_df['amount'] = transactions_df['quantity'].astype(int) * transactions_df['unit_price'].astype(float)
    clv = transactions_df.groupby('customer_barcode')['amount'].sum().mean()
    total_clv = transactions_df.groupby('customer_barcode')['amount'].sum().sum()
    explanation = "Customer Lifetime Value (CLV) estimates the average revenue per customer over their entire lifetime.\n"
    write_to_report(report_file, f"--- Customer Lifetime Value (CLV) ---\nAverage CLV: {clv:.2f}\nTotal CLV for all customers: {total_clv:.2f}\n{explanation}")

def product_profitability(transactions_df, products_df, report_file):
    transactions_df['amount'] = transactions_df['quantity'].astype(int) * transactions_df['unit_price'].astype(float)
    top_products = transactions_df.groupby('item_barcode')['amount'].sum().sort_values(ascending=False).head(10).reset_index()
    top_products = pd.merge(top_products, products_df[['barcode', 'en_full_description']], left_on='item_barcode', right_on='barcode')
    explanation = "This shows the top 10 best-selling products by revenue (PP).\n"
    write_to_report(report_file, f"--- Top Performing Products (PP) ---\n{top_products.to_string(index=False)}\n{explanation}")

def sales_trend_forecasting(transactions_df, report_file):
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    transactions_df['date'] = transactions_df['timestamp'].dt.to_period('M')
    sales_trends = transactions_df.groupby('date')['amount'].sum().to_string()
    total_sales_per_month = transactions_df.groupby('date')['amount'].sum().to_string()
    explanation = "This shows monthly sales trends and can be used for forecasting future sales (STF).\n"
    write_to_report(report_file, f"--- Sales Trend Forecasting (STF) ---\n{sales_trends}\nTotal sales per month: {total_sales_per_month}\n{explanation}")

def main():
    transactions_file_path = os.path.join('output', 'Cleaned_ml_transactions_outbox.csv')
    products_file_path = os.path.join('output', 'Cleaned_ml_items.csv')
    output_dir = 'Reports'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    report_file = os.path.join(output_dir, 'analysis_report_advanced.txt')
    open(report_file, 'w').close()  # Clear the file

    transactions_df = load_data(transactions_file_path)
    products_df = load_products(products_file_path)

    repeat_purchase_rate(transactions_df, report_file)
    customer_segmentation(transactions_df, report_file)
    cohort_analysis(transactions_df, report_file)
    product_affinity(transactions_df, report_file)
    average_order_value(transactions_df, report_file)
    purchase_frequency(transactions_df, report_file)
    customer_retention_rate(transactions_df, report_file)
    customer_lifetime_value(transactions_df, report_file)
    product_profitability(transactions_df, products_df, report_file)
    sales_trend_forecasting(transactions_df, report_file)

    print("Advanced analysis report generated successfully.")
    print(f"Report saved to: {report_file}")

if __name__ == "__main__":
    main()
