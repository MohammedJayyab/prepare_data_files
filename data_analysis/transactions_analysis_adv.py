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
    repeat_customers = transactions_df.groupby('customer_barcode')['item_barcode'].nunique().reset_index()
    repeat_rate = (repeat_customers[repeat_customers['item_barcode'] > 1].shape[0] / repeat_customers.shape[0]) * 100
    explanation = "This measures the percentage of customers who made more than one unique product purchase.\nExample: If 85% of your customers bought more than one product, you have a high repeat purchase rate."
    write_to_report(report_file, f"--- Repeat Purchase Rate ---\nRepeat purchase rate: {repeat_rate:.2f}%\n{explanation}")

def customer_segmentation(transactions_df, report_file):
    segmentation = transactions_df.groupby('customer_barcode').agg(purchase_count=('item_barcode', 'nunique')).reset_index()
    segmentation['segment'] = pd.cut(segmentation['purchase_count'], bins=[0, 1, 5, 10, float('inf')], labels=['One-time', 'Low', 'Medium', 'High'])
    segment_distribution = segmentation['segment'].value_counts().to_string()
    explanation = "This groups customers based on how many different products they purchased: 'One-time' customers only bought one product, while 'High' customers purchased many different products."
    write_to_report(report_file, f"--- Customer Segmentation ---\n{segment_distribution}\n{explanation}")

def cohort_analysis(transactions_df, report_file):
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    transactions_df['cohort'] = transactions_df.groupby('customer_barcode')['timestamp'].transform('min').dt.to_period('M')
    cohort_counts = transactions_df.groupby(['cohort']).agg(customers=('customer_barcode', 'nunique')).to_string()
    explanation = "Cohort analysis groups customers by when they made their first purchase and tracks how many are active."
    write_to_report(report_file, f"--- Cohort Analysis ---\n{cohort_counts}\n{explanation}")

def product_affinity(transactions_df, report_file):
    affinity = transactions_df.groupby('customer_barcode')['item_barcode'].apply(lambda x: x.unique()).reset_index()
    affinity['affinity_count'] = affinity['item_barcode'].apply(lambda x: len(x))
    product_affinity_stats = affinity['affinity_count'].value_counts().head(10).to_string()  # Limit to top 10
    explanation = "This shows how many different products customers buy together."
    write_to_report(report_file, f"--- Product Affinity Analysis (Top 10) ---\n{product_affinity_stats}\n{explanation}")

def average_order_value(transactions_df, report_file):
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    aov = transactions_df.groupby('customer_barcode')['quantity'].sum().mean()
    explanation = "Average Order Value (AOV) measures the average quantity of products purchased per customer."
    write_to_report(report_file, f"--- Average Order Value (AOV) ---\nAverage order value: {aov:.2f}\n{explanation}")

def purchase_frequency(transactions_df, report_file):
    purchase_counts = transactions_df['customer_barcode'].value_counts().mean()
    explanation = "Purchase frequency shows how often customers make a purchase."
    write_to_report(report_file, f"--- Purchase Frequency ---\nAverage purchase frequency: {purchase_counts:.2f}\n{explanation}")

def customer_retention_rate(transactions_df, report_file):
    purchase_counts = transactions_df.groupby('customer_barcode')['item_barcode'].count().reset_index()
    retained_customers = purchase_counts[purchase_counts['item_barcode'] > 1]
    retention_rate = (len(retained_customers) / len(purchase_counts)) * 100
    explanation = ("Customer Retention Rate (CRR) shows how many customers return after their initial purchase.")
    write_to_report(report_file, f"--- Customer Retention Rate (CRR) ---\nRetention rate: {retention_rate:.2f}%\n{explanation}")

def time_to_first_purchase(transactions_df, report_file):
    first_purchase_time = transactions_df.groupby('customer_barcode')['timestamp'].min().reset_index()
    first_purchase_time['days_to_first'] = (first_purchase_time['timestamp'] - transactions_df['timestamp'].min()).dt.days
    avg_time_to_first = first_purchase_time['days_to_first'].mean()
    explanation = "This measures the average time it takes for a customer to make their first purchase."
    write_to_report(report_file, f"--- Time-to-First Purchase ---\nAverage time to first purchase: {avg_time_to_first:.2f} days\n{explanation}")

def average_time_between_purchases(transactions_df, report_file):
    transactions_df = transactions_df.sort_values(by=['customer_barcode', 'timestamp'])
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    transactions_df['time_diff'] = transactions_df.groupby('customer_barcode')['timestamp'].diff().dt.days
    avg_time_between_purchases = transactions_df['time_diff'].mean()
    explanation = ("This measures the average time between consecutive purchases for each customer.")
    write_to_report(report_file, f"--- Average Time Between Purchases ---\nAverage time between purchases: {avg_time_between_purchases:.2f} days\n{explanation}")

def product_sales_trends(transactions_df, report_file):
    transactions_df['date'] = transactions_df['timestamp'].dt.to_period('M')
    sales_trends = transactions_df.groupby('date')['quantity'].sum().to_string()
    explanation = "This shows monthly sales trends by the total quantity of products sold."
    write_to_report(report_file, f"--- Product Sales Trends ---\n{sales_trends}\n{explanation}")

def top_performing_products(transactions_df, products_df, report_file):
    top_products = transactions_df.groupby('item_barcode')['quantity'].sum().sort_values(ascending=False).head(10).reset_index()
    top_products = pd.merge(top_products, products_df[['barcode', 'en_full_description']], left_on='item_barcode', right_on='barcode')
    explanation = "This shows the top 10 best-selling products by the total quantity sold."
    write_to_report(report_file, f"--- Top Performing Products ---\n{top_products.to_string(index=False)}\n{explanation}")

def main():
    transactions_file_path = os.path.join('output', 'Cleaned_ml_transactions_outbox_non_relevant.csv')
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
    time_to_first_purchase(transactions_df, report_file)
    average_time_between_purchases(transactions_df, report_file)
    product_sales_trends(transactions_df, report_file)
    top_performing_products(transactions_df, products_df, report_file)
    print("Advanced analysis report generated successfully.")
    print(f"Report saved to: {report_file}")

if __name__ == "__main__":
    main()
