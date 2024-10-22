import pandas as pd
import os

def load_data(file_path):
    print("Loading transactions data...")
    # Force all columns to be treated as strings
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

def load_products(file_path):
    print("Loading products data...")
    # Force all columns to be treated as strings
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

def merge_transactions_with_products(transactions_df, products_df):
    print("Merging transactions with product data...")
    
    # Ensure 'item_barcode' in transactions_df and 'barcode' in products_df are strings and clean any potential whitespace
    transactions_df['item_barcode'] = transactions_df['item_barcode'].str.strip()
    products_df['barcode'] = products_df['barcode'].str.strip()
    
    # Merge the DataFrames on 'item_barcode' and 'barcode'
    merged_df = pd.merge(transactions_df, products_df[['barcode', 'category_level1', 'category_level2', 'category_level3', 'category_level4']], 
                         left_on='item_barcode', right_on='barcode', how='left')
    
    # Check for missing category values after merge
    missing_categories = merged_df[['category_level1', 'category_level2', 'category_level3', 'category_level4']].isnull().any(axis=1).sum()
    
    if missing_categories > 0:
        print(f"Warning: {missing_categories} transactions do not have a matching category.")
        print("Sample of missing item_barcode values:")
        print(merged_df[merged_df['category_level1'].isnull()]['item_barcode'].head(10))  # Display first 10 missing barcodes
        print(merged_df[merged_df['category_level2'].isnull()]['item_barcode'].head(10))  # Display first 10 missing barcodes
        print(merged_df[merged_df['category_level3'].isnull()]['item_barcode'].head(10))  # Display first 10 missing barcodes
        print(merged_df[merged_df['category_level4'].isnull()]['item_barcode'].head(10))  # Display first 10 missing barcodes


    
    return merged_df

def write_to_report(report_file, content):
    # Open the file with UTF-8 encoding to support Arabic characters
    with open(report_file, 'a', encoding='utf-8') as f:
        f.write(content + '\n\n')  # Add spaces between sections

def get_basic_statistics(transactions_df, report_file):
    total_customers = transactions_df['customer_barcode'].nunique()
    total_products = transactions_df['item_barcode'].nunique()
    total_transactions = len(transactions_df)
    
    output = (f"--- Basic Statistics ---\n"
              f"Total unique customers: {total_customers}\n"
              f"Total unique products: {total_products}\n"
              f"Total transactions: {total_transactions}\n")
    
    write_to_report(report_file, output)

def calculate_average_transactions_per_customer(transactions_df, report_file):
    total_transactions = len(transactions_df)
    total_customers = transactions_df['customer_barcode'].nunique()
    avg_transactions_per_customer = total_transactions / total_customers
    
    output = (f"Average number of transactions per customer: {avg_transactions_per_customer:.2f}\n")
    write_to_report(report_file, output)

def calculate_mode_transactions_per_customer(transactions_df, report_file):
    customer_purchase_counts = transactions_df.groupby('customer_barcode')['item_barcode'].count().reset_index()
    customer_purchase_counts.columns = ['customer_barcode', 'total_purchases']
    
    mode_transactions = customer_purchase_counts['total_purchases'].mode()[0]
    num_customers_with_mode_transactions = customer_purchase_counts[customer_purchase_counts['total_purchases'] == mode_transactions].shape[0]
    
    total_customers = customer_purchase_counts.shape[0]
    percentage_of_mode_customers = (num_customers_with_mode_transactions / total_customers) * 100
    
    output = (f"--- Mode of Transactions per Customer ---\n"
              f"The most common number of transactions (mode) is: {mode_transactions}\n"
              f"Number of customers with {mode_transactions} transactions: {num_customers_with_mode_transactions}\n"
              f"Percentage of customers with {mode_transactions} transactions: {percentage_of_mode_customers:.2f}%\n")
    
    write_to_report(report_file, output)

def analyze_customer_purchases(transactions_df, report_file):
    customer_product_counts = transactions_df.groupby('customer_barcode')['item_barcode'].nunique()
    multiple_purchases_count = customer_product_counts[customer_product_counts > 1].count()
    
    output = (f"--- Customer Purchases ---\n"
              f"Customers with more than 1 unique product purchase: {multiple_purchases_count}\n")
    
    write_to_report(report_file, output)

def analyze_top_customers_and_products(transactions_df, report_file):
    top_customers = transactions_df['customer_barcode'].value_counts().head(5)
    
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    top_products = transactions_df.groupby('item_barcode')['quantity'].sum().sort_values(ascending=False).head(5)
    
    output = (f"--- Top Customers and Products ---\n"
              f"Top 5 customers by number of purchases:\n{top_customers.to_string(index=True)}\n\n"
              f"Top 5 products by quantity sold:\n{top_products.to_string(index=True)}\n")
    
    write_to_report(report_file, output)

def analyze_customers_with_less_than_5_purchases(transactions_df, report_file):
    customer_purchase_counts = transactions_df.groupby('customer_barcode')['item_barcode'].count().reset_index()
    customer_purchase_counts.columns = ['customer_barcode', 'total_purchases']
    
    customers_less_than_5 = customer_purchase_counts[customer_purchase_counts['total_purchases'] < 5]
    count_customers_less_than_5 = len(customers_less_than_5)
    total_transactions_less_than_5 = customers_less_than_5['total_purchases'].sum()
    
    if len(customers_less_than_5) > 20:
        customers_less_than_5 = customers_less_than_5.head(20)
    
    output = (f"--- Customers with Less Than 5 Purchases ---\n"
              f"Total customers with less than 5 purchases: {count_customers_less_than_5}\n"
              f"Total number of transactions for these customers: {total_transactions_less_than_5}\n"
              f"\nTop 20 Customers with Less Than 5 Purchases:\n"
              f"{customers_less_than_5.to_string(index=False)}\n")
    
    write_to_report(report_file, output)

def analyze_product_popularity(transactions_df, report_file, top_n=10):
    # Parse timestamp using the correct format
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    
    # Aggregate product quantities sold over time (group by day and product)
    product_popularity_over_time = transactions_df.groupby([pd.Grouper(key='timestamp', freq='D'), 'item_barcode']).agg({'quantity': 'sum'}).reset_index()
    
    # Aggregate total quantities for each product
    total_quantity_per_product = product_popularity_over_time.groupby('item_barcode')['quantity'].sum().sort_values(ascending=False)
    
    # Get the top n most popular products
    top_products = total_quantity_per_product.head(top_n).index
    
    # Filter the original data to include only the top n products
    filtered_popularity = product_popularity_over_time[product_popularity_over_time['item_barcode'].isin(top_products)]
    
    # Pivot the data for display purposes (products as columns, dates as rows)
    filtered_popularity_pivot = filtered_popularity.pivot(index='timestamp', columns='item_barcode', values='quantity').fillna(0)
    
    # Now flip columns to rows (transpose) for better readability
    transposed_popularity = filtered_popularity_pivot.T
    
    # Limit the output to 10 rows for clearer display
    output = f"--- Product Popularity (Top {top_n} Products, Transposed) ---\n{transposed_popularity.head(10).to_string(index=True)}\n"
    
    write_to_report(report_file, output)

def analyze_sales_trends(transactions_df, report_file):
    # Parse timestamp using the correct format
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    
    # Group by day and calculate total quantity sold each day
    daily_sales = transactions_df.groupby(pd.Grouper(key='timestamp', freq='D')).agg({'quantity': 'sum'})
    
    output = f"--- Sales Trends Over Time ---\n{daily_sales.to_string(index=True)}\n"
    
    write_to_report(report_file, output)

def analyze_sales_trends(transactions_df, report_file):
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    daily_sales = transactions_df.groupby(pd.Grouper(key='timestamp', freq='D')).agg({'quantity': 'sum'})
    
    output = f"--- Sales Trends Over Time ---\n{daily_sales.to_string(index=True)}\n"
    
    write_to_report(report_file, output)

def analyze_sales_by_category(transactions_df, report_file):
    # Assuming categories are already in the merged DataFrame
    if 'category_level1' in transactions_df.columns:
        transactions_with_category = transactions_df[transactions_df['category_level1'].notnull()]
        
        if transactions_with_category.empty:
            output = "--- Sales by Category ---\nNo sales data available by category (all categories missing).\n"
        else:
            sales_by_category = transactions_with_category.groupby('category_level1')['quantity'].sum().sort_values(ascending=False).head(10)
            output = f"--- Sales by Category (Top 10) ---\n{sales_by_category.to_string(index=True)}\n"
    else:
        output = "--- Sales by Category ---\nCategory information not available.\n"
    
    write_to_report(report_file, output)

def main():
    transactions_file_path = os.path.join('output', 'Cleaned_ml_transactions_outbox_non_relevant.csv')
    products_file_path = os.path.join('output', 'Cleaned_ml_items.csv')
    output_dir = 'Reports'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    report_file = os.path.join(output_dir, 'analysis_report.txt')
    
    # Clear previous report content
    open(report_file, 'w').close()
    
    # Load data
    transactions_df = load_data(transactions_file_path)
    products_df = load_products(products_file_path)
    
    # Merge transactions with product data
    transactions_with_category = merge_transactions_with_products(transactions_df, products_df)
    
    # Perform analysis and write output to the report
    get_basic_statistics(transactions_with_category, report_file)
    calculate_average_transactions_per_customer(transactions_with_category, report_file)
    calculate_mode_transactions_per_customer(transactions_with_category, report_file)
    analyze_customer_purchases(transactions_with_category, report_file)
    analyze_top_customers_and_products(transactions_with_category, report_file)
    analyze_customers_with_less_than_5_purchases(transactions_with_category, report_file)
    analyze_product_popularity(transactions_with_category, report_file)
    analyze_sales_trends(transactions_with_category, report_file)
    analyze_sales_by_category(transactions_with_category, report_file)

    print("Analysis report generated successfully.")
    print(f"Output saved to: {report_file}")

if __name__ == "__main__":
    main()
