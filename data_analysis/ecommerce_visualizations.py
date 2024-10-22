import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Function to load data (similar to your previous analysis script)
def load_data(file_path):
    return pd.read_csv(file_path, dtype=str, quotechar='"', quoting=2)

# Plot 1: Repeat Purchase Rate Histogram
def plot_repeat_purchase_rate(transactions_df, output_dir):
    repeat_customers = transactions_df.groupby('customer_barcode')['invoice_id'].nunique().reset_index()
    repeat_customers['repeat_flag'] = repeat_customers['invoice_id'] > 1

    sns.countplot(x='repeat_flag', data=repeat_customers, palette='Set2')
    plt.title("Repeat Purchase Rate Distribution")
    plt.xlabel("Customer Repeat Purchase")
    plt.ylabel("Number of Customers")
    plt.xticks([0, 1], ['One-time', 'Repeat'])
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'repeat_purchase_rate.png'))
    plt.close()
    print("Plot saved: repeat_purchase_rate.png")

# Plot 2: Customer Segmentation Pie Chart
def plot_customer_segmentation(transactions_df, output_dir):
    segmentation = transactions_df.groupby('customer_barcode').agg(order_count=('invoice_id', 'nunique')).reset_index()
    segmentation['segment'] = pd.cut(segmentation['order_count'], bins=[0, 1, 5, 10, float('inf')], labels=['One-time', 'Low', 'Medium', 'High'])
    
    segment_distribution = segmentation['segment'].value_counts(normalize=True) * 100
    segment_distribution.plot.pie(autopct='%1.1f%%', colors=sns.color_palette('Set3'))
    plt.title("Customer Segmentation by Order Count")
    plt.ylabel('')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'customer_segmentation.png'))
    plt.close()
    print("Plot saved: customer_segmentation.png")

# Plot 3: Average Order Value (AOV) Histogram
def plot_average_order_value(transactions_df, output_dir):
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    transactions_df['unit_price'] = transactions_df['unit_price'].astype(float)
    transactions_df['amount'] = transactions_df['quantity'] * transactions_df['unit_price']
    
    aov_df = transactions_df.groupby('invoice_id')['amount'].sum().reset_index()
    sns.histplot(aov_df['amount'], kde=True, color='blue', bins=20)
    plt.title("Average Order Value Distribution")
    plt.xlabel("Order Amount (Total)")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'average_order_value.png'))
    plt.close()
    print("Plot saved: average_order_value.png")

# Plot 4: Product Affinity Heatmap (Correlations Between Products)
def plot_product_affinity(transactions_df, output_dir):
    affinity = transactions_df.groupby('customer_barcode')['item_barcode'].apply(lambda x: list(x)).reset_index()
    all_combinations = pd.DataFrame(
        [(a, b) for items in affinity['item_barcode'] for a in items for b in items if a != b], 
        columns=['item_a', 'item_b']
    )
    affinity_matrix = pd.crosstab(all_combinations['item_a'], all_combinations['item_b'])

    plt.figure(figsize=(12, 8))
    sns.heatmap(affinity_matrix, cmap='coolwarm', cbar=True)
    plt.title("Product Affinity Heatmap")
    plt.xlabel("Item B")
    plt.ylabel("Item A")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'product_affinity_heatmap.png'))
    plt.close()
    print("Plot saved: product_affinity_heatmap.png")

# Plot 5: Sales Trends Line Plot (Monthly Revenue)
def plot_sales_trends(transactions_df, output_dir):
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    transactions_df['date'] = transactions_df['timestamp'].dt.to_period('M')
    monthly_sales = transactions_df.groupby('date')['amount'].sum().reset_index()

    plt.figure(figsize=(10, 6))
    sns.lineplot(x=monthly_sales['date'].astype(str), y=monthly_sales['amount'], marker='o')
    plt.xticks(rotation=45)
    plt.title("Monthly Sales Trends")
    plt.xlabel("Month")
    plt.ylabel("Total Sales (Revenue)")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'sales_trends.png'))
    plt.close()
    print("Plot saved: sales_trends.png")

# New Plot 6: Sales Frequency Over Time
def plot_sales_frequency(transactions_df, output_dir):
    # Convert timestamp to datetime and group by day to count sales frequency
    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
    sales_frequency = transactions_df.groupby(transactions_df['timestamp'].dt.date).size()

    plt.figure(figsize=(10, 6))
    sales_frequency.plot(kind='line')
    plt.title("Sales Frequency Over Time")
    plt.xlabel("Date")
    plt.ylabel("Number of Sales")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'sales_frequency.png'))
    plt.close()
    print("Plot saved: sales_frequency.png")

# New Plot 7: Customer Distribution by Total Quantity Purchased
def plot_customer_distribution_by_quantity(transactions_df, output_dir):
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    quantity_per_customer = transactions_df.groupby('customer_barcode')['quantity'].sum().reset_index()

    plt.figure(figsize=(10, 6))
    sns.histplot(quantity_per_customer['quantity'], bins=20, kde=True, color='purple')
    plt.title("Customer Distribution by Total Quantity Purchased")
    plt.xlabel("Total Quantity Purchased")
    plt.ylabel("Number of Customers")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'customer_distribution_by_quantity.png'))
    plt.close()
    print("Plot saved: customer_distribution_by_quantity.png")

# New Plot 8: Customer Distribution by Total Money Spent
def plot_customer_distribution_by_spend(transactions_df, output_dir):
    transactions_df['quantity'] = transactions_df['quantity'].astype(int)
    transactions_df['unit_price'] = transactions_df['unit_price'].astype(float)
    transactions_df['amount'] = transactions_df['quantity'] * transactions_df['unit_price']
    
    spend_per_customer = transactions_df.groupby('customer_barcode')['amount'].sum().reset_index()

    plt.figure(figsize=(10, 6))
    sns.histplot(spend_per_customer['amount'], bins=20, kde=True, color='green')
    plt.title("Customer Distribution by Total Money Spent")
    plt.xlabel("Total Amount Spent")
    plt.ylabel("Number of Customers")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'customer_distribution_by_spend.png'))
    plt.close()
    print("Plot saved: customer_distribution_by_spend.png")

# Main function to generate all plots
def main():
    transactions_file_path = os.path.join('output', 'Cleaned_ml_transactions_outbox.csv')
    output_dir = 'Reports/Visualizations'

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load the cleaned transaction data
    transactions_df = load_data(transactions_file_path)

    # Generate plots
    plot_repeat_purchase_rate(transactions_df, output_dir)
    plot_customer_segmentation(transactions_df, output_dir)
    plot_average_order_value(transactions_df, output_dir)
    plot_product_affinity(transactions_df, output_dir)
    plot_sales_trends(transactions_df, output_dir)
    plot_sales_frequency(transactions_df, output_dir)  # New plot for sales frequency over time
    plot_customer_distribution_by_quantity(transactions_df, output_dir)  # New plot for quantity
    plot_customer_distribution_by_spend(transactions_df, output_dir)     # New plot for spend

    print("All plots generated successfully.")

if __name__ == "__main__":
    main()
