Customer Feature Table - README
This document explains the various features that are generated in the Customer Feature Table. Each feature provides insights into customer behavior, purchase patterns, and customer segmentation.

Features

1. customer_barcode
   Description: Unique identifier for each customer in the dataset.
   Example: 1234567890
2. first_purchase
   Description: The date of the customer’s first purchase.
   Example: 2024-08-01
3. last_purchase
   Description: The date of the customer’s last purchase.
   Example: 2024-10-07
4. customer_lifetime
   Description: The number of days between the customer's first and last purchase.
   Calculation: last_purchase - first_purchase.
   Example: If first_purchase is 2024-08-01 and last_purchase is 2024-10-07, the customer_lifetime will be 67 days.
5. total_orders
   Description: The total number of unique orders placed by the customer.
   Example: If the customer placed 5 orders, the value will be 5.
6. order_segment
   Description: Segments customers based on the total number of orders placed.
   Segments:
   One-time: 1 order
   Low: 2–3 orders
   Medium: 4–5 orders
   High: 6–10 orders
   Very High: More than 10 orders
   Example: A customer with 5 orders will be in the Medium segment.
7. frequency
   Description: The total number of orders placed by the customer (same as total_orders).
   Example: If the customer placed 5 orders, the value will be 5.
8. frequency_zscore
   Description: The Z-score (standardized score) for the frequency of orders placed by the customer.
   Calculation: (Customer's frequency - Mean frequency) / Standard deviation of frequencies.
   Example: A frequency_zscore of 1.2 indicates the customer orders more frequently than average.
9. frequency_segment
   Description: Segments customers based on their Z-score for order frequency.
   Segments:
   Low: Z-score < -1
   Average: Z-score between -1 and 0
   High: Z-score between 0 and 1
   Very High: Z-score > 1
   Example: A customer with a Z-score of 0.8 will be in the High segment.
10. is_high_frequency_customer
    Description: Indicates whether the customer is a high-frequency customer (1 for high frequency, 0 for not).
    Example: A 1 means the customer is high-frequency.
11. total_items
    Description: The total number of items purchased by the customer.
    Example: If the customer bought 100 items across all orders, the value will be 100.
12. unique_products_purchased
    Description: The total number of unique products purchased by the customer.
    Example: If the customer purchased 50 different products, the value will be 50.
13. average_quantity_per_order
    Description: The average number of items purchased per order.
    Calculation: Total items purchased / Total orders placed.
    Example: If total_items is 100 and total_orders is 5, the average_quantity_per_order will be 20.
14. total_spent
    Description: The total amount of money spent by the customer across all orders.
    Example: If the customer spent $1,000 in total, the value will be 1000.00.
15. average_order_value
    Description: The average amount spent per order.
    Calculation: Total spent / Total orders placed.
    Example: If total_spent is $1,000 and total_orders is 5, the average_order_value will be $200.
16. average_spend_per_item
    Description: The average amount spent per item.
    Calculation: Total spent / Total items purchased.
    Example: If total_spent is $1,000 and total_items is 100, the average_spend_per_item will be $10.
17. monetary
    Description: The total money spent by the customer (same as total_spent).
    Example: If the customer spent $1,000 in total, the value will be 1000.00.
18. monetary_zscore
    Description: The Z-score (standardized score) for the customer’s total spending.
    Calculation: (Customer's spending - Mean spending) / Standard deviation of spending.
    Example: A monetary_zscore of 1.5 indicates the customer spends more than average.
19. monetary_segment
    Description: Segments customers based on their Z-score for spending.
    Segments:
    Low Spend: Z-score < -1
    Average Spend: Z-score between -1 and 0
    High Spend: Z-score between 0 and 1
    Very High Spend: Z-score > 1
    Example: A customer with a Z-score of 1.2 will be in the Very High Spend segment.
20. is_high_value_customer
    Description: Indicates whether the customer is a high-value customer based on spending (1 for high value, 0 for not).
    Example: A 1 means the customer is a high-value customer.
21. recency_days
    Description: The number of days since the customer’s last purchase.
    Calculation: Difference between today’s date and last_purchase.
    Example: If today is 2024-10-10 and the last_purchase was on 2024-10-07, the recency_days will be 3.
22. avg_time_between_purchases
    Description: The average number of days between each purchase made by the customer.
    Calculation: Average time between consecutive purchases.
    Example: If the average time between purchases is 10 days, the value will be 10.
23. most_frequent_day
    Description: The day of the week the customer most frequently makes purchases.
    Example: Monday indicates that the customer most often buys on Mondays.
24. most_frequent_time
    Description: The hour of the day the customer most frequently makes purchases.
    Example: 14 (meaning 2:00 PM).
25. monthly_spending_mean
    Description: The average monthly amount spent by the customer.
    Example: If the customer spent an average of $300 per month, the value will be 300.00.
26. monthly_spending_std
    Description: The standard deviation of the customer’s monthly spending, indicating how much their spending varies from month to month.
    Example: If the customer's spending varies by $50 per month, the monthly_spending_std will be 50.00.
27. spending_trend
    Description: A numerical representation of the trend in the customer's spending over time.
    Calculation: Positive values indicate increasing spending over time, negative values indicate decreasing spending, and values near zero indicate stable spending.
    Example: A spending_trend of 0.5 indicates increasing spending, while -0.5 indicates decreasing spending.
28. purchase_trend
    Description: Indicates whether the number of purchases made by the customer is increasing, decreasing, or stable over time.
    Values:
    Upward: Purchases are increasing.
    Downward: Purchases are decreasing.
    Stable: Purchases remain stable over time.
    Example: A customer with an Upward trend is increasing their number of purchases over time.

```bash
customer_barcode:            1234567890
first_purchase:              2024-08-01
last_purchase:               2024-10-07
customer_lifetime:           67
total_orders:                5
order_segment:               Medium
frequency:                   5
frequency_zscore:            0.
```
