import pandas as pd
import sqlite3
from pathlib import Path

# Extract Data
def extract_data(file_path, password):
    df = pd.read_excel(file_path, password=password)
    return df

# Transform Data
def transform_data(df_a, df_b):
    # Combine data from both regions
    combined_data = pd.concat([df_a, df_b], ignore_index=True)

    # Add total_sales column
    combined_data['total_sales'] = combined_data['QuantityOrdered'] * combined_data['ItemPrice']

    # Add region column
    combined_data['region'] = ['A'] * len(df_a) + ['B'] * len(df_b)

    # Remove duplicate entries based on OrderId
    combined_data.drop_duplicates(subset='OrderId', keep='first', inplace=True)

    return combined_data

# Load Data
def load_data(transformed_data):
    conn = sqlite3.connect('sales_data.db')
    transformed_data.to_sql('sales_data', conn, if_exists='replace', index=False)
    conn.close()

# Main function
def main():
    # Extract data
    df_a = extract_data('order_region_a.xlsx', 'order_region_a')
    df_b = extract_data('order_region_b.xlsx', 'order_region_b')

    # Transform data
    transformed_data = transform_data(df_a, df_b)

    # Load data
    load_data(transformed_data)

    # Write SQL queries
    conn = sqlite3.connect('sales_data.db')
    cursor = conn.cursor()

    # Count the total number of records
    cursor.execute("SELECT COUNT(*) FROM sales_data;")
    total_records = cursor.fetchone()[0]
    print(f"Total number of records: {total_records}")

    # Find the total sales amount by region
    cursor.execute("SELECT region, SUM(total_sales) FROM sales_data GROUP BY region;")
    total_sales_by_region = cursor.fetchall()
    for region, total_sales in total_sales_by_region:
        print(f"Total sales for region {region}: {total_sales}")

    # Find the average sales amount per transaction
    cursor.execute("SELECT AVG(total_sales) FROM sales_data;")
    avg_sales_per_transaction = cursor.fetchone()[0]
    print(f"Average sales amount per transaction: {avg_sales_per_transaction}")

    # Ensure there are no duplicate id values
    cursor.execute("SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1;")
    duplicate_ids = cursor.fetchall()
    if duplicate_ids:
        print("Duplicate OrderId values found:")
        for order_id, count in duplicate_ids:
            print(f"OrderId: {order_id}, Count: {count}")
    else:
        print("No duplicate OrderId values found.")

    conn.close()

if __name__ == "__main__":
    main()
