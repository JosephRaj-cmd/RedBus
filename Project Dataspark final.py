import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import subprocess
import sys

# Install required packages
subprocess.check_call([sys.executable, "-m", "pip", "install", "pymysql"])

# Step 1: Define database credentials
USER = 'root'  # Replace with your MySQL username
PASSWORD = ''  # Replace with your MySQL password
HOST = 'localhost'
PORT = 3306
DATABASE = 'Dataspark'

# Step 2: Create connection without specifying the database to create it
connection_string_without_db = f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/'
engine_without_db = create_engine(connection_string_without_db)

# Step 3: Try to create the 'Dataspark' database if it doesn't exist using `text()` method
try:
    with engine_without_db.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DATABASE}"))
    print(f"Database '{DATABASE}' created or already exists.")
except OperationalError as e:
    print(f"Error creating database: {e}")
    sys.exit(1)

# Step 4: Update the connection string to include the 'Dataspark' database
connection_string_with_db = f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
engine_with_db = create_engine(connection_string_with_db)

# Step 5: Load and clean the CSV file
try:
    file_path = r'C:\Users\Joseph\Desktop\Deep_Cleaned_Merged_Cleaned_Data.csv'  # Correct file path
    df = pd.read_csv(file_path)
    print("File loaded successfully!")

    # Preview the data
    print("First 5 rows of the dataset:")
    print(df.head())

    # Replace unknown values
    df.replace(['Unknown', '?', 'None'], pd.NA, inplace=True)

    # Handle missing values for numerical columns
    numeric_columns = df.select_dtypes(include='number').columns
    df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())

    # Handle missing values for categorical columns
    categorical_columns = df.select_dtypes(include='object').columns
    for col in categorical_columns:
        df[col].fillna(df[col].mode()[0], inplace=True)

    # Save the cleaned data to a new CSV file
    output_file_path = r'C:\Users\Joseph\Desktop\Processed_Cleaned_Data.csv'
    df.to_csv(output_file_path, index=False)
    print(f"Cleaned data has been saved to {output_file_path}")

except FileNotFoundError as e:
    print(f"Error: {e}")
    sys.exit(1)

# Step 6: Upload the cleaned data to SQL
try:
    # Load the cleaned CSV into a pandas DataFrame
    df_cleaned = pd.read_csv(output_file_path)

    # Check the contents of the dataframe (optional)
    print(df_cleaned.head())

    # Define the table name
    table_name = 'table_dataspark'  # Replace with the desired table name

    # Upload data to SQL, creating the table
    df_cleaned.to_sql(table_name, engine_with_db, if_exists='replace', index=False)

    print(f"Table '{table_name}' created in the '{DATABASE}' database and data uploaded successfully!")

    # Step 7: Execute SQL Queries for Analysis

    # 1. Most Profitable Products
    query1 = """
    SELECT `Product Name`,
           SUM((`Unit Price USD` - `Unit Cost USD`) * `Quantity`) AS total_profit
    FROM table_dataspark
    GROUP BY `Product Name`
    ORDER BY total_profit DESC
    LIMIT 10;
    """
    
    # 2. Underperforming Stores
    query2 = """
    SELECT `StoreKey`,
           SUM(`Unit Price USD` * `Quantity`) AS total_sales
    FROM table_dataspark
    GROUP BY `StoreKey`
    ORDER BY total_sales ASC
    LIMIT 10;
    """
    
    # 3. Customer Demographic Impact on Purchasing Patterns
    query3 = """
    SELECT `Gender`, 
           AVG(`Unit Price USD` * `Quantity`) AS average_purchase_value
    FROM table_dataspark
    GROUP BY `Gender`;
    """

    # 4. Top 5 Product Categories
    query4 = """
    SELECT `Category`,
           SUM(`Unit Price USD` * `Quantity`) AS total_sales
    FROM table_dataspark
    GROUP BY `Category`
    ORDER BY total_sales DESC
    LIMIT 5;
    """

    # 5. Sales Trend Over Time
    query5 = """
    SELECT DATE(`Order Date`) AS order_date,
           SUM(`Unit Price USD` * `Quantity`) AS total_sales
    FROM table_dataspark
    GROUP BY order_date
    ORDER BY order_date;
    """
    
    # 6. Best Selling Products
    query6 = """
    SELECT `Product Name`,
           SUM(`Quantity`) AS total_units_sold
    FROM table_dataspark
    GROUP BY `Product Name`
    ORDER BY total_units_sold DESC
    LIMIT 10;
    """

    # 7. Customer Distribution by Country
    query7 = """
    SELECT `Country_x`,
           COUNT(DISTINCT `CustomerKey`) AS total_customers
    FROM table_dataspark
    GROUP BY `Country_x`
    ORDER BY total_customers DESC;
    """

    # 8. Average Order Value by State
    query8 = """
    SELECT `State_x`,
           AVG(`Unit Price USD` * `Quantity`) AS average_order_value
    FROM table_dataspark
    GROUP BY `State_x`
    ORDER BY average_order_value DESC;
    """
    
    # 9. Total Quantity Sold by Brand
    query9 = """
    SELECT `Brand`,
           SUM(`Quantity`) AS total_quantity
    FROM table_dataspark
    GROUP BY `Brand`
    ORDER BY total_quantity DESC;
    """
    
    # 10. Sales by Currency Code
    query10 = """
    SELECT `Currency Code`,
           SUM(`Unit Price USD` * `Quantity`) AS total_sales
    FROM table_dataspark
    GROUP BY `Currency Code`
    ORDER BY total_sales DESC;
    """

    # Execute the queries and print the results
    queries = [query1, query2, query3, query4, query5, query6, query7, query8, query9, query10]
    for i, query in enumerate(queries, start=1):
        try:
            result_df = pd.read_sql(query, engine_with_db)
            print(f"Results for Query {i}:\n", result_df)
        except Exception as e:
            print(f"An error occurred during SQL execution: {e}")

except Exception as e:
    print(f"An error occurred during the SQL upload: {e}")
    sys.exit(1)
