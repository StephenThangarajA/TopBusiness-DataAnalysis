import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mysql.connector

# Step 1: Load the dataset
df = pd.read_csv("Top_Business_CSV.csv")

# Step 2: Standardize column names to snake_case
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

# Rename 'name' to 'company_name' to match MySQL schema
df.rename(columns={'name': 'company_name'}, inplace=True)

# Display overview
print(df.head())
print(df.columns)
print(df.info())
print(df.describe(include='all'))

# Step 3: Clean the 'sales' column
df['sales_clean'] = (
    df['sales']
    .astype(str)
    .str.strip()
    .str.replace('$', '', regex=False)
    .str.replace(' ', '', regex=False)
    .str.replace(',', '', regex=False)
    .str.replace('M', 'e6', regex=False)
    .str.replace('B', 'e9', regex=False)
)
df['sales_clean'] = pd.to_numeric(df['sales_clean'], errors='coerce')

# Show cleaned values
print("\nSample cleaned sales values:")
print(df[['sales', 'sales_clean']].head(10))

# Step 4: Drop rows with missing essential data
df.dropna(subset=['country', 'sales_clean'], inplace=True)

# Step 5: Create a new column for sales in billions
df['sales_billion'] = np.where(df['sales_clean'] > 1e9, df['sales_clean'] / 1e9, df['sales_clean'] / 1e6)

# Step 6: Basic sales statistics
sales_array = df['sales_clean'].dropna().values
print("\nSales Statistics")
print(f"Mean: ${np.mean(sales_array):,.2f}")
print(f"Max: ${np.max(sales_array):,.2f}")
print(f"Min: ${np.min(sales_array):,.2f}")
print(f"Std Dev: ${np.std(sales_array):,.2f}")

# Step 7: Group by country and calculate total sales
country_sales = df.groupby('country')['sales_clean'].sum().sort_values(ascending=False)

# Step 8: Export cleaned and grouped data
df.to_csv("Cleaned_Top_Business.csv", index=False)
country_sales.to_csv("Sales_By_Country.csv")

# Step 9: Plot top 10 countries by sales
top10 = country_sales.head(10)

if top10.empty:
    print("Top 10 country sales data is empty. Please check data cleaning.")
else:
    top10.plot(
        kind='bar',
        figsize=(10, 5),
        title='Top 10 Countries by Total Sales',
        color='skyblue'
    )
    plt.ylabel('Total Sales ($)')
    plt.xlabel('Country')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.grid(axis='y')
    plt.show()

# Step 10: Insert Cleaned Data into MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Stephen14$',
        database='business_data'
    )
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO top_business 
        (company_name, country, sales, sales_clean, market_value, profit)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row.get('company_name', ''),
            row.get('country', ''),
            row.get('sales', ''),
            float(row.get('sales_clean', 0)),
            row.get('market_value', ''),
            row.get('profit', '')
        ))

    connection.commit()
    cursor.close()
    connection.close()
    print("Data inserted into MySQL successfully.")

except mysql.connector.Error as err:
    print("MySQL Error:", err)