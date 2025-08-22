import pandas as pd
import sqlite3
import os

DB_FILE = 'food_wastage.db'

# Check if all required CSVs are present
required_csvs = ['providers_data.csv', 'receivers_data.csv', 'food_listings_data.csv', 'claims_data.csv']
for csv_file in required_csvs:
    if not os.path.exists(csv_file):
        print(f"Error: The required file '{csv_file}' was not found in the same directory.")
        print("Please ensure you have all four CSV files downloaded and placed here.")
        exit()

# Connect to SQLite database
conn = sqlite3.connect(DB_FILE)
print(f"Connected to database file '{DB_FILE}'")

# Load each CSV into a DataFrame and then into a SQL table
try:
    print("Loading data from CSVs into the database...")

    # Load and print info for providers data
    providers_df = pd.read_csv('providers_data.csv')
    print(f"Read {len(providers_df)} rows from providers_data.csv")
    providers_df.to_sql('Providers', conn, if_exists='replace', index=False)

    # Load and print info for receivers data
    receivers_df = pd.read_csv('receivers_data.csv')
    print(f"Read {len(receivers_df)} rows from receivers_data.csv")
    receivers_df.to_sql('Receivers', conn, if_exists='replace', index=False)

    # Load and print info for food listings data
    food_listings_df = pd.read_csv('food_listings_data.csv')
    print(f"Read {len(food_listings_df)} rows from food_listings_data.csv")
    food_listings_df['Expiry_Date'] = pd.to_datetime(food_listings_df['Expiry_Date'], errors='coerce')
    food_listings_df.to_sql('Food_Listings', conn, if_exists='replace', index=False)

    # Load and print info for claims data
    claims_df = pd.read_csv('claims_data.csv')
    print(f"Read {len(claims_df)} rows from claims_data.csv")
    claims_df['Timestamp'] = pd.to_datetime(claims_df['Timestamp'], errors='coerce')
    claims_df.to_sql('Claims', conn, if_exists='replace', index=False)
    
    conn.commit()
    print("\nData loaded and database created successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    conn.close()