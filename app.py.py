import streamlit as st
import pandas as pd
import sqlite3
import os

st.set_page_config(page_title="Food Waste Management", layout="wide")

DB_FILE = 'food_wastage.db'

# --- 1. Database Creation & Data Loading ---
# This function runs once on every app start to create the database
@st.cache_resource
def setup_database():
    try:
        required_csvs = ['providers_data.csv', 'receivers_data.csv', 'food_listings_data.csv', 'claims_data.csv']
        for csv_file in required_csvs:
            if not os.path.exists(csv_file):
                st.error(f"Required CSV file '{csv_file}' not found. Please ensure all CSVs are in the project folder.")
                return None
        
        conn = sqlite3.connect(DB_FILE)
        
        providers_df = pd.read_csv('providers_data.csv')
        receivers_df = pd.read_csv('receivers_data.csv')
        food_listings_df = pd.read_csv('food_listings_data.csv')
        claims_df = pd.read_csv('claims_data.csv')

        food_listings_df['Expiry_Date'] = pd.to_datetime(food_listings_df['Expiry_Date'], errors='coerce')
        claims_df['Timestamp'] = pd.to_datetime(claims_df['Timestamp'], errors='coerce')
        
        providers_df.to_sql('Providers', conn, if_exists='replace', index=False)
        receivers_df.to_sql('Receivers', conn, if_exists='replace', index=False)
        food_listings_df.to_sql('Food_Listings', conn, if_exists='replace', index=False)
        claims_df.to_sql('Claims', conn, if_exists='replace', index=False)
        conn.commit()
        
        conn.close()
        return True # Return True on success
    except Exception as e:
        st.error(f"Error during database setup: {e}")
        return False # Return False on failure

# --- 2. Database Connection & Query Functions ---
def get_db_connection():
    if not os.path.exists(DB_FILE):
        if not setup_database():
            return None
    try:
        conn = sqlite3.connect(DB_FILE)
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

@st.cache_data
def run_query(query):
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error executing query: {e}")
            conn.close()
            return pd.DataFrame()
    return pd.DataFrame()

# --- 3. Page Functions ---
def show_dashboard_page(selected_city):
    st.header("📊 Main Dashboard")

    st.subheader("10. Claims Status Distribution (Percentage)")
    st.info("This shows overall claims status and is not filtered by city selection.")
    q10 = run_query('''
    SELECT Status,
            ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM Claims), 2) AS Percentage
    FROM Claims
    GROUP BY Status;
    ''')
    if not q10.empty:
        st.dataframe(q10, use_container_width=True)
        st.bar_chart(q10.set_index("Status"))
    else:
        st.info("No claims status data available.")

    st.subheader("6. City with Highest Food Listings")
    st.info("This metric shows the city with the highest listings overall, not filtered by selection.")
    q6 = run_query('''
    SELECT Location AS City, COUNT(*) AS Listing_Count
    FROM Food_Listings
    GROUP BY Location
    ORDER BY Listing_Count DESC
    LIMIT 1;
    ''')
    if not q6.empty:
        st.dataframe(q6, use_container_width=True)
    else:
        st.info("No food listings data available.")

    st.header("📈 Analysis & Insights")
    st.subheader("12. Most Claimed Meal Type")
    q12_where = f"AND fl.Location = '{selected_city}'" if selected_city != 'All Cities' else ""
    q12 = run_query(f'''
    SELECT fl.Meal_Type, COUNT(c.Claim_ID) AS Claim_Count
    FROM Claims c
    JOIN Food_Listings fl ON c.Food_ID = fl.Food_ID
    {q12_where}
    GROUP BY fl.Meal_Type
    ORDER BY Claim_Count DESC
    LIMIT 1;
    ''')
    if not q12.empty:
        st.dataframe(q12, use_container_width=True)
    else:
        st.info(f"No most claimed meal type data for selected city: {selected_city}.")

def show_providers_page(selected_city):
    st.header("📍 Food Providers")

    city_filter_providers = ""
    if selected_city != 'All Cities':
        city_filter_providers = f" WHERE p.City = '{selected_city}'"

    st.subheader("1. Providers & Receivers per City")
    q1 = run_query(f'''
    SELECT
        p.City,
        COUNT(DISTINCT p.Provider_ID) AS Providers_Count
    FROM Providers p
    {city_filter_providers}
    GROUP BY p.City
    ORDER BY Providers_Count DESC;
    ''')
    if not q1.empty:
        st.dataframe(q1, use_container_width=True)
        st.bar_chart(q1.set_index("City")[["Providers_Count"]])
    else:
        st.info(f"No data for selected city: {selected_city}.")

    st.subheader("2. Provider Type Contribution")
    q2_where = f"WHERE City = '{selected_city}'" if selected_city != 'All Cities' else ""
    q2 = run_query(f'''
    SELECT Type AS Provider_Type, COUNT(*) AS Total
    FROM Providers
    {q2_where}
    GROUP BY Type
    ORDER BY Total DESC;
    ''')
    if not q2.empty:
        st.dataframe(q2, use_container_width=True)
        st.bar_chart(q2.set_index("Provider_Type"))
    else:
        st.info(f"No provider type data for selected city: {selected_city}.")

    st.subheader("3. Provider Contacts")
    if selected_city != 'All Cities':
        q3 = run_query(f"SELECT Name, Contact FROM Providers WHERE City = '{selected_city}';")
        if not q3.empty:
            st.dataframe(q3, use_container_width=True)
        else:
            st.info(f"No providers found in {selected_city}.")
    else:
        st.info("Select a specific city from the dropdown to view provider contacts.")
    
    st.subheader("13. Total Quantity Donated by Each Provider (Top 10)")
    q13_where = f"AND p.City = '{selected_city}'" if selected_city != 'All Cities' else ""
    q13 = run_query(f'''
    SELECT p.Name AS Provider_Name, SUM(fl.Quantity) AS Total_Quantity_Donated
    FROM Providers p
    JOIN Food_Listings fl ON p.Provider_ID = fl.Provider_ID
    {q13_where}
    GROUP BY p.Name
    ORDER BY Total_Quantity_Donated DESC
    LIMIT 10;
    ''')
    if not q13.empty:
        st.dataframe(q13, use_container_width=True)
        st.bar_chart(q13.set_index("Provider_Name"))
    else:
        st.info(f"No total quantity donated data for selected city: {selected_city}.")

def show_receivers_page(selected_city):
    st.header("🫂 Food Receivers")
    
    city_filter_receivers = ""
    if selected_city != 'All Cities':
        city_filter_receivers = f" WHERE r.City = '{selected_city}'"
    
    st.subheader("1. Providers & Receivers per City")
    q1 = run_query(f'''
    SELECT
        r.City,
        COUNT(DISTINCT r.Receiver_ID) AS Receivers_Count
    FROM Receivers r
    {city_filter_receivers}
    GROUP BY r.City
    ORDER BY Receivers_Count DESC;
    ''')
    if not q1.empty:
        st.dataframe(q1, use_container_width=True)
        st.bar_chart(q1.set_index("City")[["Receivers_Count"]])
    else:
        st.info(f"No data for selected city: {selected_city}.")
        
    st.subheader("4. Top 5 Receivers by Claims Count")
    q4_where = f"AND r.City = '{selected_city}'" if selected_city != 'All Cities' else ""
    q4 = run_query(f'''
    SELECT r.Name AS Receiver_Name, COUNT(c.Claim_ID) AS Claims_Count
    FROM Claims c
    JOIN Receivers r ON c.Receiver_ID = r.Receiver_ID
    {q4_where}
    GROUP BY r.Name
    ORDER BY Claims_Count DESC
    LIMIT 5;
    ''')
    if not q4.empty:
        st.dataframe(q4, use_container_width=True)
        st.bar_chart(q4.set_index("Receiver_Name"))
    else:
        st.info(f"No receiver claims data for selected city: {selected_city}.")

    st.subheader("11. Average Quantity Claimed per Receiver (Top 10)")
    q11_where = f"AND r.City = '{selected_city}'" if selected_city != 'All Cities' else ""
    q11 = run_query(f'''
    SELECT r.Name AS Receiver_Name, ROUND(AVG(fl.Quantity), 2) AS Avg_Quantity_Claimed
    FROM Claims c
    JOIN Receivers r ON c.Receiver_ID = r.Receiver_ID
    JOIN Food_Listings fl ON c.Food_ID = fl.Food_ID
    WHERE c.Status = 'Completed'
    {q11_where}
    GROUP BY r.Name
    ORDER BY Avg_Quantity_Claimed DESC
    LIMIT 10;
    ''')
    if not q11.empty:
        st.dataframe(q11, use_container_width=True)
    else:
        st.info(f"No average quantity claimed data for selected city: {selected_city}.")


def main():
    st.title("🍲 Food Waste Management Insights")
    
    st.sidebar.header("Filter Options")
    all_cities_query = '''
        SELECT DISTINCT City FROM Providers
        UNION
        SELECT DISTINCT Location FROM Food_Listings;
    '''
    all_cities_df = run_query(all_cities_query)
    all_cities = ['All Cities'] + sorted(all_cities_df['City'].tolist())
    selected_city = st.sidebar.selectbox("🌍 Select City to Filter Data:", all_cities)
    
    st.sidebar.markdown("---")
    page_selection = st.sidebar.radio("Navigate Pages", ["Dashboard", "Providers", "Receivers"])
    
    if page_selection == "Dashboard":
        show_dashboard_page(selected_city)
    elif page_selection == "Providers":
        show_providers_page(selected_city)
    elif page_selection == "Receivers":
        show_receivers_page(selected_city)

if __name__ == "__main__":
    main()