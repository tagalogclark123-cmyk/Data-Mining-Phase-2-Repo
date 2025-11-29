import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text # <--- Added 'text' for raw SQL queries
import config  # <--- IMPORT YOUR CONFIG FILE HERE

# --- CONFIGURATION ---
# 1. API SETUP
# Now we grab the key from the config.py file
API_KEY = config.API_KEY 
BASE_URL = "https://api.fda.gov/drug/shortages.json"

# 2. DATABASE CONNECTION (PostgreSQL)
# We pull these from config.py too, keeping this file clean!
DB_USER = config.DB_USER
DB_PASSWORD = config.DB_PASSWORD
DB_HOST = config.DB_HOST
DB_PORT = config.DB_PORT
DB_NAME = config.DB_NAME

# Create the connection string
# Format: postgresql+psycopg2://user:password@host:port/database
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the engine
try:
    engine = create_engine(DATABASE_URL)
    print("Database engine created successfully.")
except Exception as e:
    print(f"Error creating database engine: {e}")
    exit()

def fetch_fda_data(limit=1000):
    """
    Fetches data from OpenFDA.
    """
    print(f"Fetching {limit} records from OpenFDA...")
    params = {
        'limit': limit,
        'api_key': API_KEY 
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def transform_and_load(results):
    """
    Transforms OpenFDA JSON into your specific Schema format.
    """
    print("Transforming data...")
    
    shortages_list = []
    products_list = []
    
    for item in results:
        # 1. GENERATE ID (Composite Key)
        g_name = item.get('generic_name', 'Unknown')
        c_name = item.get('company_name', 'Unknown')
        # Clean string for ID generation
        shortage_id = f"{g_name[:15]}_{c_name[:10]}".replace(" ", "").upper()
        
        # 2. MAP TO YOUR SPECIFIC FIELDS
        # Update: We prioritize fields found in the user's specific JSON structure (update_date, related_info)
        
        # Helper to find reason (JSON uses 'related_info' often for context if 'reason_for_shortage' is missing)
        reason = item.get('reason_for_shortage')
        if not reason:
            reason = item.get('related_info', 'Not Listed')

        # Helper for date (JSON uses 'update_date')
        date_str = item.get('update_date', item.get('date_updated'))
        
        shortage_entry = {
            'shortage_id': shortage_id,
            'drugname': g_name,                         
            'manufacturer': c_name,                     
            'status': item.get('status', 'Unknown'),    
            'shortagereason': reason, 
            'lastupdate': date_str, 
            'availabilityscore': None,  # Placeholder for Phase 3
            'regionaffected': 'USA'     # Placeholder for Phase 3
        }
        
        shortages_list.append(shortage_entry)
        
        # 3. EXTRACT CHILD DATA (Affected Products)
        # Update: The JSON structure is flat. The product info is in the main item.
        # We extract 'package_ndc' and 'presentation' directly.
        
        product_status = item.get('availability', 'Unknown') # e.g. "Available"
        
        products_list.append({
            'shortage_id': shortage_id,
            'ndc': item.get('package_ndc', 'N/A'),
            'presentation': item.get('presentation', 'N/A'),
            'status': product_status
        })

    # Convert to DataFrames
    df_shortages = pd.DataFrame(shortages_list)
    df_products = pd.DataFrame(products_list)
    
    # --- DATE FORMAT FIX ---
    # Convert 'lastupdate' from "MM/DD/YYYY" string to a proper Python datetime object.
    # This prevents the "DatetimeFieldOverflow" error in PostgreSQL.
    df_shortages['lastupdate'] = pd.to_datetime(df_shortages['lastupdate'], errors='coerce')

    # Remove duplicates from shortages (since the flat structure repeats drug info for every package)
    df_shortages.drop_duplicates(subset=['shortage_id'], inplace=True)
    
    print(f"Prepared {len(df_shortages)} shortage reports.")
    print(f"Prepared {len(df_products)} affected product lines.")
    
    # --- LOAD TO DATABASE ---
    print("Loading to PostgreSQL...")
    
    try:
        # STRATEGY CHANGE: 
        # Instead of 'replace' (which tries to drop tables and causes your error),
        # we Empty the tables first, then Append. 
        # This keeps your Foreign Keys and Schema perfect!
        
        with engine.begin() as conn:
            # TRUNCATE empties the table. CASCADE tells Postgres to empty dependent tables (child) too.
            print("Clearing old data from tables...")
            conn.execute(text("TRUNCATE TABLE drug_shortages, affected_products RESTART IDENTITY CASCADE;"))
        
        # Now we use 'append' because the tables are empty and waiting
        df_shortages.to_sql('drug_shortages', con=engine, if_exists='append', index=False)
        df_products.to_sql('affected_products', con=engine, if_exists='append', index=False)
        
        print("Success! Data has been migrated to your PostgreSQL database.")
        
    except Exception as e:
        print(f"Database Error: {e}")

if __name__ == "__main__":
    data = fetch_fda_data(limit=1000)
    if data:
        transform_and_load(data)
    else:
        print("No data found to migrate.")