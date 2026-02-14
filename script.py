from datetime import datetime
import requests
import os
import time
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# Polygon API
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
RATE_LIMIT_DELAY = 0.2  # 200ms delay between requests to respect rate limits

# Snowflake connection details (loaded from .env)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")  # default fallback
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")  # default fallback
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")  # default fallback
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")  # default fallback
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")  # default fallback

# Column mappings derived from EXAMPLE_TICKER keys
EXAMPLE_TICKER = {
    'ticker': 'JLQD',
    'name': 'Janus Henderson Corporate Bond ETF', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'ARCX', 
    'type': 'ETF', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001500604', 
    'composite_figi': 'BBG012FDKHT8', 
    'share_class_figi': 'BBG012FDKJP8', 
    'last_updated_utc': '2026-02-07T07:07:28.766612314Z',
    'ds': '2026-02-07'
}

# Map API field names to Snowflake column names (uppercase keys from EXAMPLE_TICKER)
COLUMN_MAPPING = {key.upper(): key for key in EXAMPLE_TICKER.keys()}


def fetch_and_ingest_tickers_to_snowflake():
    """
    Fetch all tickers from Polygon API and ingest into Snowflake table.
    
    Returns:
        tuple: (number of records ingested, table name) or (None, None) on error.
    """
    conn = None
    DS = datetime.now().strftime('%Y-%m-%d')  # Current date for partitioning
    try:
        # Connect to Snowflake
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
        print("Connected to Snowflake successfully!")
        
        # Create table if it doesn't exist
        col_definitions = [
            'TICKER VARCHAR(20)',
            'NAME VARCHAR(500)',
            'MARKET VARCHAR(50)',
            'LOCALE VARCHAR(10)',
            'PRIMARY_EXCHANGE VARCHAR(10)',
            'TYPE VARCHAR(20)',
            'ACTIVE BOOLEAN',
            'CURRENCY_NAME VARCHAR(50)',
            'CIK VARCHAR(20)',
            'COMPOSITE_FIGI VARCHAR(50)',
            'SHARE_CLASS_FIGI VARCHAR(50)',
            'LAST_UPDATED_UTC TIMESTAMP',
            'DS DATE'
        ]
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            {', '.join(col_definitions)}
        )
        """
        cursor.execute(create_table_sql)
        print(f"Table {SNOWFLAKE_TABLE} created or already exists")
        
        # Fetch tickers from Polygon API
        print("Fetching tickers from Polygon API...")
        url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
        response = requests.get(url)
        data = response.json()
        tickers = []
        
        # Add first batch
        if 'results' in data:
            for ticker in data['results']:
                ticker['ds'] = DS  # Add partitioning column
                tickers.append(ticker)
        
        # Paginate through remaining results
        while 'next_url' in data:
            next_url = data['next_url']
            print(f"Fetching next page...")
            time.sleep(RATE_LIMIT_DELAY)  # Add delay to respect rate limits
            response = requests.get(next_url + f'&apiKey={POLYGON_API_KEY}')
            data = response.json()
            
            # Check if response contains an error
            if data.get('status') == 'ERROR':
                print(f"Error: {data.get('error')}")
                print("Rate limit hit. Waiting before retrying...")
                time.sleep(5)  # Wait 5 seconds before retrying
                response = requests.get(next_url + f'&apiKey={POLYGON_API_KEY}')
                data = response.json()
            
            # Only process results if they exist
            if 'results' in data:
                for ticker in data['results']:
                    ticker['ds'] = DS  # Add partitioning column
                    tickers.append(ticker)
            else:
                print("No results in response, stopping pagination")
                break
        
        print(f"Fetched {len(tickers)} tickers. Ingesting into Snowflake...")
        
        # Dynamically build INSERT statement from COLUMN_MAPPING
        col_names = ', '.join(COLUMN_MAPPING.keys())
        placeholders = ', '.join(['%s' for _ in COLUMN_MAPPING])  # Snowflake uses %s, not ?
        insert_sql = f"INSERT INTO {SNOWFLAKE_TABLE} ({col_names}) VALUES ({placeholders})"
        
        # Debug: Print SQL structure
        print(f"Number of columns: {len(COLUMN_MAPPING)}")
        print(f"Number of placeholders: {placeholders.count('%s')}")
        
        # Extract values from ticker dict using column mapping
        def extract_ticker_values(ticker, mapping):
            """Extract values from ticker dict in the order of column mapping"""
            return tuple(ticker.get(field) for field in mapping.values())
        
        # Insert data in batches
        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            data_to_insert = [extract_ticker_values(ticker, COLUMN_MAPPING) for ticker in batch]
            # Debug: Check first tuple size matches placeholders
            if data_to_insert and len(data_to_insert[0]) != len(COLUMN_MAPPING):
                raise ValueError(f"Mismatch: {len(data_to_insert[0])} values vs {len(COLUMN_MAPPING)} columns")
            cursor.executemany(insert_sql, data_to_insert)
            print(f"Ingested batch {i//batch_size + 1} ({len(data_to_insert)} records)")
        
        conn.commit()
        print(f"Successfully ingested {len(tickers)} tickers into {SNOWFLAKE_TABLE}")
        return len(tickers), SNOWFLAKE_TABLE
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
        return None, None
    finally:
        if conn:
            conn.close()
            print("Snowflake connection closed")


if __name__ == "__main__":
    fetch_and_ingest_tickers_to_snowflake()
