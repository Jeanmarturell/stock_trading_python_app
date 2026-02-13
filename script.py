import requests
import os
import csv
import time
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
RATE_LIMIT_DELAY = 0.2  # 200ms delay between requests to respect rate limits

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
    'last_updated_utc': '2026-02-07T07:07:28.766612314Z'
}


def fetch_and_save_tickers(csv_filename='tickers.csv'):
    """
    Fetch all tickers from Polygon API and save to CSV file.
    
    Args:
        csv_filename (str): Output CSV file name. Defaults to 'tickers.csv'.
    
    Returns:
        tuple: (tickers list, csv_filename) or (None, None) on error.
    """
    try:
        # Fetch initial batch of tickers
        url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
        response = requests.get(url)
        data = response.json()
        tickers = []
        
        # Add first batch
        if 'results' in data:
            for ticker in data['results']:
                tickers.append(ticker)
        
        # Paginate through remaining results
        while 'next_url' in data:
            next_url = data['next_url']
            print(f"Fetching next page: {next_url}")
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
                    tickers.append(ticker)
            else:
                print("No results in response, stopping pagination")
                break
        
        # Write tickers to CSV with same schema as example_ticker
        csv_columns = list(EXAMPLE_TICKER.keys())
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            writer.writerows(tickers)
        
        print(f"CSV file '{csv_filename}' created successfully with {len(tickers)} records")
        return tickers, csv_filename
        
    except Exception as e:
        print(f"Error: {e}")
        return None, None


if __name__ == "__main__":
    fetch_and_save_tickers()
