import requests
import os
import csv
import time
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000
RATE_LIMIT_DELAY = 0.2  # 200ms delay between requests to respect rate limits

url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []

data = response.json()
for ticker in data['results']:
    tickers.append(ticker)

while 'next_url' in data:
    time.sleep(RATE_LIMIT_DELAY)  # Add delay to respect rate limits
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    
    if 'results' in data:
        for ticker in data['results']:
            tickers.append(ticker)
    else:
        print("No results in response, stopping pagination")
        break

example_ticker = {
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


# Write tickers to CSV with same schema as example_ticker
csv_columns = list(example_ticker.keys())
csv_filename = 'tickers.csv'

try:
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(tickers)
    print(f"CSV file '{csv_filename}' created successfully with {len(tickers)} records")
except Exception as e:
    print(f"Error writing CSV: {e}")
