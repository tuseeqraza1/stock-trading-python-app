import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("API_KEY")
LIMIT = 1000

url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}"

response = requests.get(url)
tickers = []

data = response.json()

for ticker in data['results']:
    tickers.append(ticker)

while 'next_url' in data: 
    print('Requesting next page', data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={API_KEY}')
    data = response.json()
    print(data)

    if 'results' in data:
        for ticker in data['results']:
            tickers.append(ticker)
    else:
        print(f"Error: 'results' key not found. Response: {data}")
        break

# Write tickers to CSV
csv_filename = 'tickers.csv'
if tickers:
    fieldnames = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 
                  'active', 'currency_name', 'composite_figi', 'share_class_figi', 
                  'last_updated_utc', 'cik']
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(tickers)
    
    print(f"Successfully wrote {len(tickers)} tickers to {csv_filename}")
else:
    print("No tickers to write")