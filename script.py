import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from snowflake.connector import connect
load_dotenv()

API_KEY = os.getenv("API_KEY")
LIMIT = 1000  # Maximum number of records per request

# Snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

# Session for faster requests
session = requests.Session()

def run_stock_job():
    url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}"

    response = session.get(url)
    tickers = []

    data = response.json()

    for ticker in data['results']:
        tickers.append(ticker)

    while 'next_url' in data: 
        print('Requesting next page', data['next_url'])
        response = session.get(data['next_url'] + f'&apiKey={API_KEY}')
        data = response.json()
        # print(data)

        if 'results' in data:
            for ticker in data['results']:
                tickers.append(ticker)
        else:
            print(f"Error: 'results' key not found. Response: {data}")
            break

    # Upload tickers to Snowflake in batches
    if tickers:
        try:
            conn = connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            cursor = conn.cursor()
            
            # Insert data into Snowflake table in batches
            ds = datetime.utcnow().strftime('%Y-%m-%d')  # Current date in YYYY-MM-DD format
            batch_size = 100
            
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i + batch_size]
                values_list = []
                
                for ticker in batch:
                    ticker_val = ticker.get('ticker', '')
                    name_val = ticker.get('name', '').replace("'", "''")
                    market_val = ticker.get('market', '')
                    locale_val = ticker.get('locale', '')
                    exchange_val = ticker.get('primary_exchange', '')
                    type_val = ticker.get('type', '')
                    active_val = str(ticker.get('active', False)).lower()
                    currency_val = ticker.get('currency_name', '')
                    composite_figi_val = ticker.get('composite_figi', '')
                    share_class_figi_val = ticker.get('share_class_figi', '')
                    last_updated_val = ticker.get('last_updated_utc', '')
                    cik_val = ticker.get('cik', '')
                    
                    values_list.append(f"('{ticker_val}', '{name_val}', '{market_val}', '{locale_val}', '{exchange_val}', '{type_val}', {active_val}, '{currency_val}', '{composite_figi_val}', '{share_class_figi_val}', '{last_updated_val}', '{cik_val}', '{ds}')")
                
                insert_sql = f"""
                INSERT INTO {SNOWFLAKE_TABLE} 
                (ticker, name, market, locale, primary_exchange, type, active, 
                 currency_name, composite_figi, share_class_figi, last_updated_utc, cik, ds)
                VALUES {', '.join(values_list)}
                """
                cursor.execute(insert_sql)
                print(f"Uploaded batch {i // batch_size + 1} ({len(batch)} records)")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Successfully uploaded {len(tickers)} tickers to Snowflake table {SNOWFLAKE_TABLE}")
        except Exception as e:
            print(f"Error uploading to Snowflake: {str(e)}")
    else: 
        print("No tickers to upload")

if __name__ == "__main__":
    run_stock_job()
