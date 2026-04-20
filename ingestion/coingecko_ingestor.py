import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

# --- Configuration ---
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "coin-market-cap-api-data"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def fetch_coingecko_data(name, url, params=None):
    # Perform the request
    response = requests.get(url, params=params)
    
    # Force an error if the status code is NOT 200 (e.g., 429 for rate limits)
    # This will crash the script and make Airflow turn RED
    response.raise_for_status()
    
    return {
        "endpoint_type": name,
        "source": "coingecko",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload": response.json()
    }

def upload_to_azure(data):
    if not data: return
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Define Metadata and Path per requirements
        source = data['source']
        ingestion_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        time_str = datetime.now(timezone.utc).strftime('%H%M%S')
        
        # Handle filename for historical backfills to avoid overwriting during loops
        coin_suffix = f"_{data.get('coin_id')}" if 'coin_id' in data else ""
        
        # Final Path: bronze/source/ingestion_date=YYYY-MM-DD/endpoint_type_HHMMSS.json
        blob_name = f"bronze/{source}/ingestion_date={ingestion_date}/{data['endpoint_type']}{coin_suffix}_{time_str}.json"
        
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        print(f"✅ Successfully uploaded to Bronze: {blob_name}")
    except Exception as e:
        print(f"❌ Failed to upload {data['endpoint_type']} to Azure: {e}")

def run_pipeline():
    print(f"🚀 Starting CoinGecko Batch Ingestion: {datetime.now(timezone.utc)}")
    # 1. Market Snapshot (Top 50)
    upload_to_azure(fetch_coingecko_data("markets_snapshot", f"{COINGECKO_BASE}/coins/markets", {'vs_currency': 'usd', 'per_page': '50'}))
    
    # 2. Historical Backfill (Daily check)
    for coin in ['bitcoin', 'ethereum']:
        hist = fetch_coingecko_data("historical_backfill", f"{COINGECKO_BASE}/coins/{coin}/market_chart", {'vs_currency': 'usd', 'days': '30'})
        if hist:
            hist['coin_id'] = coin
            upload_to_azure(hist)
        time.sleep(2) # Avoid rate limits

if __name__ == "__main__":
    run_pipeline()