import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables (CMC_API_KEY, AZURE_STORAGE_CONNECTION_STRING)
load_dotenv()

# --- Configuration ---
CMC_API_KEY = os.getenv("CMC_API_KEY") 
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Updated per your requirement
STORAGE_ACCOUNT_NAME = "coinmarketcapapi"
CONTAINER_NAME = "coin-market-cap-api-data"

CMC_ENDPOINTS = {
    "listings": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
    "global": "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
    "quotes": "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def fetch_cmc_data(name, url, params=None):
    """Fetches data from CoinMarketCap and wraps it with metadata."""
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return {
            "endpoint_type": name,
            "source": "coinmarketcap",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": response.json().get('data', [])
        }
    except Exception as e:
        print(f"Error fetching CMC {name}: {e}")
        return None

def fetch_coingecko_data(name, url, params=None):
    """Fetches data from CoinGecko and wraps it with metadata."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return {
            "endpoint_type": name,
            "source": "coingecko",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": response.json()
        }
    except Exception as e:
        print(f"Error fetching CoinGecko {name}: {e}")
        return None

def upload_to_azure(data):
    """
    Uploads the JSON data directly to the Bronze layer in Azure Blob Storage.
    Final Path: bronze/source/ingestion_date=YYYY-MM-DD/endpoint_type_HHMMSS.json
    """
    try:
        # Initialize the client
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Define Metadata
        source = data['source']
        ingestion_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        time_str = datetime.now(timezone.utc).strftime('%H%M%S')
        
        # --- FIX: Prepend 'bronze/' to restore the original folder structure ---
        blob_name = f"bronze/{source}/ingestion_date={ingestion_date}/{data['endpoint_type']}_{time_str}.json"
        
        # Get blob client and upload
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        
        print(f"✅ Successfully uploaded to Bronze: {blob_name}")
        
    except Exception as e:
        print(f"❌ CRITICAL: Failed to upload {data['endpoint_type']} to Azure: {e}")

def run_pipeline():
    print(f"--- Starting Crypto Ingestion Pipeline: {datetime.now(timezone.utc)} ---")
    datasets = []

    # 1. CMC: Top 100 Listings
    datasets.append(fetch_cmc_data("listings", CMC_ENDPOINTS["listings"], {'limit': '100', 'convert': 'USD'}))
    
    # 2. CMC: Global Market Metrics
    datasets.append(fetch_cmc_data("global", CMC_ENDPOINTS["global"]))
    
    # 3. CMC: Latest Quotes for BTC/ETH (V2)
    datasets.append(fetch_cmc_data("quotes", CMC_ENDPOINTS["quotes"], {'symbol': 'BTC,ETH', 'convert': 'USD'}))

    # 4. CoinGecko: Market Snapshot
    datasets.append(fetch_coingecko_data("markets_snapshot", f"{COINGECKO_BASE}/coins/markets", {'vs_currency': 'usd', 'per_page': '50'}))

    # 5. CoinGecko: Historical Market Data (Loop)
    for coin in ['bitcoin', 'ethereum']:
        hist = fetch_coingecko_data("historical_backfill", f"{COINGECKO_BASE}/coins/{coin}/market_chart", {'vs_currency': 'usd', 'days': '30'})
        if hist: 
            hist['coin_id'] = coin 
            datasets.append(hist)
        time.sleep(1.5) # Compliance with free-tier rate limits

    # Filter failures and upload
    final_data = [d for d in datasets if d is not None]
    
    for item in final_data:
        upload_to_azure(item)

    print(f"--- Pipeline Finished. Total Uploads: {len(final_data)} ---")

if __name__ == "__main__":
    run_pipeline()