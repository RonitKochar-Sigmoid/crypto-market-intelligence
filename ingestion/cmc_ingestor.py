import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

# --- Configuration ---
CMC_API_KEY = os.getenv("CMC_API_KEY")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "coin-market-cap-api-data"

CMC_ENDPOINTS = {
    "listings": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
    "global": "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
    "quotes": "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
}

def fetch_cmc_data(name, url, params=None):
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return {
            "endpoint_type": name,
            "source": "coinmarketcap",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": response.json().get('data')
        }
    except Exception as e:
        print(f"❌ Error fetching {name} from CMC: {e}")
        return None

def upload_to_azure(data):
    if not data: return
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Define Metadata and Path per requirements
        source = data['source']
        ingestion_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        time_str = datetime.now(timezone.utc).strftime('%H%M%S')
        
        # Final Path: bronze/source/ingestion_date=YYYY-MM-DD/endpoint_type_HHMMSS.json
        blob_name = f"bronze/{source}/ingestion_date={ingestion_date}/{data['endpoint_type']}_{time_str}.json"
        
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        print(f"✅ Successfully uploaded to Bronze: {blob_name}")
    except Exception as e:
        print(f"❌ Failed to upload {data['endpoint_type']} to Azure: {e}")

def run_pipeline():
    print(f"🚀 Starting CMC Real-Time Ingestion: {datetime.now(timezone.utc)}")
    # 1. Top 100 Listings
    upload_to_azure(fetch_cmc_data("listings", CMC_ENDPOINTS["listings"], {'limit': '100', 'convert': 'USD'}))
    # 2. Global Metrics
    upload_to_azure(fetch_cmc_data("global", CMC_ENDPOINTS["global"]))
    # 3. Specific Quotes (BTC/ETH)
    upload_to_azure(fetch_cmc_data("quotes", CMC_ENDPOINTS["quotes"], {'symbol': 'BTC,ETH', 'convert': 'USD'}))

if __name__ == "__main__":
    run_pipeline()