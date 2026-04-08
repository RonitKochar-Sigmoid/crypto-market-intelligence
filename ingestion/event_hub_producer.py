import requests
import json
import time
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData, TransportType
import os
from dotenv import load_dotenv

load_dotenv()

CMC_API_KEY = os.getenv("CMC_API_KEY")
EH_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EH_NAME = "capstone-project-coinmarketcap"

CMC_ENDPOINTS = {
    "listings": "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
    "global": "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
    "quotes": "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def fetch_cmc_data(name, url, params=None):
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        # The V2 quotes endpoint returns data as a dictionary keyed by ID, 
        # while listings return a list. We handle that in Silver.
        return {
            "endpoint_type": name,
            "source": "coinmarketcap",
            "ingested_at": datetime.utcnow().isoformat(),
            "payload": response.json()['data']
        }
    except Exception as e:
        print(f"Error fetching CMC {name}: {e}")
        return None

def fetch_coingecko_data(name, url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return {
            "endpoint_type": name,
            "source": "coingecko",
            "ingested_at": datetime.utcnow().isoformat(),
            "payload": response.json()
        }
    except Exception as e:
        print(f"Error fetching CoinGecko {name}: {e}")
        return None

def run_pipeline():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EH_CONNECTION_STR, eventhub_name=EH_NAME, transport_type=TransportType.AmqpOverWebsocket
    )
    datasets = []

    # 1. CMC Listings (Endpoint 1)
    datasets.append(fetch_cmc_data("listings", CMC_ENDPOINTS["listings"], {'limit': '100', 'convert': 'USD'}))
    
    # 2. CMC Global Metrics (Endpoint 2)
    datasets.append(fetch_cmc_data("global", CMC_ENDPOINTS["global"]))
    
    # 3. CMC Quotes Latest (Endpoint 3 - for selective refresh of BTC/ETH)
    datasets.append(fetch_cmc_data("quotes", CMC_ENDPOINTS["quotes"], {'symbol': 'BTC,ETH', 'convert': 'USD'}))

    # 4. CoinGecko Markets (Endpoint 4 - Snapshot)
    datasets.append(fetch_coingecko_data("markets_snapshot", f"{COINGECKO_BASE}/coins/markets", {'vs_currency': 'usd', 'per_page': '50'}))

    # 5. CoinGecko Historical (Endpoint 5 - Backfill)
    for coin in ['bitcoin', 'ethereum']:
        hist = fetch_coingecko_data("historical_backfill", f"{COINGECKO_BASE}/coins/{coin}/market_chart", {'vs_currency': 'usd', 'days': '30'})
        if hist: 
            hist['coin_id'] = coin # Add context
            datasets.append(hist)
        time.sleep(1)

    # Filter out None and Send
    final_data = [d for d in datasets if d is not None]
    with producer:
        batch = producer.create_batch()
        for data in final_data:
            batch.add(EventData(json.dumps(data)))
        producer.send_batch(batch)
    print(f"Sent {len(final_data)} endpoints to Event Hub.")

if __name__ == "__main__":
    run_pipeline()
