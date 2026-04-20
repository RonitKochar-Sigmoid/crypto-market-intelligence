import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time

spark = SparkSession.builder.getOrCreate()

COINS = [
    'bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot',
    'chainlink', 'dogecoin', 'pepe', 'uniswap', 'binancecoin'
]

COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
BRONZE_TABLE = "<BRONZE_TABLE>"
GOLD_FEATURES_TABLE = "<GOLD_FEATURES_TABLE>"
GOLD_PREDICTIONS_TABLE = "<GOLD_PREDICTIONS_TABLE>"

print(f"Pipeline initialized at {datetime.now()}")
print(f"Processing {len(COINS)} coins")


def fetch_current_prices(coin_ids):
    prices_data = []

    for coin_id in coin_ids:
        max_retries = 3
        retry_delay = 3

        for attempt in range(max_retries):
            try:
                url = f"{COINGECKO_API_URL}/coins/{coin_id}"
                params = {
                    'localization': 'false',
                    'tickers': 'false',
                    'community_data': 'false',
                    'developer_data': 'false'
                }

                response = requests.get(url, params=params, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    market_data = data.get('market_data', {})

                    record = {
                        'coin_id': coin_id,
                        'timestamp': datetime.now(),
                        'price_usd': market_data.get('current_price', {}).get('usd'),
                        'market_cap': market_data.get('market_cap', {}).get('usd'),
                        'total_volume': market_data.get('total_volume', {}).get('usd'),
                        'price_change_24h': market_data.get('price_change_24h'),
                        'price_change_percentage_24h': market_data.get('price_change_percentage_24h'),
                        'market_cap_rank': market_data.get('market_cap_rank'),
                        'high_24h': market_data.get('high_24h', {}).get('usd'),
                        'low_24h': market_data.get('low_24h', {}).get('usd'),
                        'circulating_supply': market_data.get('circulating_supply'),
                        'total_supply': market_data.get('total_supply')
                    }

                    prices_data.append(record)
                    print(f"Fetched {coin_id}: {record['price_usd']}")
                    break

                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)
                        print(f"Rate limited for {coin_id}, retry {attempt+1} in {wait_time}s")
                        time.sleep(wait_time)
                    else:
                        print(f"Failed for {coin_id}: rate limited")

                else:
                    print(f"Failed for {coin_id}: status {response.status_code}")
                    break

            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Retry {attempt+1} for {coin_id}: {e}")
                    time.sleep(retry_delay)
                else:
                    print(f"Error for {coin_id}: {e}")

        time.sleep(6)

    return prices_data


print("Fetching latest prices")
prices_data = fetch_current_prices(COINS)

if not prices_data:
    raise Exception("No prices fetched")

prices_df = pd.DataFrame(prices_data)


bronze_spark_df = spark.createDataFrame(prices_df)

bronze_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_TABLE)


def calculate_features_for_coin(coin_id):
    query = f"""
    SELECT timestamp, price_usd as price, total_volume as volume, market_cap
    FROM {BRONZE_TABLE}
    WHERE coin_id = '{coin_id}'
    AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY timestamp DESC
    """

    df = spark.sql(query).toPandas()

    if len(df) < 24:
        return None

    df = df.sort_values('timestamp')

    latest_price = df.iloc[-1]['price']
    latest_volume = df.iloc[-1]['volume']

    momentum_1h = (df.iloc[-1]['price'] / df.iloc[-2]['price'] - 1) if len(df) >= 2 else 0
    momentum_6h = (df.iloc[-1]['price'] / df.iloc[-7]['price'] - 1) if len(df) >= 7 else 0
    momentum_24h = (df.iloc[-1]['price'] / df.iloc[-25]['price'] - 1) if len(df) >= 25 else 0

    volume_momentum_1h = (df.iloc[-1]['volume'] / df.iloc[-2]['volume'] - 1) if len(df) >= 2 else 0
    volume_momentum_6h = (df.iloc[-1]['volume'] / df.iloc[-7:].volume.mean() - 1) if len(df) >= 7 else 0
    volume_momentum_24h = (df.iloc[-1]['volume'] / df.iloc[-25:].volume.mean() - 1) if len(df) >= 25 else 0

    deltas = df['price'].diff()
    gains = deltas.where(deltas > 0, 0)
    losses = -deltas.where(deltas < 0, 0)

    avg_gain = gains.rolling(14).mean().iloc[-1]
    avg_loss = losses.rolling(14).mean().iloc[-1]

    rs = avg_gain / avg_loss if avg_loss != 0 else 0
    rsi = 100 - (100 / (1 + rs))

    ema_12 = df['price'].ewm(span=12, adjust=False).mean().iloc[-1]
    ema_26 = df['price'].ewm(span=26, adjust=False).mean().iloc[-1]

    macd = ema_12 - ema_26
    signal = df['price'].ewm(span=9, adjust=False).mean().iloc[-1]
    macd_histogram = macd - signal

    volatility_24h = df.iloc[-25:]['price'].std() if len(df) >= 25 else 0

    coin_id_map = {coin: idx for idx, coin in enumerate(COINS)}

    return {
        'coin_id': coin_id,
        'timestamp': datetime.now(),
        'price_usd': latest_price,
        'volume_usd': latest_volume,
        'momentum_1h': momentum_1h,
        'momentum_6h': momentum_6h,
        'momentum_24h': momentum_24h,
        'volume_momentum_1h': volume_momentum_1h,
        'volume_momentum_6h': volume_momentum_6h,
        'volume_momentum_24h': volume_momentum_24h,
        'rsi': rsi,
        'macd': macd,
        'macd_signal': signal,
        'macd_histogram': macd_histogram,
        'volatility_24h': volatility_24h,
        'coin_id_encoded': coin_id_map.get(coin_id, 0)
    }


features_list = [f for coin in COINS if (f := calculate_features_for_coin(coin))]
features_df = pd.DataFrame(features_list)

gold_features_spark_df = spark.createDataFrame(features_df)

gold_features_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(GOLD_FEATURES_TABLE)


MODEL_ENDPOINT = "<MODEL_ENDPOINT>"
MODEL_TOKEN = "<MODEL_TOKEN>"

FEATURE_COLS = [
    'price_usd', 'volume_usd',
    'momentum_1h', 'momentum_6h', 'momentum_24h',
    'volume_momentum_1h', 'volume_momentum_6h', 'volume_momentum_24h',
    'rsi', 'macd', 'macd_signal', 'macd_histogram',
    'volatility_24h', 'coin_id_encoded'
]


def call_model_endpoint(features_row):
    payload = {
        "dataframe_records": [
            {col: features_row[col] for col in FEATURE_COLS}
        ]
    }

    headers = {
        "Authorization": f"Bearer {MODEL_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            MODEL_ENDPOINT,
            headers=headers,
            data=json.dumps(payload),
            timeout=15
        )
        response.raise_for_status()
        probability = float(response.json()['predictions'][0])
    except Exception:
        probability = 0.5

    probability = max(0.0, min(1.0, probability))

    if probability >= 0.60:
        return 'UP', 'High', probability
    elif probability >= 0.52:
        return 'UP', 'Low', probability
    elif probability >= 0.48:
        return 'NEUTRAL', 'Low', probability
    elif probability >= 0.40:
        return 'DOWN', 'Low', probability
    else:
        return 'DOWN', 'High', probability


predictions = []

for _, row in features_df.iterrows():
    direction, confidence, prob = call_model_endpoint(row.to_dict())

    predictions.append({
        'coin_id': row['coin_id'],
        'timestamp': datetime.now(),
        'direction': direction,
        'confidence_level': confidence,
        'probability': prob
    })


predictions_df = pd.DataFrame(predictions)

gold_predictions_spark_df = spark.createDataFrame(predictions_df)

gold_predictions_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(GOLD_PREDICTIONS_TABLE)

print("Pipeline completed")
