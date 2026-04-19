# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC # Hourly Crypto Data Ingestion Pipeline
# MAGIC
# MAGIC **Purpose**: Fetch latest crypto prices, calculate features, generate predictions, and store results
# MAGIC
# MAGIC **Runs**: Every hour via scheduled job
# MAGIC
# MAGIC **Flow**:
# MAGIC 1. Fetch latest prices from CoinGecko API (10 coins)
# MAGIC 2. Store raw data → Bronze Layer (`finalcrypto.bronze.crypto_prices`)
# MAGIC 3. Calculate technical features → Gold Layer (`finalcrypto.gold.crypto_features_v3`)
# MAGIC 4. Call model endpoint → Get predictions
# MAGIC 5. Store predictions → Gold Layer (`finalcrypto.gold.crypto_predictions_live`)
# MAGIC
# MAGIC **Last Updated**: April 16, 2026

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
COINS = [
    'bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot',
    'chainlink', 'dogecoin', 'pepe', 'uniswap', 'binancecoin'
]

COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
BRONZE_TABLE = "finalcrypto.bronze.crypto_prices"
GOLD_FEATURES_TABLE = "finalcrypto.gold.crypto_features_v3"
GOLD_PREDICTIONS_TABLE = "finalcrypto.gold.crypto_predictions_live"

# Model endpoint details
MODEL_ENDPOINT = "https://<<YOUR_DATABRICKS_WORKSPACE_URL>>/serving-endpoints/crypto-price-predictor-v3/invocations"
MODEL_TOKEN = "<<YOUR_DATABRICKS_TOKEN>>"

print(f"✅ Pipeline initialized at {datetime.now()}")
print(f"📊 Processing {len(COINS)} coins")

# COMMAND ----------

# DBTITLE 1,Fetch Latest Prices from CoinGecko
def fetch_current_prices(coin_ids):
    """Fetch current market data with retry logic for rate limiting"""
    
    prices_data = []
    
    for coin_id in coin_ids:
        max_retries = 3
        retry_delay = 3  # Start with 3 seconds
        
        for attempt in range(max_retries):
            try:
                # Fetch current price and market data
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
                    print(f"✅ Fetched {coin_id}: ${record['price_usd']:.4f}")
                    break  # Success - exit retry loop
                    
                elif response.status_code == 429:
                    # Rate limited - retry with exponential backoff
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)  # 3s, 6s, 12s
                        print(f"⏳ Rate limit for {coin_id}, retry {attempt+1}/{max_retries} in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"⚠️  Failed to fetch {coin_id} after {max_retries} attempts: Rate limited")
                else:
                    print(f"⚠️  Failed to fetch {coin_id}: Status {response.status_code}")
                    break  # Don't retry for non-429 errors
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"⚠️  Error fetching {coin_id} (attempt {attempt+1}): {str(e)}, retrying...")
                    time.sleep(retry_delay)
                else:
                    print(f"❌ Error fetching {coin_id} after {max_retries} attempts: {str(e)}")
        
        # Rate limiting between coins (increased to 6 seconds to avoid 429s)
        time.sleep(6)
    
    return prices_data

# Fetch data
print(f"\n🔄 Fetching latest prices from CoinGecko...")
print(f"⏱️  This will take ~60 seconds (6s delay per coin to avoid rate limits)")
prices_data = fetch_current_prices(COINS)
print(f"\n✅ Successfully fetched {len(prices_data)} records")

if len(prices_data) == 0:
    raise Exception("❌ No prices fetched! Cannot proceed with pipeline.")

# Convert to DataFrame
prices_df = pd.DataFrame(prices_data)
display(prices_df)

# COMMAND ----------

# DBTITLE 1,Store in Bronze Layer
# Convert pandas to Spark DataFrame
bronze_spark_df = spark.createDataFrame(prices_df)

# Write to Bronze table (append mode)
bronze_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"✅ Stored {len(prices_df)} records in Bronze layer: {BRONZE_TABLE}")

# Verify write
recent_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {BRONZE_TABLE} 
    WHERE timestamp >= current_timestamp() - INTERVAL 5 MINUTES
""").collect()[0]['count']

print(f"📊 Recent records in Bronze (last 5 min): {recent_count}")

# COMMAND ----------

# DBTITLE 1,Calculate Technical Features
def calculate_features_for_coin(coin_id):
    """Calculate all 14 required features for a coin using historical data"""
    
    # Query last 7 days of data for this coin (enough for 24h features)
    query = f"""
    SELECT 
        timestamp,
        price_usd as price,
        total_volume as volume,
        market_cap
    FROM {BRONZE_TABLE}
    WHERE coin_id = '{coin_id}'
        AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY timestamp DESC
    """
    
    df = spark.sql(query).toPandas()
    
    if len(df) < 24:  # Need at least 24 hours of data
        print(f"⚠️ Insufficient data for {coin_id}: {len(df)} records")
        return None
    
    df = df.sort_values('timestamp')
    
    # Get latest values
    latest_price = df.iloc[-1]['price']
    latest_volume = df.iloc[-1]['volume']
    
    # Calculate momentum features
    momentum_1h = (df.iloc[-1]['price'] / df.iloc[-2]['price'] - 1) if len(df) >= 2 else 0
    momentum_6h = (df.iloc[-1]['price'] / df.iloc[-7]['price'] - 1) if len(df) >= 7 else 0
    momentum_24h = (df.iloc[-1]['price'] / df.iloc[-25]['price'] - 1) if len(df) >= 25 else 0
    
    # Calculate volume momentum
    volume_momentum_1h = (df.iloc[-1]['volume'] / df.iloc[-2]['volume'] - 1) if len(df) >= 2 else 0
    volume_momentum_6h = (df.iloc[-1]['volume'] / df.iloc[-7:].volume.mean() - 1) if len(df) >= 7 else 0
    volume_momentum_24h = (df.iloc[-1]['volume'] / df.iloc[-25:].volume.mean() - 1) if len(df) >= 25 else 0
    
    # RSI calculation (14 periods)
    deltas = df['price'].diff()
    gains = deltas.where(deltas > 0, 0)
    losses = -deltas.where(deltas < 0, 0)
    avg_gain = gains.rolling(window=14).mean().iloc[-1]
    avg_loss = losses.rolling(window=14).mean().iloc[-1]
    rs = avg_gain / avg_loss if avg_loss != 0 else 0
    rsi = 100 - (100 / (1 + rs))
    
    # MACD calculation
    ema_12 = df['price'].ewm(span=12, adjust=False).mean().iloc[-1]
    ema_26 = df['price'].ewm(span=26, adjust=False).mean().iloc[-1]
    macd = ema_12 - ema_26
    signal = df['price'].ewm(span=9, adjust=False).mean().iloc[-1]
    macd_histogram = macd - signal
    
    # Volatility (24h standard deviation)
    volatility_24h = df.iloc[-25:]['price'].std() if len(df) >= 25 else 0
    
    # BTC dominance and interaction (simplified - using market cap ratio)
    if coin_id == 'bitcoin':
        btc_dominance = 1.0
    else:
        # Query Bitcoin market cap
        btc_query = f"""
        SELECT market_cap
        FROM {BRONZE_TABLE}
        WHERE coin_id = 'bitcoin'
        ORDER BY timestamp DESC
        LIMIT 1
        """
        btc_mc = spark.sql(btc_query).collect()[0]['market_cap']
        coin_mc = df.iloc[-1]['market_cap']
        btc_dominance = btc_mc / (btc_mc + coin_mc) if coin_mc else 0.5
    
    btc_dom_x_vol_mom = btc_dominance * volume_momentum_24h
    
    # Coin ID encoding (simple integer encoding)
    coin_id_map = {coin: idx for idx, coin in enumerate(COINS)}
    coin_id_encoded = coin_id_map.get(coin_id, 0)
    
    features = {
        'coin_id': coin_id,
        'timestamp': datetime.now(),
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
        'btc_dominance': btc_dominance,
        'btc_dom_x_vol_mom': btc_dom_x_vol_mom,
        'coin_id_encoded': coin_id_encoded
    }
    
    return features

# Calculate features for all coins
print("\n🔄 Calculating features for all coins...")
features_list = []

for coin in COINS:
    features = calculate_features_for_coin(coin)
    if features:
        features_list.append(features)
        print(f"✅ {coin}: RSI={features['rsi']:.2f}, Momentum_24h={features['momentum_24h']:.4f}")

features_df = pd.DataFrame(features_list)
print(f"\n✅ Calculated features for {len(features_df)} coins")
display(features_df)

# COMMAND ----------

# DBTITLE 1,Store Features in Gold Layer
# Convert to Spark DataFrame
gold_features_spark_df = spark.createDataFrame(features_df)

# Write to Gold features table
gold_features_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(GOLD_FEATURES_TABLE)

print(f"✅ Stored {len(features_df)} feature records in Gold layer: {GOLD_FEATURES_TABLE}")

# Verify
recent_features = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {GOLD_FEATURES_TABLE} 
    WHERE timestamp >= current_timestamp() - INTERVAL 5 MINUTES
""").collect()[0]['count']

print(f"📊 Recent features in Gold (last 5 min): {recent_features}")

# COMMAND ----------

# DBTITLE 1,Call Model Endpoint for Predictions
import requests
import json
import builtins

# Model endpoint configuration
MODEL_ENDPOINT = dbutils.secrets.get(scope="crypto-secrets", key="model-endpoint-url")
MODEL_TOKEN = dbutils.secrets.get(scope="crypto-secrets", key="databricks-token")

FEATURE_COLS = [
    'momentum_1h', 'momentum_6h', 'momentum_24h',
    'volume_momentum_1h', 'volume_momentum_6h', 'volume_momentum_24h',
    'rsi', 'macd', 'macd_signal', 'macd_histogram',
    'volatility_24h', 'btc_dominance',
    'btc_dom_x_vol_mom', 'coin_id_encoded'
]

def call_model_endpoint(features_row: dict) -> dict:
    """
    Call the registered LightGBM model serving endpoint.
    Returns probability and derived direction/confidence.
    """
    # Build payload — Databricks Model Serving expects dataframe_records format
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

        result = response.json()

        # Databricks serving returns predictions under 'predictions' key
        probability = float(result['predictions'][0])

    except Exception as e:
        print(f"⚠️  Endpoint call failed for {features_row.get('coin_id', 'unknown')}: {e}")
        probability = 0.5  # fallback to neutral — do not fabricate a signal

    # Derive direction and confidence from model probability
    probability = builtins.max(0.0, builtins.min(1.0, probability))

    if probability >= 0.60:
        direction, confidence = 'UP', 'High'
    elif probability >= 0.52:
        direction, confidence = 'UP', 'Low'
    elif probability >= 0.48:
        direction, confidence = 'NEUTRAL', 'Low'
    elif probability >= 0.40:
        direction, confidence = 'DOWN', 'Low'
    else:
        direction, confidence = 'DOWN', 'High'

    return {
        'probability': probability,
        'direction': direction,
        'confidence_level': confidence
    }


print("📊 Calling model endpoint for predictions...\n")
predictions_list = []

for _, features_row in features_df.iterrows():
    coin_id = features_row['coin_id']
    features_dict = features_row.to_dict()

    prediction = call_model_endpoint(features_dict)

    pred_record = {
        'coin_id': coin_id,
        'timestamp': datetime.now(),
        'direction': prediction['direction'],
        'confidence_level': prediction['confidence_level'],
        'probability': prediction['probability'],
    }
    predictions_list.append(pred_record)

    print(f"🔮 {coin_id:12s}: {prediction['probability']:.4f} → {prediction['direction']:7s} ({prediction['confidence_level']})")

if len(predictions_list) > 0:
    predictions_df = pd.DataFrame(predictions_list)
    print(f"\n✅ Generated {len(predictions_df)} predictions from model endpoint")
    display(predictions_df)
else:
    print("\n⚠️ No predictions generated")
    predictions_df = pd.DataFrame()

# COMMAND ----------

# DBTITLE 1,Store Predictions in Gold Layer
# Convert to Spark DataFrame
gold_predictions_spark_df = spark.createDataFrame(predictions_df)

# Write to Gold predictions table
gold_predictions_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(GOLD_PREDICTIONS_TABLE)

print(f"✅ Stored {len(predictions_df)} predictions in Gold layer: {GOLD_PREDICTIONS_TABLE}")

# Verify
recent_predictions = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM {GOLD_PREDICTIONS_TABLE} 
    WHERE timestamp >= current_timestamp() - INTERVAL 5 MINUTES
""").collect()[0]['count']

print(f"📊 Recent predictions in Gold (last 5 min): {recent_predictions}")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary
# Summary statistics
print("\n" + "="*60)
print("📊 PIPELINE EXECUTION SUMMARY")
print("="*60)
print(f"⏰ Completed at: {datetime.now()}")
print(f"✅ Coins processed: {len(COINS)}")
print(f"✅ Prices fetched: {len(prices_df)}")
print(f"✅ Features calculated: {len(features_df)}")
print(f"✅ Predictions generated: {len(predictions_df)}")
print("\n📈 Prediction Summary:")

if len(predictions_df) > 0:
    up_count = len(predictions_df[predictions_df['direction'] == 'UP'])
    down_count = len(predictions_df[predictions_df['direction'] == 'DOWN'])
    buy_count = len(predictions_df[predictions_df['recommendation'] == 'BUY'])
    sell_count = len(predictions_df[predictions_df['recommendation'] == 'SELL'])
    
    print(f"   🟢 UP predictions: {up_count}")
    print(f"   🔴 DOWN predictions: {down_count}")
    print(f"   💰 BUY signals: {buy_count}")
    print(f"   💸 SELL signals: {sell_count}")
    
    # Show top predictions
    print("\n🏆 Top Predictions:")
    top_preds = predictions_df.nlargest(5, 'probability')[['coin_id', 'direction', 'probability', 'recommendation']]
    for _, row in top_preds.iterrows():
        print(f"   {row['coin_id']}: {row['direction']} ({row['probability']:.3f}) - {row['recommendation']}")

print("\n✅ Pipeline completed successfully!")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Display Latest Predictions from Gold Table
# Query the latest predictions from Gold layer for verification
print("\n" + "="*80)
print("📊 LATEST ML PREDICTIONS FROM GOLD TABLE")
print("="*80)

latest_predictions = spark.sql(f"""
    WITH ranked_predictions AS (
        SELECT 
            coin_id,
            timestamp,
            direction,
            confidence_level,
            probability,
            recommendation,
            ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY timestamp DESC) as rn
        FROM {GOLD_PREDICTIONS_TABLE}
    )
    SELECT 
        coin_id,
        timestamp,
        ROUND(probability, 4) as probability,
        direction,
        confidence_level,
        recommendation
    FROM ranked_predictions
    WHERE rn = 1
    ORDER BY probability DESC
""").toPandas()

if len(latest_predictions) > 0:
    print(f"\n🕐 Query Time: {datetime.now()}")
    print(f"📈 Total Coins: {len(latest_predictions)}\n")
    
    # Display table
    display(latest_predictions)
    
    # Statistics
    print(f"\n📊 Prediction Statistics:")
    print(f"   Probability Range: {latest_predictions['probability'].min():.4f} - {latest_predictions['probability'].max():.4f}")
    print(f"   Median: {latest_predictions['probability'].median():.4f}")
    print(f"   Std Dev: {latest_predictions['probability'].std():.4f}")
    print(f"   Unique Values: {latest_predictions['probability'].nunique()}")
    
    # Direction breakdown
    direction_counts = latest_predictions['direction'].value_counts()
    print(f"\n🔹 Direction Breakdown:")
    for direction, count in direction_counts.items():
        print(f"   {direction:10s}: {count:2d} coins ({count/len(latest_predictions)*100:5.1f}%)")
    
    # Recommendation breakdown
    rec_counts = latest_predictions['recommendation'].value_counts()
    print(f"\n🔹 Recommendation Breakdown:")
    for rec, count in rec_counts.items():
        print(f"   {rec:12s}: {count:2d} coins ({count/len(latest_predictions)*100:5.1f}%)")
    
    # Top 3 and Bottom 3
    print(f"\n🏆 Top 3 Highest Probability:")
    for _, row in latest_predictions.head(3).iterrows():
        print(f"   {row['coin_id']:12s}: {row['probability']:.4f} → {row['direction']:7s} ({row['confidence_level']:15s}) - {row['recommendation']}")
    
    print(f"\n🔻 Bottom 3 Lowest Probability:")
    for _, row in latest_predictions.tail(3).iterrows():
        print(f"   {row['coin_id']:12s}: {row['probability']:.4f} → {row['direction']:7s} ({row['confidence_level']:15s}) - {row['recommendation']}")
    
    print("\n" + "="*80)
    print("✅ Latest ML predictions successfully retrieved from Gold table")
    print("="*80)
else:
    print("\n⚠️ No predictions found in Gold table")