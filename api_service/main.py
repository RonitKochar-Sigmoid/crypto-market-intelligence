import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from deltalake import DeltaTable
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Crypto Market Intelligence API")

# --- CONFIGURATION ---
# Based on Notebook 03: Gold Layer
STORAGE_ACCOUNT_NAME = "coinmarketcapapi"
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY") 

STORAGE_OPTIONS = {
    "azure_storage_account_name": STORAGE_ACCOUNT_NAME,
    "azure_storage_account_key": STORAGE_ACCOUNT_KEY,
}

BASE_URL = f"abfss://coin-market-cap-api-data@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
GOLD_DAILY_SUMMARY = f"{BASE_URL}/gold/daily_price_summary"
GOLD_VOLATILITY = f"{BASE_URL}/gold/volatility_metrics"
SILVER_GLOBAL_PATH = f"{BASE_URL}/silver/global_metrics"

def get_df(path):
    """Robust helper to read Delta and fix JSON serialization issues."""
    if not STORAGE_ACCOUNT_KEY:
        raise HTTPException(status_code=500, detail="AZURE_STORAGE_KEY missing in .env")
    
    try:
        dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
        df = dt.to_pandas()
        
        # 1. FIX SERIALIZATION: Convert all datetime objects to strings
        # This handles 'event_timestamp', 'date', or 'silver_processing_timestamp'
        for col_name in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                df[col_name] = df[col_name].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 2. FIX NaN: Replace Pandas NaNs with None so they become 'null' in JSON
        return df.where(pd.notnull(df), None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data Lake Error: {str(e)}")

# --- ENDPOINTS ---

@app.get("/")
def home():
    return {"message": "Crypto API Online", "docs": "/docs"}

@app.get("/health")
def health():
    return {"status": "healthy", "utc_now": datetime.utcnow().isoformat()}

@app.get("/coins")
def list_coins():
    """Verify available symbols and schema columns."""
    df = get_df(GOLD_DAILY_SUMMARY)
    return {
        "symbols": sorted(df["symbol"].unique().tolist()) if "symbol" in df.columns else [],
        "columns_found": df.columns.tolist()
    }

@app.get("/coins/{symbol}/latest")
def get_latest_metrics(symbol: str):
    df = get_df(GOLD_DAILY_SUMMARY)
    
    # 1. Filter by Symbol
    coin_df = df[df["symbol"].astype(str).str.upper() == symbol.upper()]
    
    if coin_df.empty:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}.")
    
    # 2. Sort by the confirmed column
    latest_record = coin_df.sort_values("event_timestamp", ascending=False).iloc[0]
    
    # 3. SERIALIZATION SAFETY: Convert everything to native Python types
    # This prevents the 'Object of type Timestamp/NaN is not JSON serializable' error
    clean_result = {}
    for key, value in latest_record.to_dict().items():
        if pd.api.types.is_number(value) and pd.isna(value):
            clean_result[key] = None  # Convert NaN to null
        elif hasattr(value, 'isoformat'):
            clean_result[key] = value.isoformat()  # Convert Timestamps to strings
        else:
            clean_result[key] = value

    return clean_result

@app.get("/coins/{symbol}/historical")
def get_historical_data(symbol: str, days: int = 7):
    """Returns historical price data for a coin for the last X days."""
    df = get_df(GOLD_DAILY_SUMMARY)
    
    # 1. Filter by symbol
    coin_df = df[df["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    
    if coin_df.empty:
        raise HTTPException(status_code=404, detail=f"No historical data for {symbol}")

    # 2. CONVERSION FIX: Use utc=True to match your Data Lake format
    coin_df["dt_obj"] = pd.to_datetime(coin_df["event_timestamp"], utc=True)
    
    # 3. FILTER FIX: Use a UTC-aware cutoff to prevent comparison errors
    cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=days)
    historical_df = coin_df[coin_df["dt_obj"] >= cutoff].sort_values("dt_obj", ascending=False)

    # 4. SERIALIZATION FIX: Manually build the list to handle Timestamps and NaNs
    results = []
    for _, row in historical_df.iterrows():
        item = row.to_dict()
        # Convert any problematic types to standard Python types
        for key, value in item.items():
            if pd.api.types.is_number(value) and pd.isna(value):
                item[key] = None
            elif hasattr(value, 'isoformat'):
                item[key] = value.isoformat()
        
        # Keep only necessary columns for a cleaner response
        filtered_item = {
            "event_timestamp": item.get("event_timestamp"),
            "price_usd": item.get("price_usd"),
            "daily_return": item.get("daily_return"),
            "seven_day_moving_avg": item.get("7d_moving_avg") # Maps from notebook name
        }
        results.append(filtered_item)

    return results

@app.get("/market/global")
def get_global_metrics():
    """Returns the latest total market cap and BTC dominance."""
    df = get_df(SILVER_GLOBAL_PATH)
    if df.empty:
        raise HTTPException(status_code=404, detail="Global metrics not found.")
    
    # Sort by time to get the absolute latest snapshot
    latest = df.sort_values("event_timestamp", ascending=False).iloc[0]
    return latest.to_dict()

@app.get("/market/rankings")
def get_rankings(top: int = 5):
    """Uses daily_return calculated in Gold Notebook 03."""
    df = get_df(GOLD_DAILY_SUMMARY)
    
    # Sort by the return metric calculated in Databricks
    if "daily_return" not in df.columns:
        raise HTTPException(status_code=500, detail="Column 'daily_return' not found in Gold table.")
        
    # Get the most recent snapshot
    ts_col = "event_timestamp" if "event_timestamp" in df.columns else df.columns[0]
    latest_ts = df[ts_col].max()
    latest_df = df[df[ts_col] == latest_ts].copy()

    gainers = latest_df.sort_values("daily_return", ascending=False).head(top)
    losers = latest_df.sort_values("daily_return", ascending=True).head(top)

    return {
        "timestamp": latest_ts,
        "top_gainers": gainers[["symbol", "price_usd", "daily_return"]].to_dict(orient="records"),
        "top_losers": losers[["symbol", "price_usd", "daily_return"]].to_dict(orient="records")
    }

@app.get("/coins/{symbol}/volatility")
def get_coin_volatility(symbol: str):
    """Uses rolling_std from the Gold Volatility table."""
    df = get_df(GOLD_VOLATILITY)
    coin_df = df[df["symbol"].astype(str).str.upper() == symbol.upper()]
    
    if coin_df.empty:
        raise HTTPException(status_code=404, detail=f"No risk metrics for {symbol}")
        
    ts_col = "event_timestamp" if "event_timestamp" in coin_df.columns else coin_df.columns[0]
    return coin_df.sort_values(ts_col, ascending=False).iloc[0].to_dict()