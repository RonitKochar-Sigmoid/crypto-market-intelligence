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
    """
    Finalized: Dynamically finds the latest record from Notebook 03's 
    daily_price_summary output.
    """
    df = get_df(GOLD_DAILY_SUMMARY)
    coin_df = df[df["symbol"].astype(str).str.upper() == symbol.upper()]
    
    if coin_df.empty:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}. Try /coins.")
    
    # Notebook 03 uses 'event_timestamp' for the Window logic
    # We find the best available time column to sort by
    ts_col = next((c for c in ["event_timestamp", "date", "silver_processing_timestamp"] if c in coin_df.columns), coin_df.columns[0])
    
    latest_record = coin_df.sort_values(ts_col, ascending=False).iloc[0]
    return latest_record.to_dict()

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