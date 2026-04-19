# 🚀 Crypto Prediction API

REST API for cryptocurrency price predictions powered by ML models running on Databricks.

## 📋 Requirement Satisfaction

**Project Requirement:** `GET /coins/{symbol}/prediction` - Returns predicted price and trend

✅ **SATISFIED** - This API implements the required endpoint with live ML predictions.

---

## 🎯 API Endpoints

### 1. Health Check
```
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "Crypto Price Prediction API",
  "timestamp": "2026-04-17T17:30:00.123456",
  "warehouse": "Serverless Starter Warehouse"
}
```

### 2. List Available Coins
```
GET /coins
```

**Response:**
```json
{
  "success": true,
  "count": 10,
  "coins": [
    {"symbol": "BTC", "coin_id": "bitcoin"},
    {"symbol": "ETH", "coin_id": "ethereum"},
    ...
  ]
}
```

### 3. Get Coin Prediction ⭐ **MAIN ENDPOINT**
```
GET /coins/{symbol}/prediction
```

**Parameters:**
- `symbol` (path) - Coin symbol: BTC, ETH, SOL, ADA, DOT, LINK, DOGE, PEPE, UNI, BNB

**Response:**
```json
{
  "success": true,
  "symbol": "BTC",
  "coin_id": "bitcoin",
  "timestamp": "2026-04-17T10:28:46",
  "prediction": {
    "direction": "NEUTRAL",
    "trend": "NEUTRAL",
    "confidence": "Low",
    "probability": 0.5,
    "recommendation": "HOLD"
  },
  "price": {
    "current_usd": 75642.0,
    "change_24h_pct": 1.20
  }
}
```

**Prediction Fields:**
- `direction`: UP | DOWN | NEUTRAL
- `trend`: BULLISH | BEARISH | NEUTRAL
- `confidence`: Very High | High | Medium-High | Medium | Low
- `probability`: 0.0 - 1.0 (ML model confidence score)
- `recommendation`: STRONG BUY | BUY | HOLD | SELL | STRONG SELL

---

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.8+
- Databricks workspace access
- SQL Warehouse access

### 1. Install Dependencies
```bash
pip install flask databricks-sql-connector
```

### 2. Generate Databricks Token
1. Go to Databricks workspace
2. Click profile icon (top right) → **Settings**
3. Navigate to **Developer** → **Access Tokens**
4. Click **Generate new token**
5. Set comment: "Crypto Prediction API"
6. Set lifetime: 90 days
7. Click **Generate** and copy the token

### 3. Configure Environment Variables

**Option A: Export (Recommended)**
```bash
export DATABRICKS_TOKEN='your-token-here'
```

**Option B: Create `.env` file**
```bash
echo "DATABRICKS_TOKEN=your-token-here" > .env
```

### 4. Start the API

**Option A: Using the quick start script**
```bash
chmod +x run_api.sh
./run_api.sh
```

**Option B: Manual start**
```bash
export DATABRICKS_TOKEN='your-token-here'
python3 crypto_prediction_api.py
```

The API will start on `http://localhost:5000`

---