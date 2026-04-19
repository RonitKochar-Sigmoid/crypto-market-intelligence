from flask import Flask, jsonify, request
from databricks import sql
import os
from datetime import datetime

app = Flask(__name__)

# Databricks connection settings
SERVER_HOSTNAME = os.getenv('DATABRICKS_SERVER_HOSTNAME', '<<YOUR_DATABRICKS_WORKSPACE_URL>>')
HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/<<YOUR_WAREHOUSE_ID>>')  
ACCESS_TOKEN = os.getenv('DATABRICKS_TOKEN') 
# Table configuration
PREDICTIONS_TABLE = 'finalcrypto.gold.crypto_predictions_live'
FEATURES_TABLE = 'finalcrypto.gold.crypto_features_v3'
BRONZE_TABLE = 'finalcrypto.bronze.crypto_prices'

# Coin symbol mapping
SYMBOL_MAP = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'SOL': 'solana',
    'ADA': 'cardano',
    'DOT': 'polkadot',
    'LINK': 'chainlink',
    'DOGE': 'dogecoin',
    'PEPE': 'pepe',
    'UNI': 'uniswap',
    'BNB': 'binancecoin'
}

def get_db_connection():
    """Create Databricks SQL connection"""
    if not ACCESS_TOKEN:
        raise ValueError("DATABRICKS_TOKEN environment variable is required. Generate one from User Settings > Access Tokens")
    
    return sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN
    )

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Crypto Price Prediction API',
        'timestamp': datetime.now().isoformat(),
        'warehouse': 'Serverless Starter Warehouse'
    }), 200

@app.route('/coins/<symbol>/prediction', methods=['GET'])
def get_coin_prediction(symbol):
    """
    GET /coins/{symbol}/prediction
    Returns: Predicted price and trend
    
    Example: GET /coins/BTC/prediction
    """
    try:
        symbol_upper = symbol.upper()
        coin_id = SYMBOL_MAP.get(symbol_upper)
        
        if not coin_id:
            return jsonify({
                'success': False,
                'error': f'Unknown symbol: {symbol}. Available: {", ".join(SYMBOL_MAP.keys())}'
            }), 404
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Get latest prediction
                query = f"""
                SELECT 
                    p.coin_id,
                    p.timestamp,
                    p.direction,
                    p.confidence_level,
                    p.probability,
                    p.recommendation,
                    b.price_usd,
                    b.price_change_percentage_24h
                FROM {PREDICTIONS_TABLE} p
                LEFT JOIN {BRONZE_TABLE} b 
                  ON p.coin_id = b.coin_id 
                  AND b.timestamp = (SELECT MAX(timestamp) FROM {BRONZE_TABLE} WHERE coin_id = '{coin_id}')
                WHERE p.coin_id = '{coin_id}'
                ORDER BY p.timestamp DESC
                LIMIT 1
                """
                cursor.execute(query)
                row = cursor.fetchone()
                
                if not row:
                    return jsonify({
                        'success': False,
                        'error': f'No predictions found for {symbol_upper}'
                    }), 404
                
                return jsonify({
                    'success': True,
                    'symbol': symbol_upper,
                    'coin_id': row[0],
                    'timestamp': row[1].isoformat(),
                    'prediction': {
                        'direction': row[2],
                        'trend': 'BULLISH' if row[2] == 'UP' else 'BEARISH' if row[2] == 'DOWN' else 'NEUTRAL',
                        'confidence': row[3],
                        'probability': float(row[4]),
                        'recommendation': row[5]
                    },
                    'price': {
                        'current_usd': float(row[6]) if row[6] else None,
                        'change_24h_pct': float(row[7]) if row[7] else None
                    }
                }), 200
                
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/coins', methods=['GET'])
def list_coins():
    """List all available coins"""
    return jsonify({
        'success': True,
        'count': len(SYMBOL_MAP),
        'coins': [{'symbol': k, 'coin_id': v} for k, v in SYMBOL_MAP.items()]
    }), 200

if __name__ == '__main__':
    print("Crypto Prediction API - Starting Server")
    print("=" * 80)
    print(f"Server: {SERVER_HOSTNAME}")
    print(f"Warehouse: Serverless Starter Warehouse")
    print(f"HTTP Path: {HTTP_PATH}")
    print()
    print("Endpoints:")
    print("   GET  /health")
    print("   GET  /coins")
    print("   GET  /coins/<symbol>/prediction")
    print()
    print("Examples:")
    print("   curl http://localhost:5000/health")
    print("   curl http://localhost:5000/coins")
    print("   curl http://localhost:5000/coins/BTC/prediction")
    print()
    print("IMPORTANT: Make sure to set DATABRICKS_TOKEN environment variable!")
    print("   export DATABRICKS_TOKEN='your-token-here'")
    print()
    print("=" * 80)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
