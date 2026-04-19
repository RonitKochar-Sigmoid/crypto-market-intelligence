# Databricks notebook source
# DBTITLE 1,Crypto ML Pipeline (V3 Only)
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, lag, lead, avg, stddev, sum as spark_sum, lit, when, abs as spark_abs, from_unixtime, min as spark_min, max as spark_max
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score

print("="*80)
print("🚀 CRYPTO ML V3 MODEL TRAINING PIPELINE")
print("="*80)

# COMMAND ----------
# DBTITLE 1,Configuration
COINS = ['bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot', 'chainlink', 'dogecoin', 'pepe', 'uniswap', 'binancecoin']
BRONZE_TABLE = 'finalcrypto.bronze.crypto_prices'
GOLD_FEATURES_TABLE = 'finalcrypto.gold.crypto_features_v3'

# COMMAND ----------
# DBTITLE 1,Feature Engineering (8h Horizon)
df = spark.table(BRONZE_TABLE).orderBy('coin_id', 'timestamp_ms')

window_1h = Window.partitionBy('coin_id').orderBy('timestamp_ms').rowsBetween(-1, -1)
window_6h = Window.partitionBy('coin_id').orderBy('timestamp_ms').rowsBetween(-6, -1)
window_24h = Window.partitionBy('coin_id').orderBy('timestamp_ms').rowsBetween(-24, -1)

# Core technical indicators
df = df.withColumn('momentum_1h', (col('price_usd') / avg('price_usd').over(window_1h)) - 1) \
       .withColumn('momentum_6h', (col('price_usd') / avg('price_usd').over(window_6h)) - 1) \
       .withColumn('momentum_24h', (col('price_usd') / avg('price_usd').over(window_24h)) - 1) \
       .withColumn('volume_momentum_1h', (col('total_volume') / avg('total_volume').over(window_1h)) - 1) \
       .withColumn('volume_momentum_6h', (col('total_volume') / avg('total_volume').over(window_6h)) - 1) \
       .withColumn('volume_momentum_24h', (col('total_volume') / avg('total_volume').over(window_24h)) - 1)

# Target Construction (8 hours ahead for V3)
df = df.withColumn('future_price_8h', lead('price_usd', 8).over(Window.partitionBy('coin_id').orderBy('timestamp_ms')))
df = df.withColumn('target', when(col('future_price_8h') > col('price_usd'), 1).otherwise(0))

df = df.dropna()

print("💾 Writing to Gold Features Table...")
df.write.format('delta').mode('overwrite').saveAsTable(GOLD_FEATURES_TABLE)

# COMMAND ----------
# DBTITLE 1,Temporal Train/Val/Test Split
df_pandas = spark.table(GOLD_FEATURES_TABLE).toPandas()

# [CRITICAL BUG FIX]: Ensure categorical integer maps EXACTLY align with Production Inference Pipeline
coin_mapping = {coin: idx for idx, coin in enumerate(COINS)}
df_pandas['coin_id_encoded'] = df_pandas['coin_id'].map(coin_mapping)

df_pandas = df_pandas.sort_values(by=['coin_id', 'timestamp'])

# Slice data without leakage
total_rows = len(df_pandas)
train_idx = int(total_rows * 0.77)  # roughly 140 days
val_idx = int(total_rows * 0.88)    # roughly 20 days

df_train = df_pandas.iloc[:train_idx].copy()
df_val = df_pandas.iloc[train_idx:val_idx].copy()
df_test = df_pandas.iloc[val_idx:].copy()

print(f"Train: {len(df_train)} records | Val: {len(df_val)} records | Test: {len(df_test)} records")

# Persist Splits
for name, data in [('train', df_train), ('val', df_val), ('test', df_test)]:
    spark.createDataFrame(data).write.format('delta').mode('overwrite').saveAsTable(f'finalcrypto.gold.crypto_{name}_v3')

# COMMAND ----------
# DBTITLE 1,Hyperopt & LightGBM Environment Tuning
features = ['momentum_1h', 'momentum_6h', 'momentum_24h', 'volume_momentum_1h', 
            'volume_momentum_6h', 'volume_momentum_24h', 'coin_id_encoded']

X_train, y_train = df_train[features], df_train['target']
X_val, y_val = df_val[features], df_val['target']

space = {
    'learning_rate': hp.loguniform('learning_rate', np.log(0.01), np.log(0.2)),
    'max_depth': scope.int(hp.quniform('max_depth', 3, 10, 1)),
    'num_leaves': scope.int(hp.quniform('num_leaves', 10, 100, 1)),
    'colsample_bytree': hp.uniform('colsample_bytree', 0.6, 1.0),
    'subsample': hp.uniform('subsample', 0.6, 1.0)
}

def objective(params):
    model = lgb.LGBMClassifier(**params, n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict(X_val)
    return {'loss': -f1_score(y_val, preds, average='macro'), 'status': STATUS_OK}

print("Running Hyperopt for optimal V3 parameter configuration...")
trials = Trials()
best_params = fmin(fn=objective, space=space, algo=tpe.suggest, max_evals=50, trials=trials)

best_params['max_depth'] = int(best_params['max_depth'])
best_params['num_leaves'] = int(best_params['num_leaves'])

# Final Logging & Unity Catalog Registration
mlflow.set_experiment("/Users/<<YOUR_EMAIL>>/crypto_price_prediction_v3")
with mlflow.start_run() as run:
    model = lgb.LGBMClassifier(**best_params, n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    mlflow.lightgbm.log_model(model, "model", registered_model_name="finalcrypto.models.crypto_price_classifier_v3")
    print("✅ Best Model trained & registered successfully!")

# COMMAND ----------
# DBTITLE 1,Test Evaluation
X_test, y_test = df_test[features], df_test['target']
metrics = {
    'Accuracy': accuracy_score(y_test, model.predict(X_test)),
    'AUC': roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
}
print(f"📊 Final Holdout Test Metrics (V3): {metrics}")
