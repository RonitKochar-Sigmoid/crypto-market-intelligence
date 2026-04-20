FROM python:3.9-slim

WORKDIR /app

# Ensure you have a requirements.txt in the same folder
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your ingestor scripts
COPY cmc_ingestor.py .
COPY coingecko_ingestor.py .

# Command is handled by the KubernetesPodOperator in the DAG
CMD ["python", "cmc_ingestor.py"]