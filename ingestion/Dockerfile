FROM python:3.9-slim

WORKDIR /app

# Copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy both ingestor scripts
COPY cmc_ingestor.py .
COPY coingecko_ingestor.py .

# We leave CMD empty or as a placeholder because the K8s Job 
# will override this to run a specific script.
CMD ["python", "cmc_ingestor.py"]