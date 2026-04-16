FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Update these two lines:
COPY event_hub_producer.py .
CMD ["python", "event_hub_producer.py"]