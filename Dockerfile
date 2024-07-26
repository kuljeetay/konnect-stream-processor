FROM python:3.9-slim

ENV KAFKA_TOPIC="cdc-events"
ENV KAFKA_HOST="localhost:9092"
ENV OPENSEARCH_HOST="localhost"
ENV OPENSEARCH_PORT=9200

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "consumer.py"]