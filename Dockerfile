FROM python:3.9-slim

ENV KAFKA_TOPIC="cdc-events"
ENV KAFKA_HOST="kafka:29092"
ENV OPENSEARCH_HOST="opensearch-node"
ENV OPENSEARCH_PORT=9200

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "src/consumer/main.py"]