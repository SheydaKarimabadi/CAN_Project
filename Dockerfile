FROM python:3.11-slim

# 1. Set working directory
WORKDIR /app


# only minimal dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 librdkafka-dev ca-certificates \
    build-essential \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt /app/requirements.txt

# Force pip to use pre-built wheels (NO COMPILATION)
RUN pip install --no-cache-dir -r requirements.txt                 #--only-binary=:all:

COPY src/ /app/src
COPY results/ /app/results
COPY data/ /app/data


# Set environment variables for Kafka
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV TOPIC_CAN_LOGS=can_logs

EXPOSE 8000
CMD ["bash"]

