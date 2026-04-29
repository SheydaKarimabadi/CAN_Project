import json
import time
import redis
import joblib
import pandas as pd
import numpy as np
from confluent_kafka import Consumer
from datetime import datetime
from prometheus_client import start_http_server, Counter

# ---------------------- Prometheus Metrics ----------------------
PREDICTIONS = Counter("predictions_total", "Total predictions made")
start_http_server(8001)  # 🔥 Expose metrics on port 8001

# ---------------------- Config ----------------------
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "can_raw"
REDIS_HOST = "redis"
REDIS_STREAM = "can_stream_pred"

MODEL_PATH = "/app/results/model.pkl"
ENCODER_PATH = "/app/results/label_encoder.pkl"

# ---------------- Load model ----------------
model = joblib.load(MODEL_PATH)
label_encoder = joblib.load(ENCODER_PATH)

FEATURE_COLUMNS = [
    "timestamp",
    "can_id",
    "dlc",
    "byte_0",
    "byte_1",
    "byte_2",
    "byte_3",
    "byte_4",
    "byte_5",
    "byte_6",
    "byte_7",
    "entropy",
    "transition_rate",
    "non_zero_count"
]


def create_consumer():
    cfg = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "predictor_group",
        "auto.offset.reset": "latest",
    }
    return Consumer(cfg)


def preprocess_message(msg_dict):
    """Convert a raw Kafka message to a model-ready input row."""

    # Convert timestamp string → int64 (microseconds since epoch)
    if isinstance(msg_dict["timestamp"], str):
        dt = datetime.fromisoformat(msg_dict["timestamp"])
        msg_dict["timestamp"] = int(dt.timestamp() * 1_000_000)

    # Build DataFrame with correct column order
    df = pd.DataFrame([[msg_dict[col] for col in FEATURE_COLUMNS]],
                      columns=FEATURE_COLUMNS)

    # Make categorical columns same format as training
    df["can_id"] = df["can_id"].astype("category")
    df["dlc"] = df["dlc"].astype("category")

    return df


def main():
    print("🚀 Starting Kafka Predictor (LightGBM)...")

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            data = json.loads(msg.value())

            # ---- Preprocess and predict ----
            X = preprocess_message(data)
            pred_num = model.predict(X)[0]
            pred_label = label_encoder.inverse_transform([pred_num])[0]

            # Add prediction to data
            data["predicted_label"] = pred_label

            # ---- Write to Redis ----
            r.xadd(REDIS_STREAM, data)

            print(f"Predicted → {pred_label}")
            # ---------------- Prometheus Count ----------------
            PREDICTIONS.inc()

        except Exception as e:
            print("Prediction error:", e)


if __name__ == "__main__":
    main()
