import json
import time
import redis
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Counter, Histogram, Gauge, start_http_server






PRINT_LIMIT = 10
print_count = 0


# ---------------------------- CONFIG ----------------------------
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "can_raw"
REDIS_HOST = "redis"





REDIS_STREAM = "can_stream_history"


# ---------------------------- METRICS ----------------------------
MESSAGES = Counter("sink_messages_total", "Messages written to Redis stream")
ERRORS = Counter("sink_errors_total", "Errors during sink")
LATENCY = Histogram("sink_write_latency_seconds", "Write latency to Redis stream")
LAG = Gauge("sink_kafka_lag", "Kafka lag per partition", ["partition"])


# ---------------------------- CREATE CONSUMER ----------------------------
def create_consumer(group_id="redis_sink_group_debug"):
    cfg = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,           # group override to debug offsets
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    print("Kafka Consumer config =", cfg)
    return Consumer(cfg)


# ---------------------------- LAG CALC ----------------------------
def calc_lag(consumer):
    try:
        #partitions = [TopicPartition(KAFKA_TOPIC, p) for p in range(10)]
        md = consumer.list_topics(timeout=5)
        real_partitions = md.topics[KAFKA_TOPIC].partitions

        partitions = [
                    TopicPartition(KAFKA_TOPIC, p) for p in real_partitions.keys()
        ]

        committed = consumer.committed(partitions)

        for tp in committed:
            if tp.offset < 0:
                continue

            low, high = consumer.get_watermark_offsets(tp)
            lag = max(high - tp.offset, 0)
            LAG.labels(partition=str(tp.partition)).set(lag)

        print("Lag updated.")
    except Exception as e:
        print("Lag calc error:", e)


# ---------------------------- MAIN LOOP ----------------------------
def main():
    print("\n🚀 Starting Kafka → Redis sink (DEBUG MODE)…")
    print("Waiting for Kafka and Redis...")

    # Start metrics server
    start_http_server(8002)
    print("Metrics exposed at :8002/metrics")

    # Connect Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        r.ping()
        print("✔ Connected to Redis")
    except Exception as e:
        print("❌ Redis connection failed:", e)
        return

    # Connect Kafka
    consumer = create_consumer()
    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"✔ Subscribed to Kafka topic: {KAFKA_TOPIC}")
    except Exception as e:
        print("❌ Kafka subscription error:", e)
        return

    print("\n🔥 Sink loop starting… Listening for messages…\n")

    last_lag_calc = time.time()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("⏳ No message… waiting...")
            continue

        if msg.error():
            print("❌ Kafka error:", msg.error())
            ERRORS.inc()
            continue

        #print("📥 Received message:", msg.value())
        # --- print only first 10 messages ---
        global print_count
        if print_count < PRINT_LIMIT:
            print(f"📥 Received message #{print_count+1}: {msg.value()}")
            print_count += 1


        try:
            data = json.loads(msg.value())

            # IMPORTANT: Redis requires string values
            redis_safe = {k: str(v) for k, v in data.items()}

            with LATENCY.time():
                msg_id = r.xadd(REDIS_STREAM, redis_safe)

            print(f"✅ Wrote to Redis stream: {msg_id}")

            MESSAGES.inc()

        except Exception as e:
            print("❌ Sink write error:", e)
            ERRORS.inc()

        # Update lag every 5 sec
        if time.time() - last_lag_calc > 5:
            print("🧮 Calculating lag...")
            calc_lag(consumer)
            last_lag_calc = time.time()


if __name__ == "__main__":
    main()
