import polars as pl
import time
import json
from datetime import datetime
from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, start_http_server


# =====================
#  CONFIG
# =====================
KAFKA_TOPIC = "can_raw"
BOOTSTRAP = "kafka:9092"
BATCH_SIZE = 500

# =====================
#  Prometheus metrics
# =====================
MSG_SENT = Counter("producer_messages_sent_total", "Total messages sent to Kafka")
MSG_ERROR = Counter("producer_messages_error_total", "Errors sending messages")
SEND_LAT = Histogram("producer_send_latency_seconds", "Kafka produce latency (seconds)")
SLEEP_LAT = Histogram("producer_inter_message_sleep_seconds", "Time slept to preserve timing")

start_http_server(8000)   # /metrics on port 8000

# =====================
#  Kafka Producer
# =====================
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "queue.buffering.max.ms": 50,
    "linger.ms": 5,
    "batch.size": 100000,
    "compression.type": "lz4"
})

# =====================
#  Delivery Callback
# =====================
def delivery_report(err, msg):
    if err is not None:
        MSG_ERROR.inc()
    else:
        MSG_SENT.inc()

# =====================
#  Main
# =====================
def main():
    print("📌 Streaming dataset in batches...")

    # استفاده از LazyFrame برای جلوگیری از بارگذاری کامل در حافظه
    lf = pl.scan_parquet("/app/data/concat_fuzzy_Dos.parquet")

    # اولین timestamp برای شیفت زمانی
    first_row = lf.select("timestamp").limit(1).collect()
    original_start = first_row[0, 0]

    NOW = datetime.now()
    new_start = datetime(NOW.year, NOW.month, NOW.day,
                         original_start.hour,
                         original_start.minute,
                         original_start.second)

    last_ts = None

    # خواندن داده‌ها به صورت batch
    for batch_df in lf.collect(streaming=True).iter_slices(BATCH_SIZE):
        # تبدیل timestamp به datetime و شیفت زمانی
        ts_list = []
        rows = []
        for row in batch_df.rows(named=True):
            ts = row["timestamp"]
            ts_shifted = ts - original_start + new_start
            ts_list.append(ts_shifted)

            # تبدیل همه datetimeها به string قبل از json.dumps
            row_fixed = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}
            rows.append(row_fixed)

        # REALTIME pacing
        if last_ts is not None:
            delta = (ts_list[0] - last_ts).total_seconds()
            if delta > 0:
                with SLEEP_LAT.time():
                    time.sleep(delta)
        last_ts = ts_list[0]

        # ارسال پیام‌ها به Kafka
        for row, ts in zip(rows, ts_list):
            row["timestamp"] = ts.isoformat()
            with SEND_LAT.time():
                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=json.dumps(row).encode("utf-8"),
                    callback=delivery_report
                )

        producer.flush()
        print(f"Sent batch of {len(rows)} messages")

    print("✅ Finished streaming all data.")

if __name__ == "__main__":
    main()
