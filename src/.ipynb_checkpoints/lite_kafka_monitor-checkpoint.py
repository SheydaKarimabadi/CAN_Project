#!/usr/bin/env python3
"""
lite_kafka_monitor.py

Lightweight Kafka monitor (NO Prometheus, NO Grafana)
Shows:
  • throughput (msgs/sec)
  • consumer lag
  • lag rate
  • avg latency (if message contains a timestamp)
  • bottleneck warnings
"""

import time
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, TopicPartition
from dateutil import parser

def utc_ts():
    return datetime.now(timezone.utc).timestamp()

def extract_timestamp(value_bytes):
    """Try to extract message timestamp from payload"""
    try:
        data = json.loads(value_bytes.decode("utf-8"))
        ts = data.get("timestamp") or data.get("ts")
        if ts:
            # اگر عدد بود float می‌کنیم، اگر string ISO بود parse می‌کنیم
            try:
                return float(ts)
            except:
                return parser.isoparse(ts).timestamp()
    except:
        pass
    return None

def get_high_watermarks(consumer, topic):
    """Fetch end offsets (HWMs) for topic partitions"""
    md = consumer.list_topics(timeout=5)
    parts = md.topics[topic].partitions.keys()
    tp_list = [TopicPartition(topic, p) for p in parts]
    return {p.partition: consumer.get_watermark_offsets(p)[1] for p in tp_list}

def get_committed_offsets(consumer, group, topic):
    """Fetch committed offsets for a group"""
    c = Consumer({
        "bootstrap.servers": consumer_config["bootstrap.servers"],
        "group.id": group,
        "enable.auto.commit": False
    })
    md = c.list_topics(timeout=5)
    parts = md.topics[topic].partitions.keys()
    tps = [TopicPartition(topic, p) for p in parts]
    committed = c.committed(tps, timeout=5)
    c.close()
    return {tp.partition: (tp.offset if tp.offset >= 0 else 0) for tp in committed}

def measure_latency(consumer, topic, samples=50):
    """Consume a few messages and estimate avg latency"""
    consumer.assign([TopicPartition(topic, p) for p in range(20)])  # assign broad range
    collected = []
    start = time.time()
    while len(collected) < samples and (time.time() - start) < 2:
        msg = consumer.poll(0.1)
        if not msg:
            continue
        ts = extract_timestamp(msg.value())
        if ts:
            collected.append(utc_ts() - ts)
    return sum(collected)/len(collected) if collected else None

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--groups", default="")
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--latency-samples", type=int, default=50)
    args = parser.parse_args()

    groups = [g.strip() for g in args.groups.split(",") if g.strip()]

    global consumer_config
    consumer_config = {
        "bootstrap.servers": args.bootstrap,
        "group.id": "monitor-tool",
        "enable.auto.commit": False
    }

    consumer = Consumer(consumer_config)

    print(f"\n📡 Kafka Monitor Started")
    print(f"Topic: {args.topic}")
    print(f"Consumer Groups: {groups}")
    print(f"Interval: {args.interval}s\n")

    prev_hwm = get_high_watermarks(consumer, args.topic)
    prev_lags = {g: 0 for g in groups}

    while True:
        time.sleep(args.interval)

        # ---- THROUGHPUT ----
        curr_hwm = get_high_watermarks(consumer, args.topic)
        throughput = {
            p: (curr_hwm[p] - prev_hwm.get(p, 0)) / args.interval
            for p in curr_hwm
        }
        prev_hwm = curr_hwm

        # ---- LAG ----
        lag_report = {}
        lag_rate_report = {}
        for g in groups:
            committed = get_committed_offsets(consumer, g, args.topic)
            lag = {p: curr_hwm[p] - committed.get(p, 0) for p in curr_hwm}
            lag_report[g] = lag

            total_lag = sum(lag.values())
            lag_rate = (total_lag - prev_lags[g]) / args.interval  # مثبت یعنی lag داره زیاد میشه
            lag_rate_report[g] = lag_rate
            prev_lags[g] = total_lag

        # ---- LATENCY ----
        latency = measure_latency(consumer, args.topic, samples=args.latency_samples)

        # ---- PRINT REPORT ----
        print("\n==============================")
        print(f"⏱ {datetime.now().strftime('%H:%M:%S')}")

        print("\n🔥 Throughput (msg/sec):")
        for p, t in throughput.items():
            print(f"  • partition {p}: {t:.2f}")

        print("\n🐢 Lag:")
        for g, lags in lag_report.items():
            total = sum(lags.values())
            print(f"  Group {g}: total={total}")
            for p, l in lags.items():
                print(f"     p{p}: {l}")
            print(f"  Lag Rate: {lag_rate_report[g]:.2f} msgs/sec")

        print("\n⏳ Avg Latency:")
        if latency is None:
            print("  (no timestamp found in messages)")
        else:
            print(f"  {latency:.4f} seconds")

        print("\n⚠️ Bottleneck Warnings:")
        for g in groups:
            if lag_rate_report[g] > 0:
                print(f"  ⚠️ Bottleneck detected in group {g}")

        print("==============================\n")
