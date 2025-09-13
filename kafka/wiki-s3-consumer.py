# kafka/wiki-s3-consumer.py
# Kafka → S3 sink (batched, gzip, NDJSON).
# Consumes Wikipedia changes, buffers to size/time thresholds, and writes to
# s3://$S3_BUCKET/$S3_PREFIX/ds=YYYY-MM-DD/part-<epoch-ms>.json.gz

import os, sys, json, time, gzip
from datetime import datetime, timezone
import boto3
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

TOPIC = "wikipedia-changes"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = "wiki-s3-writer"
AUTO_OFFSET_RESET = "earliest"

S3_BUCKET = os.getenv("S3_BUCKET")          # required
S3_PREFIX  = os.getenv("S3_PREFIX", "raw")

# Tunables (env override)
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_SECS", "10"))

def wait_for_topic(bootstrap: str, topic: str, max_wait_sec: int = 60):
    """Block until topic metadata shows partitions (avoids startup race)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    start, backoff = time.time(), 0.5
    while time.time() - start < max_wait_sec:
        md = admin.list_topics(timeout=5)
        t = md.topics.get(topic)
        if t and not t.error and t.partitions:
            print(f"Topic '{topic}' is ready with {len(t.partitions)} partition(s).")
            return
        print("Topic not ready yet, retrying...")
        time.sleep(backoff)
        backoff = min(backoff * 1.5, 5)
    raise RuntimeError(f"Topic '{topic}' not available after {max_wait_sec}s.")

def s3_put_bytes(s3, key: str, data: bytes):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)

def flush_batch(s3, buf):
    """Write current buffer to a date-partitioned, gzipped NDJSON object."""
    if not buf:
        return
    now = datetime.now(timezone.utc)
    ds = now.strftime("%Y-%m-%d")
    ts_ms = int(now.timestamp() * 1000)  # millisecond suffix to avoid collisions
    key = f"{S3_PREFIX}/ds={ds}/part-{ts_ms}.json.gz"
    payload = ("\n".join(json.dumps(x) for x in buf)).encode("utf-8")
    s3_put_bytes(s3, key, gzip.compress(payload))
    print(f"Wrote {len(buf)} records -> s3://{S3_BUCKET}/{key}")

def main():
    if not S3_BUCKET:
        print("ERROR: S3_BUCKET env var is required.", file=sys.stderr)
        sys.exit(2)

    wait_for_topic(BOOTSTRAP, TOPIC)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": True,  # at-least-once with periodic commits
    })
    consumer.subscribe([TOPIC])
    print(f"Subscribed to {TOPIC}. Consuming...")

    s3 = boto3.client("s3")

    buf, last_flush = [], time.time()
    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            # time-based flush even without new messages
            if msg is None:
                if buf and (now - last_flush >= FLUSH_INTERVAL_SEC):
                    flush_batch(s3, buf); buf.clear(); last_flush = now
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                time.sleep(1)
                continue

            # decode → buffer
            try:
                buf.append(json.loads(msg.value().decode("utf-8")))
            except Exception:
                continue

            # size/time thresholds
            if len(buf) >= BATCH_SIZE or (now - last_flush >= FLUSH_INTERVAL_SEC):
                flush_batch(s3, buf); buf.clear(); last_flush = now
    finally:
        try:
            if buf:
                flush_batch(s3, buf)
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
