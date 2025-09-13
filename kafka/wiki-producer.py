# kafka/wiki-producer.py
# Wikipedia RecentChanges → Kafka.
# Streams the Wikimedia SSE feed and publishes a compact JSON to `TOPIC`.
# Designed for GitOps: env-configured, resilient HTTP (retry/keep-alive),
# explicit User-Agent (required by WMF), at-least-once delivery with keys,
# and an exponential backoff reconnect loop.

import json, os, textwrap, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "wikipedia-changes"
URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = os.getenv(
    "USER_AGENT",
    "wiki-pipeline/1.0 (contact: your-email@example.com)",  # set to your email or repo URL
)

def delivery_callback(err, msg):
    """Log per-message delivery result (useful when tailing container logs)."""
    if err:
        print(f"ERROR delivery: {err}")
    else:
        try:
            k = msg.key().decode()
        except Exception:
            k = str(msg.key())
        print(textwrap.dedent(f"Produced to {msg.topic()} key={k}"))

def sse_events(url):
    """Yield parsed SSE events from the Wikimedia stream."""
    s = requests.Session()
    s.headers.update({
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": USER_AGENT,         # WMF requires an identifying UA
        "Connection": "keep-alive",
    })
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))

    with s.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        event = {}
        for line in r.iter_lines(decode_unicode=True):
            if line is None:
                continue
            if not line:
                if "data" in event:
                    yield event
                event = {}
                continue
            if line.startswith(":"):  # heartbeat
                continue
            field, _, value = line.partition(":")
            value = value.lstrip(" ")
            if field == "event":
                event["event"] = value
            elif field == "data":
                event["data"] = (event.get("data", "") + ("\n" if "data" in event else "") + value)
            elif field == "id":
                event["id"] = value

def run_once():
    print(f"Streaming {URL} -> Kafka topic '{TOPIC}'")
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "wiki-producer",
    })
    for ev in sse_events(URL):
        if ev.get("event") != "message" or not ev.get("data"):
            continue
        try:
            data = json.loads(ev["data"])
        except json.JSONDecodeError:
            continue

        msg = {
            "id": data.get("id"),
            "type": data.get("type"),
            "title": data.get("title"),
            "user": data.get("user"),
            "bot": data.get("bot"),
            "timestamp": data.get("timestamp"),
            "comment": data.get("comment"),
            "minor": data.get("minor", False),
        }

        # Stable key = change id (good for partitioning/compaction downstream)
        producer.produce(
            topic=TOPIC,
            key=str(msg["id"]),
            value=json.dumps(msg),
            callback=delivery_callback,
        )
        producer.poll(0)  # serve delivery callbacks
    producer.flush()

if __name__ == "__main__":
    # Robust reconnect with exponential backoff.
    backoff = 1
    while True:
        try:
            run_once()
            backoff = 1
        except Exception as e:
            print(f"SSE error: {e}; reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
