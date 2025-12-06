import json
import time

from kafka import KafkaConsumer

BATCH_SIZE = 1_00_00
TOPIC = "trades.BTCUSDT"


def run_batch():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id="batch-processor",
    )

    events = []

    print("\nRunning batch job...")
    start_time = time.perf_counter()

    for msg in consumer:
        event = msg.value
        events.append(event)
        if len(events) >= BATCH_SIZE:
            break

    if not events:
        print("No new events.")
        return

    # Calculate batch latency using event timestamps
    oldest = min(e["ts"] for e in events)
    newest = max(e["ts"] for e in events)
    now_ms = int(time.time() * 1000)

    batch_latency_oldest = (now_ms - oldest) / 1000.0
    batch_latency_newest = (now_ms - newest) / 1000.0

    # Compute average price
    avg_price = sum(e["price"] for e in events) / len(events)

    print(f"Batch size: {BATCH_SIZE}")
    print(f"Processed {len(events)} trades")
    print(f"Average price: {avg_price:.2f}")
    print(f"Latency of oldest event: {batch_latency_oldest:.2f} seconds")
    print(f"Latency of newest event: {batch_latency_newest:.2f} seconds\n")
    print(f"Total time spent: {time.perf_counter() - start_time} seconds")


if __name__ == "__main__":
    run_batch()
