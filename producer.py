import asyncio
import json
import os

import websockets
from kafka import KafkaProducer

BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
TOPIC = "trades.BTCUSDT"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    allow_auto_create_topics=True,
)


async def stream_trades():
    async with websockets.connect(BINANCE_WS) as ws:
        print("Connected to Binance stream...")
        async for msg in ws:
            trade = json.loads(msg)

            event = {
                "symbol": trade["s"],
                "price": float(trade["p"]),
                "quantity": float(trade["q"]),
                "ts": trade["T"],
            }

            producer.send(TOPIC, event)
            print("Sent:", event)


asyncio.run(stream_trades())
