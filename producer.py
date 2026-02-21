from confluent_kafka import Producer
from dataclasses import dataclass, asdict
import json
import random
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "stock-topic"

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


@dataclass
class OHLCVBar:
    symbol: str
    window_start: int
    window_end: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trades: int
    first_trade_time: int
    last_trade_time: int


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def generate_random_bar(symbols: list, last_price: float):
    jakarta = ZoneInfo("Asia/Jakarta")
    now = datetime.now(jakarta)
    window_start = int(now.timestamp() * 1000)
    window_end = int((now + timedelta(minutes=1)).timestamp() * 1000)

    # Random walk
    open_price = last_price
    close_price = open_price + random.uniform(-0.5, 0.5)
    high_price = max(open_price, close_price) + random.uniform(0, 0.3)
    low_price = min(open_price, close_price) - random.uniform(0, 0.3)
    volume = random.uniform(100, 1000)

    vwap = (open_price + high_price + low_price + close_price) / 4

    bar = OHLCVBar(
        symbol=random.choice(symbols),
        window_start=window_start,
        window_end=window_end,
        open=round(open_price, 5),
        high=round(high_price, 5),
        low=round(low_price, 5),
        close=round(close_price, 5),
        volume=round(volume, 2),
        vwap=round(vwap, 5),
        trades=1,
        first_trade_time=window_start,
        last_trade_time=window_end
    )

    return bar, close_price


if __name__ == "__main__":
    symbols = ["NVDA", "AAPL", "MSFT", "GOOG", "META"]
    last_price = 1.10000

    print("Starting stock producer...")
    random.seed(42)

    while True:
        bar, last_price = generate_random_bar(symbols, last_price)

        producer.produce(
            TOPIC,
            key=bar.symbol,
            value=json.dumps(asdict(bar)).encode("utf-8"),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(5)

