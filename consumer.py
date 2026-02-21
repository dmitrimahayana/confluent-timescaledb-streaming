import json
import os
import psycopg2
import psycopg2.extras
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaException
from typing import Any, Dict, List, Tuple


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "stock-topic"
GROUP_ID = "stock-group"

DB_DSN = os.getenv(
    "TIMESCALE_DSN",
    "postgresql://postgres:postgres@localhost:25432/postgres"
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "2.0"))


UPSERT_SQL = """
INSERT INTO ohlcv_bars (
  symbol, ts, window_start, window_end, open, high, low, close, volume, vwap
) VALUES %s
ON CONFLICT (symbol, ts) DO UPDATE SET
  window_start = EXCLUDED.window_start,
  window_end   = EXCLUDED.window_end,
  open         = EXCLUDED.open,
  high         = EXCLUDED.high,
  low          = EXCLUDED.low,
  close        = EXCLUDED.close,
  volume       = EXCLUDED.volume,
  vwap         = EXCLUDED.vwap; 
"""

def parse_message_value(raw: bytes) -> Dict[str, Any]:
    """
    Expect JSON bytes from Kafka.
    Example fields:
    {
      "symbol":"AAPL",
      "window_start": 1700000000000,
      "window_end":   1700000060000,
      "open": 1.23, "high": 1.30, "low": 1.20, "close": 1.25,
      "volume": 123.45, "vwap": 1.245
    }
    """
    return json.loads(raw.decode("utf-8"))


def ms_to_utc_ts(ms: int) -> datetime:
    # Timescale hypertable time column (timestamptz)
    ts = datetime.fromtimestamp(ms / 1000.0, tz=ZoneInfo("Asia/Jakarta"))
    return ts


def to_row(v: Dict[str, Any]) -> Tuple:
    window_start_ms = int(v["window_start"])
    return (
        v["symbol"],
        ms_to_utc_ts(window_start_ms),  # ✅ ts derived from window_start
        window_start_ms,
        int(v["window_end"]),
        float(v["open"]),
        float(v["high"]),
        float(v["low"]),
        float(v["close"]),
        float(v["volume"]),
        float(v["vwap"]),
    )


def flush_rows(conn, rows: List[Tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            UPSERT_SQL,
            rows,
            page_size=min(len(rows), 1000),
        )
    conn.commit()


if __name__ == "__main__":
    # DB connection
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    buffer_rows: List[Tuple] = []
    last_flush = time.time()

    # Consumer config
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # ✅ commit only after DB commit
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    })
    consumer.subscribe([TOPIC])
    print(f"Listening on topic={TOPIC}, group={GROUP_ID} ... Ctrl+C to stop")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Periodic flush when idle
                if buffer_rows and (time.time() - last_flush) >= FLUSH_INTERVAL_SEC:
                    flush_rows(conn, buffer_rows)
                    consumer.commit(asynchronous=False)  # ✅ safe commit
                    buffer_rows.clear()
                    last_flush = time.time()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else ""
            payload = parse_message_value(msg.value())
            buffer_rows.append(to_row(payload))

            if len(buffer_rows) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL_SEC:
                flush_rows(conn, buffer_rows)
                consumer.commit(asynchronous=False)  # ✅ commit after DB success
                buffer_rows.clear()
                last_flush = time.time()

            print(f"⬇️ {msg.topic()}[{msg.partition()}] offset={msg.offset()} key={key} value={payload}")

    except KeyboardInterrupt:
        print("Stopping...")
        # Final flush before exit
        if buffer_rows:
            flush_rows(conn, buffer_rows)
            consumer.commit(asynchronous=False)

    finally:
        try:
            consumer.close()
        finally:
            conn.close()
