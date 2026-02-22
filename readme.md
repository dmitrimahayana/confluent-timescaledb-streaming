# Kafka Confluent
## Get docker compose file
Download file in here: 
```
https://github.com/confluentinc/cp-all-in-one/blob/8.0.0-post/cp-all-in-one-community/docker-compose.yml
```
Navigate to docker compose location and run `docker compose up -d`

## Create a topic `(example: 3 partitions, replication factor 1)`
docker exec -it broker-confluent bash -lc \
'kafka-topics --bootstrap-server broker-confluent:9092 --create --topic stock-topic --partitions 3 --replication-factor 1'

## Verify it exists
docker exec -it broker-confluent bash -lc \
'kafka-topics --bootstrap-server broker-confluent:9092 --list'

## Describe it
docker exec -it broker-confluent bash -lc \
'kafka-topics --bootstrap-server broker-confluent:9092 --describe --topic stock-topic'

## Delete topic
docker exec -it broker-confluent bash -lc \
'kafka-topics --bootstrap-server broker-confluent:9092 --delete --topic stock-topic'

# Python ENV
## Prepare venv
uv venv ./venv --python 3.11

## Install packages
uv pip install -r requirements.txt

# Timescaledb
## Pull docker image
docker pull timescale/timescaledb-ha:pg18

## Create new db in timescaledb
docker run -d --name timescaledb -p 25432:5432  -v /Users/dmitri/Documents/data-engineer/timescaledb:/pgdata -e PGDATA=/pgdata -e POSTGRES_PASSWORD=postgres timescale/timescaledb-ha:pg18

## Create Timescaledb Table
Target table from Kafka consumer output
```
CREATE TABLE ohlcv_bars (
  symbol       text        NOT NULL,
  ts           timestamptz NOT NULL,          -- REAL column (not generated)
  window_start bigint      NOT NULL,          -- ms epoch
  window_end   bigint      NOT NULL,          -- ms epoch
  open         double precision NOT NULL,
  high         double precision NOT NULL,
  low          double precision NOT NULL,
  close        double precision NOT NULL,
  volume       double precision NOT NULL,
  vwap         double precision NOT NULL,
  ingestion_ts timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, ts)
);
```
Create hypertable in timescaledb
```
SELECT create_hypertable('ohlcv_bars', 'ts', if_not_exists => TRUE);
```
Create Indexing column
```
CREATE INDEX IF NOT EXISTS ohlcv_bars_symbol_ts_idx
ON ohlcv_bars (symbol, ts DESC);
```
## Create Timescaledb Aggregation
Create Streaming M.view
```
CREATE MATERIALIZED VIEW ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT
  symbol,
  time_bucket('1 minute', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume)      AS volume,
  -- simple VWAP aggregation: sum(price*vol) / sum(vol)
  CASE WHEN sum(volume) = 0 THEN NULL
       ELSE sum(vwap * volume) / sum(volume)
  END AS vwap
FROM ohlcv_bars
GROUP BY symbol, bucket;
```
Add Cronjob Refresh M.view
```
SELECT add_continuous_aggregate_policy('ohlcv_1m',
  start_offset => INTERVAL '1 hour',
  end_offset   => INTERVAL '10 seconds',
  schedule_interval => INTERVAL '1 minute'
);
```
Test the output
```
SELECT *
FROM ohlcv_1m
ORDER BY bucket DESC;
```

## Connect docker Timescaledb to Confluent network
```
docker network connect confluent_default timescaledb
```