package com.amazonaws.services.msf.domain;

import java.util.Objects;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;


// @JsonIgnoreProperties(ignoreUnknown = true)
public class StockPrice {
    private String symbol;
    private long windowStart;
    private long windowEnd;
    private double open, high, low, close, volume, vwap;
    private int trades;
    private long firstTradeTime;
    private long lastTradeTime;

    public StockPrice() {}

    public String getSymbol() { return symbol; }

    @JsonProperty("symbol")
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public long getWindowStart() { return windowStart; }

    @JsonProperty("window_start")
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }

    @JsonProperty("window_end")
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public double getOpen() { return open; }
    @JsonProperty("open")
    public void setOpen(double open) { this.open = open; }

    public double getHigh() { return high; }
    @JsonProperty("high")
    public void setHigh(double high) { this.high = high; }

    public double getLow() { return low; }
    @JsonProperty("low")
    public void setLow(double low) { this.low = low; }

    public double getClose() { return close; }
    @JsonProperty("close")
    public void setClose(double close) { this.close = close; }

    public double getVolume() { return volume; }
    @JsonProperty("volume")
    public void setVolume(double volume) { this.volume = volume; }

    public double getVwap() { return vwap; }
    @JsonProperty("vwap")
    public void setVwap(double vwap) { this.vwap = vwap; }

    public int getTrades() { return trades; }
    @JsonProperty("trades")
    public void setTrades(int trades) { this.trades = trades; }

    public long getFirstTradeTime() { return firstTradeTime; }
    @JsonProperty("first_trade_time")
    public void setFirstTradeTime(long firstTradeTime) { this.firstTradeTime = firstTradeTime; }

    public long getLastTradeTime() { return lastTradeTime; }
    @JsonProperty("last_trade_time")
    public void setLastTradeTime(long lastTradeTime) { this.lastTradeTime = lastTradeTime; }

    @Override
    public String toString() {
        return "StockPrice{" +
                "symbol='" + symbol + '\'' +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", volume=" + volume +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StockPrice)) return false;
        StockPrice that = (StockPrice) o;
        return windowStart == that.windowStart &&
                Objects.equals(symbol, that.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, windowStart);
    }
}
