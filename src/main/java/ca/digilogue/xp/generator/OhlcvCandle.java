package ca.digilogue.xp.generator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents an OHLCV (Open, High, Low, Close, Volume) candle.
 */
public class OhlcvCandle {
    private String symbol;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;
    private Instant timestamp;

    // No-arg constructor for Jackson deserialization
    public OhlcvCandle() {
        this.timestamp = Instant.now();
    }

    public OhlcvCandle(String symbol, double open, double high, double low, double close, double volume) {
        this.symbol = symbol;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.timestamp = Instant.now();
    }

    // JsonCreator for deserialization with timestamp
    @JsonCreator
    public OhlcvCandle(
            @JsonProperty("symbol") String symbol,
            @JsonProperty("open") double open,
            @JsonProperty("high") double high,
            @JsonProperty("low") double low,
            @JsonProperty("close") double close,
            @JsonProperty("volume") double volume,
            @JsonProperty("timestamp") Instant timestamp) {
        this.symbol = symbol;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format(
            "OhlcvCandle{symbol='%s', open=%.2f, high=%.2f, low=%.2f, close=%.2f, volume=%.2f, timestamp=%s}",
            symbol, open, high, low, close, volume, timestamp
        );
    }
}

