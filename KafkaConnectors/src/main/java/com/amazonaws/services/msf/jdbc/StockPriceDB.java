package com.amazonaws.services.msf.jdbc;
import com.amazonaws.services.msf.domain.StockPrice;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.sql.Timestamp;


public class StockPriceDB implements JdbcQueryStatement<StockPrice> {
    private static final String UPSERT_QUERY_TEMPLATE =
        "INSERT INTO %s (symbol, ts, window_start, window_end, open, high, low, close, volume, vwap, ingestion_ts)\n" +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())\n"+
        "ON CONFLICT (symbol, ts) DO UPDATE SET\n" + 
        "  window_start = EXCLUDED.window_start,\n" + 
        "  window_end   = EXCLUDED.window_end,\n" + 
        "  open         = EXCLUDED.open,\n" + 
        "  high         = EXCLUDED.high,\n" + 
        "  low          = EXCLUDED.low,\n" + 
        "  close        = EXCLUDED.close,\n" + 
        "  volume       = EXCLUDED.volume,\n" + 
        "  vwap         = EXCLUDED.vwap,\n" + 
        "  ingestion_ts = EXCLUDED.ingestion_ts;";

    private final String sql;
    private final JdbcStatementBuilder<StockPrice> statementBuilder = new JdbcStatementBuilder<StockPrice>() {
        @Override
        public void accept(PreparedStatement preparedStatement, StockPrice stockPrice) throws SQLException {
            String symbol = stockPrice.getSymbol();
            long windowStartMs = stockPrice.getWindowStart();
            Instant instant = Instant.ofEpochMilli(windowStartMs);
            ZonedDateTime jakartaTime = instant.atZone(ZoneId.of("Asia/Jakarta"));
            Timestamp ts = Timestamp.from(jakartaTime.toInstant());
            // Timestamp timestamp = Timestamp.from(stockPrice.getTimestamp());
            long windowStart = stockPrice.getWindowStart();
            long windowEnd = stockPrice.getWindowStart();
            Double open = stockPrice.getOpen();
            Double high = stockPrice.getHigh();
            Double low = stockPrice.getLow();
            Double close = stockPrice.getClose();
            Double volume = stockPrice.getVolume();
            Double vwap = stockPrice.getVwap();

            // Replace the parameters positionally (note that some parameters are repeated in the SQL statement)
            preparedStatement.setString(1, symbol);
            preparedStatement.setTimestamp(2, ts);
            preparedStatement.setLong(3, windowStart);
            preparedStatement.setLong(4, windowEnd);
            preparedStatement.setDouble(5, open);
            preparedStatement.setDouble(6, high);
            preparedStatement.setDouble(7, low);
            preparedStatement.setDouble(8, close);
            preparedStatement.setDouble(9, volume);
            preparedStatement.setDouble(10, vwap);
        }
    };


    public StockPriceDB(String tableName) {
        this.sql = String.format(UPSERT_QUERY_TEMPLATE, tableName);
    }


    @Override
    public String query() {
        return sql;
    }


    @Override
    public void statement(PreparedStatement preparedStatement, StockPrice stockPrice) throws SQLException {
        statementBuilder.accept(preparedStatement, stockPrice);
    }
}
