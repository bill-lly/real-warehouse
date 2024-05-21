package com.gs.cloud.warehouse.lookup;

import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class LookupProcessor<T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(LookupProcessor.class);
  private JdbcConnectionProvider connectionProvider;
  private transient Cache<String, List<T>> cache;
  private transient PreparedStatement statement;
  private String sql;
  private Boolean cacheWithEmpty;

  protected final static int maxRetryTimes = 3;

  public LookupProcessor(JdbcConnectionProvider connectionProvider,
                         String sql, Boolean cacheWithEmpty) {
    this.connectionProvider = connectionProvider;
    this.sql = sql;
    this.cacheWithEmpty = cacheWithEmpty;
  }

  public void initCache() {
      this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(60 * 60 * 1000, TimeUnit.MILLISECONDS)
        .maximumSize(5000)
        .build();
  }

  public List<T> getEntities(String cacheKey,
                                       Function<ResultSet, T> getResult) {
    List<T> cachedRows = cache.getIfPresent(cacheKey);
    if (cachedRows != null) {
      return new ArrayList<>(cachedRows);
    }
    List<T> rows = new ArrayList<>();
    for (int retry = 0; retry <= maxRetryTimes; retry++) {
      try {
        statement.setString(1, cacheKey);
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            rows.add(getResult.apply(resultSet));
          }
          if (cacheWithEmpty || !rows.isEmpty()) {
            cache.put(cacheKey, rows);
          }
        }
        return rows;
      } catch (SQLException e) {
        LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
        if (retry >= maxRetryTimes) {
          throw new RuntimeException("Execution of JDBC statement failed.", e);
        }
        try {
          if (!connectionProvider.isConnectionValid()) {
            statement.close();
            connectionProvider.closeConnection();
            establishConnectionAndStatement();
          }
        } catch (SQLException | ClassNotFoundException exception) {
          LOG.error(
              "JDBC connection is not valid, and reestablish connection failed",
              exception);
          throw new RuntimeException("Reestablish JDBC connection failed", exception);
        }
        try {
          Thread.sleep(1000 * retry);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
      }
    }
    return rows;
  }

  public void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
    Connection dbConn = connectionProvider.getOrEstablishConnection();
    statement = dbConn.prepareStatement(sql);
  }
}
