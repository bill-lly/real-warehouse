package com.gs.cloud.warehouse.robot.process.impl;

import com.gs.cloud.warehouse.robot.entity.RobotStateMapping;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class StateSearchImpl implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StateSearchImpl.class);
  private final static int maxRetryTimes = 3;
  private final JdbcConnectionProvider connectionProvider;
  private transient PreparedStatement statement;
  private Date t2999;
  private boolean inited = false;

  private final static String sql =
      "select " +
          "product_id, " +
          "start_time, " +
          "work_state_code, " +
          "work_state, " +
          "end_time, " +
          "duration, " +
          "end_date, " +
          "is_online, " +
          "recv_start_time, " +
          "recv_end_time " +
      "from robotbi.robot_state_1_0_work_state_time_crossday " +
      "where " +
          "product_id = ? and start_time <= ? and end_time > ?";

  private LookupProcessor<RobotStateMapping> lookupProcessor;

  public StateSearchImpl(Properties properties) {
    String jdbcUrl = properties.getProperty("robotbi.jdbc.url");
    String userName = properties.getProperty("robotbi.jdbc.user.name");
    String password = properties.getProperty("robotbi.jdbc.password");
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
    JdbcConnectionOptions options = builder.withUrl(jdbcUrl)
        .withUsername(userName)
        .withPassword(password)
        .withConnectionCheckTimeoutSeconds(60)
        .build();
    connectionProvider = new SimpleJdbcConnectionProvider(options);
    Calendar t = Calendar.getInstance();
    //服务时间是utc时间，写入到mysql的时候会自动+8小时
    t.set(2999, Calendar.DECEMBER, 31, 15, 59, 59);
    t.set(Calendar.MILLISECOND, 0);
    t2999 = t.getTime();
  }

  private void init() throws Exception {
    establishConnectionAndStatement();
    this.inited = true;
  }

  public RobotStateMapping search(String productId, Date date) throws Exception {
    if (!inited) {
      init();
    }
    statement.setString(1, productId);
    statement.setTimestamp(2, new java.sql.Timestamp(date.getTime()));
    statement.setTimestamp(3, new java.sql.Timestamp(date.getTime()));

    //由于服务设置的是utc时区，mysql设置的是东八区，所以不需要做时区处理，会自动转东八进行查询
    for (int retry = 0; retry <= maxRetryTimes; retry++) {
      try {
        try (ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            return convert(resultSet);
          }
        }
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
    return null;
  }

  private RobotStateMapping convert(ResultSet resultSet) throws SQLException {
    RobotStateMapping mapping = new RobotStateMapping();
    mapping.setProductId(resultSet.getString(1));
    mapping.setStartTime(resultSet.getTimestamp(2));
    mapping.setWorkStateCode(resultSet.getString(3));
    mapping.setWorkState(resultSet.getString(4));
    mapping.setEndTime(resultSet.getTimestamp(5));
    mapping.setDuration(resultSet.getLong(6));
    mapping.setIsOnline(resultSet.getInt(8));
    mapping.setRecvStartTime(resultSet.getTimestamp(9));
    mapping.setRecvEndTime(resultSet.getTimestamp(10));
    return mapping;
  }

  public void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
    Connection dbConn = connectionProvider.getOrEstablishConnection();
    statement = dbConn.prepareStatement(sql);
  }


}
