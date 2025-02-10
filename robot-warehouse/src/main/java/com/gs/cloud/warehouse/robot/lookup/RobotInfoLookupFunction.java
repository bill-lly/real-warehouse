package com.gs.cloud.warehouse.robot.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.robot.entity.MonitorResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class RobotInfoLookupFunction extends ProcessFunction<MonitorResult, MonitorResult> {

  private static final Logger LOG = LoggerFactory.getLogger(LookupProcessor.class);
  private final static String sql = "select product_id, alias, software_version, customer_code, model_type_code from robot where product_id = ?";
  private final LookupProcessor<BaseEntity> lookupProcessor;

  public RobotInfoLookupFunction(Properties properties) {
    String jdbcUrl = properties.getProperty("gs.robot.jdbc.url");
    String userName = properties.getProperty("gs.robot.user.name");
    String password = properties.getProperty("gs.robot.password");
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
    JdbcConnectionOptions options = builder.withUrl(jdbcUrl)
        .withUsername(userName)
        .withPassword(password)
        .withConnectionCheckTimeoutSeconds(60)
        .build();
    JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(options);
    lookupProcessor = new LookupProcessor(connectionProvider, sql, false);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    lookupProcessor.initCache();
    lookupProcessor.establishConnectionAndStatement();
  }

  @Override
  public void processElement(MonitorResult value, Context ctx, Collector<MonitorResult> out) throws Exception {
    List<BaseEntity> entities = lookupProcessor.getEntities(value.getKey(), resultSet -> {
      try {
        MonitorResult monitorResult = new MonitorResult();
        monitorResult.setAlias(resultSet.getString(2));
        monitorResult.setSoftwareVersion(resultSet.getString(3));
        monitorResult.setCustomerCode(resultSet.getString(4));
        monitorResult.setModelTypeCode(resultSet.getString(5));
        return monitorResult;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    if (entities.isEmpty()) {
      out.collect(value);
    } else {
      for (BaseEntity entity:entities) {
        MonitorResult monitorResult = (MonitorResult)entity;
        value.setAlias(monitorResult.getAlias());
        value.setSoftwareVersion(monitorResult.getSoftwareVersion());
        value.setCustomerCode(monitorResult.getCustomerCode());
        value.setModelTypeCode(monitorResult.getModelTypeCode());
        out.collect(value);
      }
    }
  }
}