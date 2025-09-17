package com.gs.cloud.warehouse.robot.iot.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
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

public class ProductKeyFamilyCodeILookupFunction extends ProcessFunction<RccPropertyReport, RccPropertyReport> {

  private static final Logger LOG = LoggerFactory.getLogger(ProductKeyFamilyCodeILookupFunction.class);
  private final static String sql = "select bin_to_uuid(product_key, 1) as product_key, family_code as robot_family_code from product where product_key = uuid_to_bin(?,1)";
  private final LookupProcessor<BaseEntity> lookupProcessor;

  public ProductKeyFamilyCodeILookupFunction(Properties properties) {
    String jdbcUrl = properties.getProperty("iot.device.management.jdbc.url");
    String userName = properties.getProperty("iot.device.management.user.name");
    String password = properties.getProperty("iot.device.management.password");
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
    JdbcConnectionOptions options = builder.withUrl(jdbcUrl)
        .withUsername(userName)
        .withPassword(password)
        .withConnectionCheckTimeoutSeconds(60)
        .build();
    JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(options);
    lookupProcessor = new LookupProcessor(connectionProvider, sql, true);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    lookupProcessor.initCache();
    lookupProcessor.establishConnectionAndStatement();
  }

  @Override
  public void processElement(RccPropertyReport value, Context ctx, Collector<RccPropertyReport> out) {
    List<BaseEntity> entities = lookupProcessor.getEntities(value.getProductKey(), resultSet -> {
      try {
        RccPropertyReport rccPropertyReport = new RccPropertyReport();
        rccPropertyReport.setRobotFamilyCode(resultSet.getString(2));
        return rccPropertyReport;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    if (entities.isEmpty()) {
      out.collect(value);
    } else {
      for (BaseEntity entity:entities) {
        RccPropertyReport robotStateMapping = (RccPropertyReport)entity;
        value.setRobotFamilyCode(robotStateMapping.getRobotFamilyCode());
        out.collect(value);
      }
    }
  }
}
