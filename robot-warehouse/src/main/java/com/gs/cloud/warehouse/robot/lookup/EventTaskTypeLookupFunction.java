package com.gs.cloud.warehouse.robot.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.robot.entity.IncidentEvent;
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

public class EventTaskTypeLookupFunction
    extends ProcessFunction<IncidentEvent, IncidentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(EventTaskTypeLookupFunction.class);
  private final static String sql = "select code, is_create_ticket from yew.event_task_type where is_enabled  = 1 and execution_layer = 'REMOTE_MAINTENANCE' and code = ?";
  private final LookupProcessor<BaseEntity> lookupProcessor;

  public EventTaskTypeLookupFunction(Properties properties) {
    String jdbcUrl = properties.getProperty("yew.jdbc.url");
    String userName = properties.getProperty("yew.jdbc.user.name");
    String password = properties.getProperty("yew.jdbc.password");
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
  public void processElement(IncidentEvent value, Context ctx, Collector<IncidentEvent> out) throws Exception {
    List<BaseEntity> entities = lookupProcessor.getEntities(value.getIncidentCode(), resultSet -> {
      try {
        IncidentEvent incidentEvent = new IncidentEvent();
        incidentEvent.setCreateTicket(resultSet.getBoolean(2));
        return incidentEvent;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    if (entities.isEmpty()) {
      out.collect(value);
    } else {
      for (BaseEntity entity:entities) {
        IncidentEvent incidentEvent = (IncidentEvent)entity;
        value.setCreateTicket(incidentEvent.isCreateTicket());
        out.collect(value);
      }
    }
  }
}
