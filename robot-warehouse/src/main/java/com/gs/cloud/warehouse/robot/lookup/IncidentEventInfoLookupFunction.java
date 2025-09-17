package com.gs.cloud.warehouse.robot.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.robot.entity.IncidentEvent;
import com.gs.cloud.warehouse.robot.entity.MonitorResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class IncidentEventInfoLookupFunction extends ProcessFunction<IncidentEvent, IncidentEvent> {
    private final static String sql =
            "select incident_code," +
                    "incident_title," +
                    "incident_level," +
                    "other_effective," +
                    "is_blocking_task " +
            "from bg_rt_incident_type_dim " +
            "where incident_code = ?";
    private final LookupProcessor<IncidentEvent> lookupProcessor;


    public IncidentEventInfoLookupFunction(Properties properties) {
        String jdbcUrl = properties.getProperty("bigdata.real.dim.jdbc.url");
        String userName = properties.getProperty("bigdata.real.dim.jdbc.user.name");
        String password = properties.getProperty("bigdata.real.dim.jdbc.password");

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
    public void processElement(IncidentEvent value, Context ctx, Collector<IncidentEvent> out) throws Exception {
        List<IncidentEvent> entities = lookupProcessor.getEntities(value.getIncidentCode(), resultSet -> {
            try {
                IncidentEvent incidentEvent = new IncidentEvent();
                incidentEvent.setIncidentTitle(resultSet.getString(2));
                incidentEvent.setIncidentLevel(resultSet.getString(3));
                incidentEvent.setOtherEffective(resultSet.getString(4));
                incidentEvent.setIsBlockingTask(resultSet.getInt(5));
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
                value.setIncidentTitle(incidentEvent.getIncidentTitle());
                value.setIncidentLevel(incidentEvent.getIncidentLevel());
                value.setOtherEffective(incidentEvent.getOtherEffective());
                value.setIsBlockingTask(incidentEvent.getIsBlockingTask());
                out.collect(value);
            }
        }
    }
}
