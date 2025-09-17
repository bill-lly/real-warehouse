package com.gs.cloud.warehouse.robot.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.robot.entity.RobotDiagnosisData;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.binary.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RobotStateLookupFunction implements Serializable {
    private final static String sql =
            "select product_id," +
                    "attached_task," +
                    "attached_online_status," +
                    "attached_work_status," +
                    "attached_ess_status ," +
                    "event_time," +
                    "state_key_cn," +
                    "state_value_cn " +
            "from robot_diagnosis_timeline_state " +
            "where product_id = ?";
    private final LookupProcessor<BaseEntity> lookupProcessor;


    public RobotStateLookupFunction(Properties properties)  throws Exception {
        String jdbcUrl = properties.getProperty("robotbi.jdbc.url");
        String userName = properties.getProperty("robotbi.jdbc.user.name");
        String password = properties.getProperty("robotbi.jdbc.password");

        JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        JdbcConnectionOptions options = builder.withUrl(jdbcUrl)
                .withUsername(userName)
                .withPassword(password)
                .withConnectionCheckTimeoutSeconds(60)
                .build();
        JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(options);
        lookupProcessor = new LookupProcessor(connectionProvider, sql, true);
    }

    public void open(Configuration parameters) throws Exception {
        lookupProcessor.initCache();
        lookupProcessor.establishConnectionAndStatement();
    }

    public List<BaseEntity> processElement(BaseEntity value) throws Exception {
        List<BaseEntity> res = new ArrayList<>();
        List<BaseEntity> entities = lookupProcessor.getEntities(value.getKey(), resultSet -> {
            try {
                RobotDiagnosisData robotDiagnosisData = new RobotDiagnosisData();
                robotDiagnosisData.setAttachedTask(resultSet.getString(2));
                robotDiagnosisData.setAttachedOnlineStatus(resultSet.getString(3));
                robotDiagnosisData.setAttachedWorkStatus(resultSet.getString(4));
                robotDiagnosisData.setAttachedEssStatus(resultSet.getString(5));
                robotDiagnosisData.setStartTime(resultSet.getDate(6));
                robotDiagnosisData.setStateKeyCn(resultSet.getString(7));
                robotDiagnosisData.setStateValueCn(resultSet.getString(8));

                return robotDiagnosisData;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        if (!entities.isEmpty()) {
            for (BaseEntity entity:entities) {
                RobotDiagnosisData robotDiagnosisData = (RobotDiagnosisData)entity;
                if (StringUtils.equals(null, ((RobotDiagnosisData) value).getAttachedTask())) {
                    ((RobotDiagnosisData) value).setAttachedTask(robotDiagnosisData.getAttachedTask());
                }

                if (StringUtils.equals(null, ((RobotDiagnosisData) value).getAttachedOnlineStatus())) {
                    ((RobotDiagnosisData)value).setAttachedOnlineStatus(robotDiagnosisData.getAttachedOnlineStatus());
                }

                if (StringUtils.equals(null, ((RobotDiagnosisData) value).getAttachedWorkStatus())) {
                    ((RobotDiagnosisData)value).setAttachedWorkStatus(robotDiagnosisData.getAttachedWorkStatus());
                    ((RobotDiagnosisData)value).setStartTime(robotDiagnosisData.getStartTime());
                }

                if (StringUtils.equals(null, ((RobotDiagnosisData) value).getAttachedEssStatus())) {
                    ((RobotDiagnosisData)value).setAttachedEssStatus(robotDiagnosisData.getAttachedEssStatus());
                }

                if (StringUtils.equals(null, ((RobotDiagnosisData) value).getStateKeyCn())) {
                    ((RobotDiagnosisData)value).setStateKeyCn(robotDiagnosisData.getStateKeyCn());
                }
                if (StringUtils.equals(null, ((RobotDiagnosisData) value).getStateValueCn())) {
                    ((RobotDiagnosisData)value).setStateValueCn(robotDiagnosisData.getStateValueCn());
                }
                res.add(value);
            }
        }
        return res;
    }
}