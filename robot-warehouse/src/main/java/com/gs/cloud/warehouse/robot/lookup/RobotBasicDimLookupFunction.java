package com.gs.cloud.warehouse.robot.lookup;

import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.RobotStateMapping;
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

public class RobotBasicDimLookupFunction
    extends ProcessFunction<RobotStateMapping, RobotStateMapping> {

    private static final Logger LOG = LoggerFactory.getLogger(RobotBasicDimLookupFunction.class);
    private final static String sql = "select product_id, robot_family_code, alias, customer_category, scene, group_name, terminal_user_name, customer_grade, delivery_status, business_area, udesk_maint_group_name, udesk_maint_level, udesk_ics_promotion, udesk_project_property from bigdata_real_dim.bg_rt_robot_basic_dim where product_id = ?";
    private final LookupProcessor<BaseEntity> lookupProcessor;

    public RobotBasicDimLookupFunction(Properties properties) {
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
    public void processElement(RobotStateMapping value, Context ctx, Collector<RobotStateMapping> out) throws Exception {
        List<BaseEntity> entities = lookupProcessor.getEntities(value.getKey(), resultSet -> {
            try {
                RobotStateMapping robotStateMapping = new RobotStateMapping();
                robotStateMapping.setRobotFamilyCode(resultSet.getString(2));
                robotStateMapping.setAlias(resultSet.getString(3));
                robotStateMapping.setCustomerCategory(resultSet.getString(4));
                robotStateMapping.setScene(resultSet.getString(5));
                robotStateMapping.setGroupName(resultSet.getString(6));
                robotStateMapping.setTerminalUserName(resultSet.getString(7));
                robotStateMapping.setCustomerGrade(resultSet.getString(8));
                robotStateMapping.setDeliveryStatus(resultSet.getString(9));
                robotStateMapping.setBusinessArea(resultSet.getString(10));
                robotStateMapping.setUdeskMaintGroupName(resultSet.getString(11));
                robotStateMapping.setUdeskMaintLevel(resultSet.getString(12));
                robotStateMapping.setUdeskIcsPromotion(resultSet.getString(13));
                robotStateMapping.setUdeskProjectProperty(resultSet.getString(14));
                return robotStateMapping;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        if (entities.isEmpty()) {
            out.collect(value);
        } else {
            for (BaseEntity entity:entities) {
                RobotStateMapping robotStateMapping = (RobotStateMapping)entity;
                value.setRobotFamilyCode(robotStateMapping.getRobotFamilyCode());
                value.setAlias(robotStateMapping.getAlias());
                value.setCustomerCategory(robotStateMapping.getCustomerCategory());
                value.setScene(robotStateMapping.getScene());
                value.setGroupName(robotStateMapping.getGroupName());
                value.setTerminalUserName(robotStateMapping.getTerminalUserName());
                value.setCustomerGrade(robotStateMapping.getCustomerGrade());
                value.setDeliveryStatus(robotStateMapping.getDeliveryStatus());
                value.setBusinessArea(robotStateMapping.getBusinessArea());
                value.setUdeskMaintGroupName(robotStateMapping.getUdeskMaintGroupName());
                value.setUdeskMaintLevel(robotStateMapping.getUdeskMaintLevel());
                value.setUdeskIcsPromotion(robotStateMapping.getUdeskIcsPromotion());
                value.setUdeskProjectProperty(robotStateMapping.getUdeskProjectProperty());
                out.collect(value);
            }
        }
    }
}
