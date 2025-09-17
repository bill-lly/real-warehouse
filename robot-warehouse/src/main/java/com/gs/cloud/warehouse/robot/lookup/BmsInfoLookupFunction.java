package com.gs.cloud.warehouse.robot.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
import com.gs.cloud.warehouse.robot.entity.BmsBatteryInfo;
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

public class BmsInfoLookupFunction extends ProcessFunction<BmsBatteryInfo, BmsBatteryInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(BmsInfoLookupFunction.class);
    private final static String sql = "select product_id, robot_family_code, hardwareversion6, hardwareversion14 from bigdata_real_dim.bg_rt_robot_basic_dim where product_id = ?";
    private final LookupProcessor<BaseEntity> lookupProcessor;

    public BmsInfoLookupFunction(Properties properties) {
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
    public void processElement(BmsBatteryInfo value,
                               ProcessFunction<BmsBatteryInfo, BmsBatteryInfo>.Context context,
                               Collector<BmsBatteryInfo> out) throws Exception {
        List<BaseEntity> entities = lookupProcessor.getEntities(value.getKey(), resultSet -> {
            try {
                BmsBatteryInfo bmsBatteryInfo = new BmsBatteryInfo();
                bmsBatteryInfo.setRobotFamilyCode(resultSet.getString(2));
                bmsBatteryInfo.setHardwareVersion6(resultSet.getString(3));
                bmsBatteryInfo.setHardwareVersion14(resultSet.getString(4));
                return bmsBatteryInfo;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        if (entities.isEmpty()) {
            out.collect(value);
        } else {
            for (BaseEntity entity : entities) {
                BmsBatteryInfo bmsBatteryInfo = (BmsBatteryInfo) entity;
                value.setRobotFamilyCode(bmsBatteryInfo.getRobotFamilyCode());
                value.setHardwareVersion6(bmsBatteryInfo.getHardwareVersion6());
                value.setHardwareVersion14(bmsBatteryInfo.getHardwareVersion14());
                out.collect(value);
            }
        }
    }
}
