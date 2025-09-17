package com.gs.cloud.warehouse.ics.function.lookup;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.ics.entity.ItemCrewCardDetail;
import com.gs.cloud.warehouse.lookup.LookupProcessor;
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

public class PositionCrewRelationFunction extends ProcessFunction<ItemCrewCardDetail, ItemCrewCardDetail> {
    private static final Logger LOG = LoggerFactory.getLogger(PositionCrewRelationFunction.class);
    private final static String sql = "SELECT id, position_id, crew_id, crew_name, is_deleted, created_time, updated_time, created_by, updated_by FROM t_position_crew_relation WHERE is_deleted = '0' AND crew_id = ?";
    private final LookupProcessor<BaseEntity> lookupProcessor;

    public PositionCrewRelationFunction(Properties properties) {
        String jdbcUrl = properties.getProperty("ics.service.jdbc.url");
        String userName = properties.getProperty("ics.service.jdbc.user.name");
        String password = properties.getProperty("ics.service.jdbc.password");
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
    public void processElement(ItemCrewCardDetail value, Context context, Collector<ItemCrewCardDetail> out) throws Exception {
        if(value.getCrewId()==null){
            out.collect(value);
            return;
        }
        List<BaseEntity> entities = lookupProcessor.getEntities(String.valueOf(value.getCrewId()), resultSet -> {
            try {
                ItemCrewCardDetail itemCrewCardDetail = new ItemCrewCardDetail();
                itemCrewCardDetail.setPositionIdList(String.valueOf(resultSet.getInt(2)));
                return itemCrewCardDetail;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        if (entities.isEmpty()) {
            out.collect(value);
        } else {
            for (BaseEntity entity:entities) {
                ItemCrewCardDetail itemCrewCardDetail = (ItemCrewCardDetail)entity;
                value.setPositionIdList(itemCrewCardDetail.getPositionIdList());
                out.collect(value);
            }
        }
    }
}
