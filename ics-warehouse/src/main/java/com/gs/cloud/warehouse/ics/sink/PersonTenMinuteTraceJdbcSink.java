package com.gs.cloud.warehouse.ics.sink;

import com.gs.cloud.warehouse.ics.entity.PersonTenMinuteTrace;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.Properties;

public class PersonTenMinuteTraceJdbcSink {
    public static DataStreamSink<PersonTenMinuteTrace> getJdbcSink(Properties properties, DataStream<PersonTenMinuteTrace> resultStream){
        return resultStream.addSink(JdbcSink.sink(
                "replace into t_ten_minute_person_trace " +
                "(item_id, crew_id, slice_timestamp, report_total_time, maximum_duration_space_time, maximum_duration_space_code, maximum_duration_space_name, space_info_detial)" +
                "values (?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, mapping) -> {
                    statement.setLong(1, mapping.getItemId());
                    statement.setLong(2, mapping.getCrewId());
                    statement.setLong(3, mapping.getSliceTimestamp());
                    statement.setLong(4, mapping.getReportTotalDura());
                    statement.setLong(5,mapping.getMaxSpaceDura());
                    statement.setString(6, mapping.getMaxSpaceCode());
                    statement.setString(7, mapping.getMaxSpaceName());
                    statement.setString(8, mapping.getSpaceInfoDetial());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5000)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(properties.getProperty("ics.datav.jdbc.url"))
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername(properties.getProperty("ics.datav.jdbc.user.name"))
                        .withPassword(properties.getProperty("ics.datav.jdbc.password"))
                        .build()
        ));
    }

}
