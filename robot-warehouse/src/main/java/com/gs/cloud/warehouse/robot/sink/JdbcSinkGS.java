package com.gs.cloud.warehouse.robot.sink;

import com.gs.cloud.warehouse.robot.entity.MonitorResult;
import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.RobotStateMapping;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.Properties;

public class JdbcSinkGS {

  public static DataStreamSink<RobotStateMapping> addRobotStateMappingSink(Properties properties,
                                                                           DataStream<RobotStateMapping> resultStream) {

    return resultStream.addSink(JdbcSink.sink(
        "replace into real_robot_state_1_0_work_state_time_crossday " +
            "(date, product_id, start_time, work_state_code, work_state, end_time, duration, end_date, is_online, recv_start_time, recv_end_time, robot_family_code, alias, customer_category, scene, group_name, terminal_user_name, customer_grade, delivery_status, business_area, udesk_maint_group_name, udesk_maint_level, udesk_ics_promotion, udesk_project_property)" +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (statement, mapping) -> {
          statement.setDate(1, new java.sql.Date(mapping.getStartTime().getTime()));
          statement.setString(2, mapping.getProductId());
          statement.setTimestamp(3, new java.sql.Timestamp(mapping.getStartTime().getTime()));
          statement.setString(4, mapping.getWorkStateCode());
          statement.setString(5, mapping.getWorkState());
          statement.setTimestamp(6, new java.sql.Timestamp(mapping.getEndTime().getTime()));
          statement.setLong(7, mapping.getDuration());
          statement.setDate(8, new java.sql.Date(mapping.getEndTime().getTime()));
          statement.setObject(9, mapping.getIsOnline());
          statement.setTimestamp(10, new java.sql.Timestamp(mapping.getRecvStartTime().getTime()));
          statement.setTimestamp(11, new java.sql.Timestamp(mapping.getRecvEndTime().getTime()));
          statement.setString(12, mapping.getRobotFamilyCode());
          statement.setString(13, mapping.getAlias());
          statement.setString(14, mapping.getCustomerCategory());
          statement.setString(15, mapping.getScene());
          statement.setString(16, mapping.getGroupName());
          statement.setString(17, mapping.getTerminalUserName());
          statement.setString(18, mapping.getCustomerGrade());
          statement.setString(19, mapping.getDeliveryStatus());
          statement.setString(20, mapping.getBusinessArea());
          statement.setString(21, mapping.getUdeskMaintGroupName());
          statement.setString(22, mapping.getUdeskMaintLevel());
          statement.setString(23, mapping.getUdeskIcsPromotion());
          statement.setString(24, mapping.getUdeskProjectProperty());
        },
        JdbcExecutionOptions.builder()
            .withBatchSize(5000)
            .withBatchIntervalMs(1000)
            .withMaxRetries(5)
            .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(properties.getProperty("robotbi.jdbc.url"))
            .withDriverName("com.mysql.jdbc.Driver")
            .withUsername(properties.getProperty("robotbi.jdbc.user.name"))
            .withPassword(properties.getProperty("robotbi.jdbc.password"))
            .build()
    ));
  }


  public static DataStreamSink<MonitorWindowStat> addMonitorWindowStatSink(Properties properties,
                                                                           DataStream<MonitorWindowStat> resultStream) {
    return resultStream.addSink(JdbcSink.sink(
        "replace into real_monitor_window_stat" +
            "(product_id, start_timestamp, end_timestamp, avg_offset, incident_cnt, total_displacement, code_incident_cnt, duration_map, max_duration_map, state_map, incident_map, displacement_map)" +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (statement, mapping) -> {
              statement.setString(1, mapping.getProductId());
              statement.setTimestamp(2, new java.sql.Timestamp(mapping.getStartTimestamp().getTime()));
              statement.setTimestamp(3, new java.sql.Timestamp(mapping.getEndTimestamp().getTime()));
              statement.setLong(4, mapping.getAvgOffset());
              statement.setInt(5, mapping.getIncidentCnt());
              statement.setDouble(6, mapping.getTotalDisplacement());
              statement.setString(7,
                  mapping.getCodeIncidentCnt().isEmpty() ? null : mapping.getCodeIncidentCnt().toString());
              statement.setString(8,
                  mapping.getDurationMap().isEmpty() ? null : mapping.getDurationMap().toString());
              statement.setString(9,
                  mapping.getMaxDurationMap().isEmpty() ? null : mapping.getMaxDurationMap().toString());
              statement.setString(10,
                  mapping.getWorkStates().isEmpty() ? null : mapping.getWorkStates().toString());
              statement.setString(11,
                  mapping.getIncidentMap().isEmpty() ? null : mapping.getIncidentMap().toString());
              statement.setString(12,
                  mapping.getDisplacementMap().isEmpty() ? null : mapping.getDisplacementMap().toString());
        },
        JdbcExecutionOptions.builder()
            .withBatchSize(5000)
            .withBatchIntervalMs(1000)
            .withMaxRetries(5)
            .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(properties.getProperty("robotbi.jdbc.url"))
            .withDriverName("com.mysql.jdbc.Driver")
            .withUsername(properties.getProperty("robotbi.jdbc.user.name"))
            .withPassword(properties.getProperty("robotbi.jdbc.password"))
            .build()
    ));
  }

  public static DataStreamSink<MonitorResult> addMonitorResultSink(Properties properties,
                                                                       DataStream<MonitorResult> resultStream) {
    return resultStream.addSink(JdbcSink.sink(
        "replace into real_monitor_result" +
            "(product_id, start_timestamp, trigger_timestamp, code, msg) values (?, ?, ?, ?, ?)",
        (statement, result) -> {
          statement.setString(1, result.getProductId());
          statement.setTimestamp(2, new java.sql.Timestamp(result.getStartTimestamp().getTime()));
          statement.setTimestamp(3, new java.sql.Timestamp(result.getTriggerTimestamp().getTime()));
          statement.setString(4, result.getCode());
          statement.setString(5, result.getMsg());
        },
        JdbcExecutionOptions.builder()
            .withBatchSize(5000)
            .withBatchIntervalMs(1000)
            .withMaxRetries(5)
            .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(properties.getProperty("robotbi.jdbc.url"))
            .withDriverName("com.mysql.jdbc.Driver")
            .withUsername(properties.getProperty("robotbi.jdbc.user.name"))
            .withPassword(properties.getProperty("robotbi.jdbc.password"))
            .build()
    ));
  }
}
