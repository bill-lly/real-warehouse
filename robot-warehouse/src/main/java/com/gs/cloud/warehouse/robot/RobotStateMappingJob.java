/**
 * 统计机器人工作状态和在离线状态的mapping时间表
 */
package com.gs.cloud.warehouse.robot;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.entity.RobotIsOnline;
import com.gs.cloud.warehouse.robot.entity.RobotStateMapping;
import com.gs.cloud.warehouse.robot.entity.RobotWorkState;
import com.gs.cloud.warehouse.robot.lookup.RobotBasicDimLookupFunction;
import com.gs.cloud.warehouse.robot.process.StateMappingCoGroupProcessor;
import com.gs.cloud.warehouse.source.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 实时统计机器人工作状态和在离线状态的Mapping关系
 */

public class RobotStateMappingJob {

  private final static String JOB_NAME = "RobotStateMapping";

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //env.setParallelism(1);
    DataStream<RobotWorkState> workStateSource =
        env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotWorkState(), JOB_NAME),
                        watermarkStrategyWorkState(),
                        "business data");
    DataStream<RobotIsOnline> isOnlineSource =
        env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotIsOnline(), JOB_NAME),
                        watermarkStrategyIsOnline(),
                        "business data").setParallelism(1);
    DataStream<RobotWorkState> robotWorkStateDataStream = lastRobotWorkState(workStateSource);
    DataStream<RobotIsOnline> robotIsOnlineDataStream = lastRobotIsOnline(isOnlineSource);

    DataStream<RobotStateMapping> result = robotWorkStateDataStream.coGroup(robotIsOnlineDataStream)
        .where(RobotWorkState::getProductId).equalTo(RobotIsOnline::getProductId)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .apply(new StateMappingCoGroupProcessor(properties))
        .process(new RobotBasicDimLookupFunction(properties));
    //result.print();
    addRobotStateMappingSink(properties, result);
    env.execute();
  }

  private static WatermarkStrategy<RobotWorkState> watermarkStrategyWorkState() {
    return WatermarkStrategy.<RobotWorkState>forBoundedOutOfOrderness(Duration.ofSeconds(60))
        .withTimestampAssigner(
            (event, timestamp) -> event.getAdjustCreatedAtT().getTime());
  }

  private static WatermarkStrategy<RobotIsOnline> watermarkStrategyIsOnline() {
    return WatermarkStrategy.<RobotIsOnline>forBoundedOutOfOrderness(Duration.ofSeconds(60))
        .withTimestampAssigner(
            (event, timestamp)-> event.getCldDate().getTime());
  }

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }


  private static DataStream<RobotWorkState> lastRobotWorkState(DataStream<RobotWorkState> workStateSource) {
    return workStateSource
        .keyBy(RobotWorkState::getProductId)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .process(new ProcessWindowFunction<RobotWorkState, RobotWorkState, String, TimeWindow>() {
          private transient ValueState<RobotWorkState> lastWorkStateValueState;
          @Override
          public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.Disabled)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
            ValueStateDescriptor<RobotWorkState> descriptor =
                new ValueStateDescriptor<>(
                    "lastRobotWorkState",
                    TypeInformation.of(RobotWorkState.class));
            descriptor.enableTimeToLive(ttlConfig);
            lastWorkStateValueState = getRuntimeContext().getState(descriptor);
            super.open(parameters);
          }

          @Override
          public void process(String s, Context context,
                              Iterable<RobotWorkState> elements,
                              Collector<RobotWorkState> out) throws Exception {
            List<RobotWorkState> input = (List<RobotWorkState>)elements;
            if (input == null || input.isEmpty()) {
              return;
            }
            Collections.sort(input);
            RobotWorkState lastRobotWorkState = lastWorkStateValueState.value();
            for (RobotWorkState state : elements) {
              if (state.getWorkState() == null) {
                continue;
              }
              if (lastRobotWorkState == null
                  || (lastRobotWorkState.getAdjustCreatedAtT().getTime() <= state.getAdjustCreatedAtT().getTime()
                  && !lastRobotWorkState.getWorkStateCode().equals(state.getWorkStateCode()))) {
                out.collect(state);
              }
              lastRobotWorkState = state;
            }
            lastWorkStateValueState.update(lastRobotWorkState);
          }
        });
  }

  private static DataStream<RobotIsOnline> lastRobotIsOnline(DataStream<RobotIsOnline> robotIsOnlineSource) {
    return robotIsOnlineSource
        .keyBy(RobotIsOnline::getProductId)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .process(new ProcessWindowFunction<RobotIsOnline, RobotIsOnline, String, TimeWindow>() {
          private transient ValueState<RobotIsOnline> lastIsOnlineValueState;

          @Override
          public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.Disabled)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
            ValueStateDescriptor<RobotIsOnline> descriptor =
                new ValueStateDescriptor<>(
                    "lastRobotIsOnline",
                    TypeInformation.of(RobotIsOnline.class));
            descriptor.enableTimeToLive(ttlConfig);
            lastIsOnlineValueState = getRuntimeContext().getState(descriptor);
            super.open(parameters);
          }

          @Override
          public void process(String s, Context context,
                              Iterable<RobotIsOnline> elements,
                              Collector<RobotIsOnline> out) throws Exception {
            List<RobotIsOnline> input = (List<RobotIsOnline>)elements;
            if (input == null || input.isEmpty()) {
              return;
            }
            Collections.sort(input);
            RobotIsOnline lastRobotState = lastIsOnlineValueState.value();
            for (RobotIsOnline isOnline : elements) {
              if (lastRobotState == null
                  || (lastRobotState.getCldDate().getTime() <= isOnline.getCldDate().getTime()
                  && lastRobotState.getIsOnline().intValue() != isOnline.getIsOnline().intValue())) {
                out.collect(isOnline);
              }
              lastRobotState = isOnline;
            }
            lastIsOnlineValueState.update(lastRobotState);
          }
        });
  }

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

}
