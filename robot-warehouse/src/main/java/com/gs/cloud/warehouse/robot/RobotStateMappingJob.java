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
import com.gs.cloud.warehouse.robot.sink.JdbcSinkGS;
import com.gs.cloud.warehouse.robot.source.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
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
    JdbcSinkGS.addRobotStateMappingSink(properties, result);
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

}
