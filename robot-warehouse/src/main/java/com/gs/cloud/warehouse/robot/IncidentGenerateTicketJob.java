/**
 * 机器人生单策略
 */

package com.gs.cloud.warehouse.robot;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.entity.RobotWorkState;
import com.gs.cloud.warehouse.robot.source.KafkaSourceFactory;
import com.gs.cloud.warehouse.robot.trigger.SingleElementWindows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class IncidentGenerateTicketJob {

  private final static String JOB_NAME = "IncidentGenerateTicket";

  public static void main(String[] args) throws Exception {

    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    WatermarkStrategy<RobotWorkState> strategy = WatermarkStrategy
        .<RobotWorkState>forBoundedOutOfOrderness(Duration.ofSeconds(0))
        .withTimestampAssigner((event, timestamp) -> event.getRecvTimestampT().getTime())
        .withIdleness(Duration.ofSeconds(10));
    DataStream<RobotWorkState> workStateSource =
        env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotWorkState(), JOB_NAME),
            strategy, "business data");

    workStateSource.keyBy(RobotWorkState::getProductId)
        .window(SingleElementWindows.create(Time.seconds(10)))
        .process(new ProcessWindowFunction<RobotWorkState, RobotWorkState, String, TimeWindow>() {

          @Override
          public void process(String s, Context context,
                              Iterable<RobotWorkState> elements,
                              Collector<RobotWorkState> out) throws Exception {
            elements.forEach(x-> System.out.println(x.toString()));
          }
        }).print();
    env.execute();
  }

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }
}
