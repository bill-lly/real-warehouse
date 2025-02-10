package com.gs.cloud.warehouse.robot.iot;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.robot.iot.entity.WindowedRccPosition;
import com.gs.cloud.warehouse.robot.iot.process.PositionCalculateProcessor;
import com.gs.cloud.warehouse.trigger.EventTimePurgeTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import com.gs.cloud.warehouse.util.KafkaSourceUtils;

import java.time.Duration;
import java.util.Properties;

public class RobotPositionJob {

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
//    env.getCheckpointConfig().setCheckpointInterval(30 * 1000);

    KafkaSource<RccPropertyReport> kafkaSource =
        KafkaSourceUtils.getKafkaSource(
              properties, new RccPropertyReport(), "RobotPositionJob");

    WatermarkStrategy<RccPropertyReport> watermarkStrategy =
        WatermarkStrategy.<RccPropertyReport>forBoundedOutOfOrderness(Duration.ofSeconds(0))
        .withTimestampAssigner((event, timestamp) -> event.getCldTimestampUtc().getTime());
    DataStream<RccPropertyReport> dataStream = env.fromSource(kafkaSource, watermarkStrategy, "rcc position");
    PositionCalculateProcessor processor = new PositionCalculateProcessor(properties);
    DataStream<WindowedRccPosition> main = dataStream
        .filter((FilterFunction<RccPropertyReport>) value -> value.getTimestampUtc() != null && value.getProductId() != null)
        .keyBy(RccPropertyReport::getDeviceId)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .trigger(EventTimePurgeTrigger.create())
        .process(processor);
    main.sinkTo(processor.getKafkaSink());
    if ("prod".equals(properties.getProperty("env"))) {
      processor.sinkHudi(main);
    }
    env.execute();
  }

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }
}
