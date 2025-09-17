package com.gs.cloud.warehouse.robot.iot;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.iot.entity.IotEntity;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.robot.iot.entity.WindowedRccCoverageArea;
import com.gs.cloud.warehouse.robot.iot.format.GenericRecordDeserializer;
import com.gs.cloud.warehouse.robot.iot.process.PositionCalculateProcessor;
import com.gs.cloud.warehouse.robot.iot.process.RccPropertyReportProcessor;
import com.gs.cloud.warehouse.robot.iot.process.TaskStatusProcessor;
import com.gs.cloud.warehouse.robot.iot.process.WindowedRccCoverageAreaProcessor;
import com.gs.cloud.warehouse.trigger.EventTimePurgeTrigger;
import com.gs.cloud.warehouse.util.KafkaSourceUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

public class IotDataIngestion {

  private static final Logger LOG = LoggerFactory.getLogger(IotDataIngestion.class);

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
//    env.getCheckpointConfig().setCheckpointInterval(30 *1000 *2);

    GenericRecordDeserializer deserializer = new GenericRecordDeserializer(
            properties.getProperty("avro.confluent.url"));

      KafkaSourceBuilder<String> kafkaSourceBuilder = KafkaSource.<String>builder()
        .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers.iot"))
        .setTopics(properties.getProperty("kafka.topic.iot.robot.rcc.property"))
        .setGroupId(properties.getProperty("kafka.group.id.iot.robot.rcc.property"))
        .setDeserializer(deserializer);

      if ("release".equals(properties.getProperty("env"))) {
          kafkaSourceBuilder.setProperties(getTestProperties());
      }

    @SuppressWarnings("unchecked")KafkaSource<String> kafkaSource
            = KafkaSourceUtils.scanMode(kafkaSourceBuilder, properties).build();

    WatermarkStrategy<IotEntity> watermarkStrategy =
            WatermarkStrategy.<IotEntity>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner((event, timestamp) -> event.getCldUnixTimestamp());
    DataStream<IotEntity> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "business data")
        .flatMap(new FlatMapFunction<String, IotEntity>() {
          private final ObjectMapper objectMapper = new ObjectMapper();
          private final SimpleDateFormat df_yyyyMMdd = new SimpleDateFormat("yyyyMMdd");

          @Override
          public void flatMap(String s, Collector<IotEntity> collector)  {
              try {
                  JsonNode jsonNode = objectMapper.readTree(s);
                  IotEntity iotEntity = new IotEntity();
                  iotEntity.setProductKey(jsonNode.get("productKey").asText());
                  iotEntity.setDeviceId(jsonNode.get("deviceId").asText());
                  iotEntity.setVersion(jsonNode.get("version").asText());
                  iotEntity.setNamespace(jsonNode.get("namespace").asText());
                  iotEntity.setObjectName(jsonNode.get("objectName").asText());
                  String cldUnixTimestamp = jsonNode.get("timestamp").asText();
                  iotEntity.setCldUnixTimestamp(Long.parseLong(cldUnixTimestamp));
                  iotEntity.setMethod(jsonNode.get("method").asText());
                  iotEntity.setData(s);
                  iotEntity.setPt(
                          df_yyyyMMdd.format(
                                  DateUtils.addHours(
                                          Date.from(
                                                  Instant.ofEpochMilli(
                                                          Long.parseLong(cldUnixTimestamp))), 8)));
                  collector.collect(iotEntity);
              } catch (Exception e) {
                  LOG.error("Failed to parse the string to json, s={}", s, e);
              }
          }
        })
    .filter((FilterFunction<IotEntity>) value -> "PropertyReport".equals(value.getMethod()))
    .assignTimestampsAndWatermarks(watermarkStrategy);

    RccPropertyReportProcessor rccPropertyReportProcessor = new RccPropertyReportProcessor(properties);
    DataStream<RccPropertyReport> main = rccPropertyReportProcessor.process(dataStream);

    //位置信息
    PositionCalculateProcessor positionProcessor = new PositionCalculateProcessor(properties);
    main.map((MapFunction<RccPropertyReport, RccPropertyReport>) rccPropertyReport -> {
      //减少不必要的数据
      rccPropertyReport.setTaskStatus(null);
      return rccPropertyReport;
    }).filter((FilterFunction<RccPropertyReport>) rccPropertyReport -> rccPropertyReport.getPosition() != null)
            .keyBy(RccPropertyReport::getProductId)
            .window(TumblingEventTimeWindows.of(Time.seconds(60)))
            .trigger(EventTimePurgeTrigger.create())
            .process(positionProcessor)
            .sinkTo(positionProcessor.getKafkaSink());

      //任务信息
      TaskStatusProcessor taskStatusProcessor = new TaskStatusProcessor(properties);
      WindowedRccCoverageAreaProcessor windowedRccCoverageAreaProcessor = new WindowedRccCoverageAreaProcessor(properties);
      main.map((MapFunction<RccPropertyReport, RccPropertyReport>) rccPropertyReport -> {
                  //减少不必要的数据
                  rccPropertyReport.setPosition(null);
                  return rccPropertyReport;
              }).filter((FilterFunction<RccPropertyReport>) rccPropertyReport -> rccPropertyReport.getTaskStatus() != null)
              .keyBy(RccPropertyReport::getProductId)
              .window(TumblingEventTimeWindows.of(Time.seconds(60)))
              .trigger(EventTimePurgeTrigger.create())
              .process(taskStatusProcessor)
              .keyBy(WindowedRccCoverageArea::getProductId)
              .process(windowedRccCoverageAreaProcessor)
              .sinkTo(taskStatusProcessor.getKafkaSink());

    if ("prod".equals(properties.getProperty("env"))) {
      rccPropertyReportProcessor.sinkHudi(dataStream);
    }
    env.execute();
  }

    private static Properties getTestProperties() {
        Properties testProperties = new Properties();
        testProperties.setProperty("security.protocol", "SASL_SSL");
        testProperties.setProperty("ssl.truststore.location", "/opt/mix.4096.client.truststore.jks");
        testProperties.setProperty("ssl.truststore.password", "KafkaOnsClient");
        testProperties.setProperty("sasl.mechanism", "PLAIN");
        testProperties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"2Mr66GQhtuNJyhkxxDeOEDckayaxZ\";");
        testProperties.setProperty("ssl.endpoint.identification.algorithm", "");
        return testProperties;
    }

    private static void validate(ParameterTool params) {
        Preconditions.checkNotNull(params.get("env"), "env can not be null");
    }
}
