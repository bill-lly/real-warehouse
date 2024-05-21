package com.gs.cloud.warehouse.robot.iot;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.iot.entity.IotEntity;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.robot.iot.process.RccPropertyReportProcessor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.formats.avro.registry.confluent.ConfluentAvroDeserializerFactory;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.KafkaSourceUtils;

import java.util.Properties;

public class IotDataIngestion {

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
//    env.getCheckpointConfig().setCheckpointInterval(30 *1000);
    KafkaSourceBuilder<String> kafkaSourceBuilder = KafkaSource.<String>builder()
        .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers.iot"))
        .setTopics(properties.getProperty("kafka.topic.iot.robot.rcc.property"))
        .setGroupId(properties.getProperty("kafka.group.id.iot.robot.rcc.property"))
        .setValueOnlyDeserializer(ConfluentAvroDeserializerFactory.forGeneric(
            properties.getProperty("avro.confluent.url")));
    @SuppressWarnings("unchecked") KafkaSource<String> kafkaSource =
        KafkaSourceUtils.scanMode(kafkaSourceBuilder, properties).build();

    DataStream<IotEntity> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "business data")
        .map(new MapFunction<String, IotEntity>() {
          private final ObjectMapper objectMapper = new ObjectMapper();
          @Override
          public IotEntity map(String value) throws Exception {
            JsonNode jsonNode = objectMapper.readTree(value);
            IotEntity iotEntity = new IotEntity();
            iotEntity.setProductKey(jsonNode.get("productKey").asText());
            iotEntity.setDeviceId(jsonNode.get("deviceId").asText());
            iotEntity.setVersion(jsonNode.get("version").asText());
            iotEntity.setNamespace(jsonNode.get("namespace").asText());
            iotEntity.setObjectName(jsonNode.get("objectName").asText());
            iotEntity.setCldUnixTimestamp(jsonNode.get("timestamp").asText());
            iotEntity.setMethod(jsonNode.get("method").asText());
            iotEntity.setData(value);
            return iotEntity;
          }
        })
    .filter((FilterFunction<IotEntity>)
        value -> "PropertyReport".equals(value.getMethod()));
    RccPropertyReportProcessor rccPropertyReportProcessor = new RccPropertyReportProcessor(properties);
    DataStream<RccPropertyReport> main = rccPropertyReportProcessor.process(dataStream);
    main.sinkTo(rccPropertyReportProcessor.getKafkaSink());
    if ("prod".equals(properties.getProperty("env"))) {
      rccPropertyReportProcessor.sinkHudi(main);
    }
    env.execute();
  }

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }
}
