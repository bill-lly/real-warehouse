package com.gs.cloud.warehouse.util;

import com.gs.cloud.warehouse.entity.FactEntity;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;

import java.util.Properties;

public class KafkaSourceUtils {

  @SuppressWarnings("unchecked")
  public static <T extends FactEntity> KafkaSource<T> getKafkaSource(Properties properties,
                                                                     T data,
                                                                     String jobName) {
    String env = properties.getProperty("env");
    @SuppressWarnings("rawtypes") KafkaSourceBuilder kafkaSourceBuilder = KafkaSource.<T>builder()
        .setBootstrapServers(properties.getProperty(data.getKafkaServer()))
        .setTopics(properties.getProperty(data.getKafkaTopic()))
            .setGroupId(properties.getProperty(data.getKafkaTopic()) + "." + jobName + "." + env)
        .setValueOnlyDeserializer(data.getSerDeserializer());
    KafkaSourceBuilder<T> kafkaSourceBuilderScan = KafkaSourceUtils.scanMode(kafkaSourceBuilder, properties);
    return kafkaSourceBuilderScan.build();
  }

  @SuppressWarnings("rawtypes")
  public static KafkaSourceBuilder scanMode(KafkaSourceBuilder kafkaSourceBuilder,
                                            Properties properties) {
    String scanMode = properties.getProperty("kafkaScanMode", "group-offsets");
    switch (scanMode) {
      case "earliest":
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
        break;
      case "latest":
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
        break;
      case "group-offsets":
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
        break;
      case "timestamp":
        Long timestamp = Preconditions.checkNotNull(Long.parseLong(properties.getProperty("kafkaScanTimestamp")));
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(timestamp));
        break;
      default:
        throw new RuntimeException(String.format("no supported scan mode, %s", scanMode));
    }
    return kafkaSourceBuilder;
  }
}
