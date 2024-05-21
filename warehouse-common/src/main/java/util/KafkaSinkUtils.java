package util;

import com.gs.cloud.warehouse.entity.FactEntity;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

import java.util.Properties;

public class KafkaSinkUtils {

  @SuppressWarnings("unchecked")
  public static <T extends FactEntity> KafkaSink<T> getKafkaSink(Properties properties,
                                                                 T data) {

    KafkaSinkBuilder<T> builder =  KafkaSink.<T>builder()
        .setBootstrapServers(properties.getProperty(data.getKafkaServer()))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(properties.getProperty(data.getKafkaTopic()))
            .setKeySerializationSchema(data.getKeySerializer())
            .setValueSerializationSchema(data.getSerDeserializer())
            .build());
    return builder.build();
  }
}
