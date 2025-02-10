package com.gs.cloud.warehouse.ics.source;

import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.util.KafkaSourceUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.util.Properties;

public class KafkaSourceFactory {

  public static <T extends FactEntity> KafkaSource<T> getKafkaSource(Properties properties,
                                                                     T data,
                                                                     String jobName) {
    return KafkaSourceUtils.getKafkaSource(properties, data, jobName);
  }
}
