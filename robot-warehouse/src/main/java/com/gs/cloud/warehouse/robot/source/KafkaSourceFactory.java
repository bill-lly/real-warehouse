package com.gs.cloud.warehouse.robot.source;

import com.gs.cloud.warehouse.entity.FactEntity;
import org.apache.flink.connector.kafka.source.KafkaSource;
import com.gs.cloud.warehouse.util.KafkaSourceUtils;

import java.util.Properties;

public class KafkaSourceFactory {

  public static <T extends FactEntity> KafkaSource<T> getKafkaSource(Properties properties,
                                                                     T data,
                                                                     String jobName) {
    return KafkaSourceUtils.getKafkaSource(properties, data, jobName);
  }
}
