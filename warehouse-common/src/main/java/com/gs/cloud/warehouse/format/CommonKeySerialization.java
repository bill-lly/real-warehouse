package com.gs.cloud.warehouse.format;

import com.gs.cloud.warehouse.entity.FactEntity;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

public class CommonKeySerialization<T extends FactEntity> implements SerializationSchema<T> {

  @Override
  public byte[] serialize(T element) {
    return element.getKey().getBytes(StandardCharsets.UTF_8);
  }
}
