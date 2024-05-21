package com.gs.cloud.warehouse.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class CommonFormat<T> implements DeserializationSchema<T>, SerializationSchema<T> {

  private static final transient Charset charset = StandardCharsets.UTF_8;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final Class<T> clazz;

  public CommonFormat(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T deserialize(byte[] message) throws IOException {
    return objectMapper.readValue(new String(message, charset), clazz);
  }

  @Override
  public byte[] serialize(T element) {
    try {
      String str = objectMapper.writeValueAsString(element);
      return str.getBytes(StandardCharsets.UTF_8);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isEndOfStream(T nextElement) {
    return false;
  }

  @Override
  public TypeInformation getProducedType() {
    return TypeInformation.of(clazz);
  }

}
