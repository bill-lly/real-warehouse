package com.gs.cloud.warehouse.robot.format;

import com.gs.cloud.warehouse.robot.entity.RobotWorkState;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RobotWorkStateDeserializer implements DeserializationSchema<RobotWorkState> {

  private static final transient Charset charset = StandardCharsets.UTF_8;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public RobotWorkState deserialize(byte[] message) throws IOException {
    return objectMapper.readValue(new String(message, charset), RobotWorkState.class);
  }

  @Override
  public boolean isEndOfStream(RobotWorkState nextElement) {
    return false;
  }

  @Override
  public TypeInformation getProducedType() {
    return TypeInformation.of(RobotWorkState.class);
  }
}
