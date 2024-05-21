package com.gs.cloud.warehouse.robot.format;

import com.gs.cloud.warehouse.robot.entity.RobotState;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RobotIsOnlineDeserializer implements DeserializationSchema<RobotState> {

  private static final transient Charset charset = StandardCharsets.UTF_8;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public RobotState deserialize(byte[] message) throws IOException {
    return objectMapper.readValue(new String(message, charset), RobotState.class);
  }

  @Override
  public boolean isEndOfStream(RobotState nextElement) {
    return false;
  }

  @Override
  public TypeInformation getProducedType() {
    return TypeInformation.of(RobotState.class);
  }
}
