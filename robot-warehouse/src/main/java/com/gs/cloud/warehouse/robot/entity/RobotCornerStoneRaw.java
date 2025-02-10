package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RobotCornerStoneRaw extends FactEntity {

  private String productId;

  @JsonProperty("X-robot")
  private JsonNode xRobot;

  @JsonProperty("collectData")
  private JsonNode collectData;

  @JsonProperty("publishTimestampMS")
  private String publishTimestamp;

  private String reportTimestampMS;

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public JsonNode getxRobot() {
    return xRobot;
  }

  public void setxRobot(JsonNode xRobot) {
    this.xRobot = xRobot;
  }

  public JsonNode getCollectData() {
    return collectData;
  }

  public void setCollectData(JsonNode collectData) {
    this.collectData = collectData;
  }

  public String getPublishTimestamp() {
    return publishTimestamp;
  }

  public void setPublishTimestamp(String publishTimestamp) {
    this.publishTimestamp = publishTimestamp;
  }

  public String getReportTimestampMS() {
    return reportTimestampMS;
  }

  public void setReportTimestampMS(String reportTimestampMS) {
    this.reportTimestampMS = reportTimestampMS;
  }

  @Override
  public Date getEventTime() {
    return null;
  }

  @Override
  public String getKey() {
    return null;
  }

  @Override
  public CommonFormat<RobotCornerStoneRaw> getSerDeserializer() {
    return new CommonFormat<>(RobotCornerStoneRaw.class);
  }

  @Override
  public CommonKeySerialization<RobotCornerStoneRaw> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.beep";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.robot.cornerstone.raw";
  }

  @Override
  public String getKafkaGroupId() {
    return "kafka.group.id.ods.beep.cornerStoneRaw";
  }

  @Override
  public int compareTo(@NotNull BaseEntity o) {
    return 0;
  }

  @Override
  public String toString() {
    return "RobotCornerStoneRaw{" +
        "xRobot='" + xRobot + '\'' +
        ", collectData='" + collectData + '\'' +
        ", publishTimestamp='" + publishTimestamp + '\'' +
        '}';
  }
}
