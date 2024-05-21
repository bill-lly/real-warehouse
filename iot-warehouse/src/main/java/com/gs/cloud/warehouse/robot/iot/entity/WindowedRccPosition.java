package com.gs.cloud.warehouse.robot.iot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WindowedRccPosition extends FactEntity {

  @JsonProperty("deviceId")
  private String deviceId;

  @JsonProperty("productId")
  private String productId;

  @JsonProperty("windowStartTimeUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date windowStartTimeUtc;

  @JsonProperty("windowEndTimeUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date windowEndTimeUtc;

  @JsonProperty("cldStartTimeUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date cldStartTimeUtc;

  @JsonProperty("cldEndTimeUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date cldEndTimeUtc;

  @JsonProperty("startTimeUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date startTimeUtc;

  @JsonProperty("endTimeUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date endTimeUtc;

  @JsonProperty("distance")
  private Double distance;

  @JsonProperty("effectiveDistance")
  private Double effectiveDistance;

  @JsonProperty("angle")
  private Double angle;

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public Date getWindowStartTimeUtc() {
    return windowStartTimeUtc;
  }

  public void setWindowStartTimeUtc(Date windowStartTimeUtc) {
    this.windowStartTimeUtc = windowStartTimeUtc;
  }

  public Date getWindowEndTimeUtc() {
    return windowEndTimeUtc;
  }

  public void setWindowEndTimeUtc(Date windowEndTimeUtc) {
    this.windowEndTimeUtc = windowEndTimeUtc;
  }

  public Date getCldStartTimeUtc() {
    return cldStartTimeUtc;
  }

  public void setCldStartTimeUtc(Date cldStartTimeUtc) {
    this.cldStartTimeUtc = cldStartTimeUtc;
  }

  public Date getCldEndTimeUtc() {
    return cldEndTimeUtc;
  }

  public void setCldEndTimeUtc(Date cldEndTimeUtc) {
    this.cldEndTimeUtc = cldEndTimeUtc;
  }

  public Date getStartTimeUtc() {
    return startTimeUtc;
  }

  public void setStartTimeUtc(Date startTimeUtc) {
    this.startTimeUtc = startTimeUtc;
  }

  public Date getEndTimeUtc() {
    return endTimeUtc;
  }

  public void setEndTimeUtc(Date endTimeUtc) {
    this.endTimeUtc = endTimeUtc;
  }

  public Double getDistance() {
    return distance;
  }

  public void setDistance(Double distance) {
    this.distance = distance;
  }

  public Double getEffectiveDistance() {
    return effectiveDistance;
  }

  public void setEffectiveDistance(Double effectiveDistance) {
    this.effectiveDistance = effectiveDistance;
  }

  public Double getAngle() {
    return angle;
  }

  public void setAngle(Double angle) {
    this.angle = angle;
  }

  @Override
  public String toString() {
    return "WindowedRccPosition{" +
        "deviceId='" + deviceId + '\'' +
        ", productId='" + productId + '\'' +
        ", windowStartTimeUtc=" + windowStartTimeUtc +
        ", windowEndTimeUtc=" + windowEndTimeUtc +
        ", cldStartTimeUtc=" + cldStartTimeUtc +
        ", cldEndTimeUtc=" + cldEndTimeUtc +
        ", startTimeUtc=" + startTimeUtc +
        ", endTimeUtc=" + endTimeUtc +
        ", distance=" + distance +
        ", effectiveDistance=" + effectiveDistance +
        ", angle=" + angle +
        '}';
  }

  @Override
  public Date getEventTime() {
    return getCldStartTimeUtc();
  }

  @Override
  public String getKey() {
    return getDeviceId();
  }

  @Override
  public CommonFormat<WindowedRccPosition> getSerDeserializer() {
    return new CommonFormat<>(WindowedRccPosition.class);
  }

  @Override
  public CommonKeySerialization<WindowedRccPosition> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return null;
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.rcc.robot.displacement";
  }

  @Override
  public String getKafkaGroupId() {
    return null;
  }

  @Override
  public int compareTo(BaseEntity o) {
    return getEventTime().compareTo(o.getEventTime());
  }
}
