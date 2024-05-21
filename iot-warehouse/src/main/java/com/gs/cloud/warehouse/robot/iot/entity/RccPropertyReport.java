package com.gs.cloud.warehouse.robot.iot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RccPropertyReport extends FactEntity {

  @JsonProperty("deviceId")
  private String deviceId;

  @JsonProperty("productId")
  private String productId;

  @JsonProperty("unixTimestamp")
  private Long unixTimestamp;

  @JsonProperty("cldUnixTimestamp")
  private Long cldUnixTimestamp;

  @JsonProperty("timestampUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date timestampUtc;

  @JsonProperty("cldTimestampUtc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date cldTimestampUtc;

  @JsonProperty("x")
  private Integer x;

  @JsonProperty("y")
  private Integer y;

  @JsonProperty("angle")
  private Double angle;

  private String pt;

  private String timestampUtcStr;

  private String cldTimestampUtcStr;

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

  public Long getUnixTimestamp() {
    return unixTimestamp;
  }

  public void setUnixTimestamp(Long unixTimestamp) {
    this.unixTimestamp = unixTimestamp;
  }

  public Long getCldUnixTimestamp() {
    return cldUnixTimestamp;
  }

  public void setCldUnixTimestamp(Long cldUnixTimestamp) {
    this.cldUnixTimestamp = cldUnixTimestamp;
  }

  public Date getTimestampUtc() {
    return timestampUtc;
  }

  public void setTimestampUtc(Date timestampUtc) {
    this.timestampUtc = timestampUtc;
  }

  public Date getCldTimestampUtc() {
    return cldTimestampUtc;
  }

  public void setCldTimestampUtc(Date cldTimestampUtc) {
    this.cldTimestampUtc = cldTimestampUtc;
  }

  public Integer getX() {
    return x;
  }

  public void setX(Integer x) {
    this.x = x;
  }

  public Integer getY() {
    return y;
  }

  public void setY(Integer y) {
    this.y = y;
  }

  public Double getAngle() {
    return angle;
  }

  public void setAngle(Double angle) {
    this.angle = angle;
  }

  public String getPt() {
    return pt;
  }

  public void setPt(String pt) {
    this.pt = pt;
  }

  @JsonIgnore
  public String getTimestampUtcStr() {
    return timestampUtcStr;
  }

  public void setTimestampUtcStr(String timestampUtcStr) {
    this.timestampUtcStr = timestampUtcStr;
  }

  @JsonIgnore
  public String getCldTimestampUtcStr() {
    return cldTimestampUtcStr;
  }

  public void setCldTimestampUtcStr(String cldTimestampUtcStr) {
    this.cldTimestampUtcStr = cldTimestampUtcStr;
  }

  @Override
  public String toString() {
    return "RccPropertyReport{" +
        "deviceId='" + deviceId + '\'' +
        ", productId='" + productId + '\'' +
        ", unixTimestampUtc=" + unixTimestamp +
        ", cldUnixTimestampUtc=" + cldUnixTimestamp +
        ", timestampUtc=" + timestampUtc +
        ", cldTimestampUtc=" + cldTimestampUtc +
        ", x=" + x +
        ", y=" + y +
        ", angle=" + angle +
        ", pt='" + pt + '\'' +
        '}';
  }

  @Override
  public Date getEventTime() {
    return getCldTimestampUtc();
  }

  @Override
  public String getKey() {
    return getDeviceId();
  }

  @Override
  public CommonFormat<RccPropertyReport> getSerDeserializer() {
    return new CommonFormat<>(RccPropertyReport.class);
  }

  @Override
  public CommonKeySerialization<RccPropertyReport> getKeySerializer() {
    return new CommonKeySerialization<>();

  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.rcc.property.report";
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
