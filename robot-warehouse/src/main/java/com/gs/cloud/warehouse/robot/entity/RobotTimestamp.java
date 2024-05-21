package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RobotTimestamp extends FactEntity {

  //机器人sn
  @JsonProperty("product_id")
  private String productId;
  //创建时间
  @JsonProperty("created_at_t")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date timestampUtc;
  //云端收到时间
  @JsonProperty("recv_timestamp_t")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date cldTimestampUtc;

  private Long avgOffset;

  @Override
  public Date getEventTime() {
    return getCldTimestampUtc();
  }

  @Override
  public String getKey() {
    return getProductId();
  }

  @Override
  public CommonFormat<RobotTimestamp> getSerDeserializer() {
    return new CommonFormat<>(RobotTimestamp.class);
  }

  @Override
  public CommonKeySerialization<RobotTimestamp> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.robot-status-v1-diagnosis.notice";
  }

  @Override
  public String getKafkaGroupId() {
    return "kafka.group.id.robot-status-v1-diagnosis.notice";
  }

  @Override
  public int compareTo(BaseEntity o) {
    return 0;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
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

  public Long getOffset() {
    return getCldTimestampUtc().getTime() - getTimestampUtc().getTime();
  }

  public Long getAvgOffset() {
    return avgOffset;
  }

  public void setAvgOffset(Long avgOffset) {
    this.avgOffset = avgOffset;
  }
}

