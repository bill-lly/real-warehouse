package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.format.RobotWorkStateDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RobotWorkState extends FactEntity {

  //机器人sn
  @JsonProperty("product_id")
  private String productId;
  //创建时间
  @JsonProperty("created_at_t")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date createdAtT;
  //云端收到时间
  @JsonProperty("recv_timestamp_t")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date recvTimestampT;
  //工作状态Code
  @JsonProperty("work_state_code")
  private String workStateCode;
  //工作状态desc
  @JsonProperty("work_state")
  private String workState;

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public Date getCreatedAtT() {
    return createdAtT;
  }

  public void setCreatedAtT(Date createdAtT) {
    this.createdAtT = createdAtT;
  }

  public String getWorkStateCode() {
    return workStateCode;
  }

  public void setWorkStateCode(String workStateCode) {
    this.workStateCode = workStateCode;
  }

  public String getWorkState() {
    return workState;
  }

  public void setWorkState(String workState) {
    this.workState = workState;
  }

  public Date getRecvTimestampT() {
    return recvTimestampT;
  }

  public void setRecvTimestampT(Date recvTimestampT) {
    this.recvTimestampT = recvTimestampT;
  }

  public Date getAdjustCreatedAtT() {
    if (createdAtT.getTime() <= recvTimestampT.getTime()) {
      return createdAtT;
    }
    return recvTimestampT;
  }

  public CommonFormat<RobotWorkState> getSerDeserializer() {
    return new CommonFormat<>(RobotWorkState.class);
  }

  @Override
  public CommonKeySerialization<RobotWorkState> getKeySerializer() {
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
  public String toString() {
    return "RobotWorkState{" +
        "productId='" + productId + '\'' +
        ", createdAtT=" + createdAtT +
        ", recvTimestampT=" + recvTimestampT +
        ", workStateCode='" + workStateCode + '\'' +
        ", workState='" + workState + '\'' +
        '}';
  }

  @Override
  public Date getEventTime() {
    return getAdjustCreatedAtT();
  }

  @Override
  public String getKey() {
    return getProductId();
  }

  @Override
  public int compareTo(BaseEntity o) {
    return this.getEventTime().compareTo(o.getEventTime());
  }
}
