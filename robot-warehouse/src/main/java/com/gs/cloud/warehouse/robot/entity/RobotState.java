package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.enums.OnlineEnum;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RobotState extends FactEntity {

  @JsonProperty("product_id")
  private String productId;

  //工作状态
  @JsonProperty("work_status")
  private String workStatus;

  //工作状态云端收到时间utc
  @JsonProperty("last_mqtt_online_utc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date lastMqttOnlineUtc;

  //工作状态车端时间戳
  @JsonProperty("local_timestamp")
  private Long localTimestamp;

  //在离线状态  1:在线; 0:离线
  @JsonProperty("is_online")
  private Integer isOnline;

  //is_online=1，在离线云接受时间utc
  @JsonProperty("last_online_utc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date lastOnlineUtc;

  //is_online=0，在离线云接受时间utc
  @JsonProperty("last_offline_utc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date lastOfflineUtc;

  @JsonProperty("cld_created_at_utc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date cldCreatedAtUtc;

  @JsonProperty("cld_updated_at_utc")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date cldUpdatedAtUtc;

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public Integer getIsOnline() {
    return isOnline;
  }

  public void setIsOnline(Integer isOnline) {
    this.isOnline = isOnline;
  }

  public Date getCldCreatedAtUtc() {
    return cldCreatedAtUtc;
  }

  public void setCldCreatedAtUtc(Date cldCreatedAtUtc) {
    this.cldCreatedAtUtc = cldCreatedAtUtc;
  }

  public Date getCldUpdatedAtUtc() {
    return cldUpdatedAtUtc;
  }

  public void setCldUpdatedAtUtc(Date cldUpdatedAtUtc) {
    this.cldUpdatedAtUtc = cldUpdatedAtUtc;
  }

  public Date getCldDate() {
    return cldUpdatedAtUtc == null ? cldCreatedAtUtc : cldUpdatedAtUtc;
  }

  public String getWorkStatus() {
    return workStatus;
  }

  public void setWorkStatus(String workStatus) {
    this.workStatus = workStatus;
  }

  public Date getLastMqttOnlineUtc() {
    return lastMqttOnlineUtc;
  }

  public void setLastMqttOnlineUtc(Date lastMqttOnlineUtc) {
    this.lastMqttOnlineUtc = lastMqttOnlineUtc;
  }

  public Long getLocalTimestamp() {
    return localTimestamp;
  }

  public void setLocalTimestamp(Long localTimestamp) {
    this.localTimestamp = localTimestamp;
  }

  public Date getLastOnlineUtc() {
    return lastOnlineUtc;
  }

  public void setLastOnlineUtc(Date lastOnlineUtc) {
    this.lastOnlineUtc = lastOnlineUtc;
  }

  public Date getLastOfflineUtc() {
    return lastOfflineUtc;
  }

  public void setLastOfflineUtc(Date lastOfflineUtc) {
    this.lastOfflineUtc = lastOfflineUtc;
  }

  public String getIsOnlineCode() {
    return isOnline == 1 ? OnlineEnum.ONLINE.getCode() : OnlineEnum.OFFLINE.getCode();
  }

  public boolean isNewData() {
    return getWorkStatus() != null && getLocalTimestamp() != null
        && getLastMqttOnlineUtc() != null && getIsOnline() != null
        && getLastOnlineUtc() != null && getLastOfflineUtc() != null;
  }

  @Override
  public CommonFormat<RobotState> getSerDeserializer() {
    return new CommonFormat<>(RobotState.class);
  }

  @Override
  public CommonKeySerialization<RobotState> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.dim.bot.robot";
  }

  @Override
  public String getKafkaGroupId() {
    return "kafka.group.id.dim.bot.robot";
  }

  @Override
  public String toString() {
    return "RobotState{" +
        "productId='" + productId + '\'' +
        ", workStatus='" + workStatus + '\'' +
        ", lastMqttOnlineUtc=" + lastMqttOnlineUtc +
        ", localTimestamp=" + localTimestamp +
        ", isOnline=" + isOnline +
        ", lastOnlineUtc=" + lastOnlineUtc +
        ", lastOfflineUtc=" + lastOfflineUtc +
        ", cldCreatedAtUtc=" + cldCreatedAtUtc +
        ", cldUpdatedAtUtc=" + cldUpdatedAtUtc +
        '}';
  }

  @Override
  public Date getEventTime() {
    return getCldDate();
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
