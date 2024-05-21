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
public class RobotIsOnline extends FactEntity {

  @JsonProperty("product_id")
  private String productId;
  //在离线状态  1:在线; 0:离线
  @JsonProperty("is_online")
  private Integer isOnline;

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

  @Override
  public CommonFormat<RobotIsOnline> getSerDeserializer() {
    return new CommonFormat<>(RobotIsOnline.class);
  }

  @Override
  public CommonKeySerialization<RobotIsOnline> getKeySerializer() {
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
    return "RobotIsOnline{" +
        "productId='" + productId + '\'' +
        ", isOnline='" + isOnline + '\'' +
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
