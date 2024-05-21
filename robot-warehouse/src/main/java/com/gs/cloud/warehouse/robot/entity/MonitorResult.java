package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MonitorResult extends FactEntity {

  @JsonProperty("productId")
  private String productId;

  @JsonProperty("startTimestamp")
  private Date startTimestamp;

  @JsonProperty("triggerTimestamp")
  private Date triggerTimestamp;

  @JsonProperty("code")
  private String code;

  @JsonProperty("msg")
  private String msg;

  @JsonIgnore
  private String alias;

  @JsonIgnore
  private String softwareVersion;

  @JsonIgnore
  private String customerCode;

  @JsonIgnore
  private String maintenanceRegionCode;

  @JsonIgnore
  private String modelTypeCode;

  public MonitorResult() {}

  public MonitorResult(String productId, Date startTimestamp, Date triggerTimestamp, String code, String msg) {
    this.productId = productId;
    this.startTimestamp = startTimestamp;
    this.triggerTimestamp = triggerTimestamp;
    this.code = code;
    this.msg = msg;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public Date getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(Date startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public String getCode() {
    return code;
  }

  public String getMsg() {
    return msg;
  }

  public Date getTriggerTimestamp() {
    return triggerTimestamp;
  }

  public void setTriggerTimestamp(Date triggerTimestamp) {
    this.triggerTimestamp = triggerTimestamp;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getSoftwareVersion() {
    return softwareVersion;
  }

  public void setSoftwareVersion(String softwareVersion) {
    this.softwareVersion = softwareVersion;
  }

  public String getCustomerCode() {
    return customerCode;
  }

  public void setCustomerCode(String customerCode) {
    this.customerCode = customerCode;
  }

  public String getMaintenanceRegionCode() {
    return maintenanceRegionCode;
  }

  public void setMaintenanceRegionCode(String maintenanceRegionCode) {
    this.maintenanceRegionCode = maintenanceRegionCode;
  }

  public String getModelTypeCode() {
    return modelTypeCode;
  }

  public void setModelTypeCode(String modelTypeCode) {
    this.modelTypeCode = modelTypeCode;
  }

  @Override
  public Date getEventTime() {
    return null;
  }

  @Override
  public String getKey() {
    return getProductId();
  }

  @Override
  public CommonFormat<MonitorResult> getSerDeserializer() {
    return new CommonFormat<>(MonitorResult.class);
  }

  @Override
  public CommonKeySerialization<MonitorResult> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.ads.cld.monitor";
  }

  @Override
  public String getKafkaGroupId() {
    return null;
  }

  @Override
  public int compareTo(@NotNull BaseEntity o) {
    return 0;
  }

  @Override
  public String toString() {
    return "MonitorResult{" +
        "productId='" + productId + '\'' +
        ", startTimestamp=" + startTimestamp +
        ", triggerTimestamp=" + triggerTimestamp +
        ", code='" + code + '\'' +
        ", msg='" + msg + '\'' +
        '}';
  }
}
