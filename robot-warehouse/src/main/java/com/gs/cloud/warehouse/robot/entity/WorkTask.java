package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkTask extends FactEntity {

  @JsonProperty("id")
  private String id;

  @JsonProperty("product_id")
  private String productId;

  @JsonProperty("task_code")
  private String taskCode;

  @JsonProperty("task_title")
  private String taskTitle;

  @JsonProperty("task_source")
  private String taskSource;

  @JsonProperty("report_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date reportTime;

  @JsonProperty("need_rm_intervention")
  private Integer needRmIntervention;

  @JsonProperty("state")
  private String state;

  @JsonProperty("remark")
  private String remark;

  @JsonProperty("create_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date createTime;

  @JsonProperty("update_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date updateTime;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public String getTaskCode() {
    return taskCode;
  }

  public void setTaskCode(String taskCode) {
    this.taskCode = taskCode;
  }

  public String getTaskTitle() {
    return taskTitle;
  }

  public void setTaskTitle(String taskTitle) {
    this.taskTitle = taskTitle;
  }

  public String getTaskSource() {
    return taskSource;
  }

  public void setTaskSource(String taskSource) {
    this.taskSource = taskSource;
  }

  public Date getReportTime() {
    return reportTime;
  }

  public void setReportTime(Date reportTime) {
    this.reportTime = reportTime;
  }

  public Integer getNeedRmIntervention() {
    return needRmIntervention;
  }

  public void setNeedRmIntervention(Integer needRmIntervention) {
    this.needRmIntervention = needRmIntervention;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getRemark() {
    return remark;
  }

  public void setRemark(String remark) {
    this.remark = remark;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }

  public boolean isIgnored() {
    return "IGNORED".equals(getState()) || "RULE_IGNORED".equals(getState());
  }

  @Override
  public CommonFormat<WorkTask> getSerDeserializer() {
    return new CommonFormat<>(WorkTask.class);
  }

  @Override
  public CommonKeySerialization<WorkTask> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.dwd.maint.workTask";
  }

  @Override
  public String getKafkaGroupId() {
    return "kafka.group.id.dwd.maint.workTask";
  }

  @Override
  public Date getEventTime() {
    return getUpdateTime();
  }

  @Override
  public String getKey() {
    return getProductId();
  }

  @Override
  public int compareTo(BaseEntity o) {
    return this.getEventTime().compareTo(o.getEventTime());
  }

  @Override
  public String toString() {
    return "WorkTask{" +
        "id='" + id + '\'' +
        ", productId='" + productId + '\'' +
        ", taskCode='" + taskCode + '\'' +
        ", taskTitle='" + taskTitle + '\'' +
        ", taskSource='" + taskSource + '\'' +
        ", reportTime=" + reportTime +
        ", needRmIntervention=" + needRmIntervention +
        ", state='" + state + '\'' +
        ", remark='" + remark + '\'' +
        ", createTime=" + createTime +
        ", updateTime=" + updateTime +
        '}';
  }
}
