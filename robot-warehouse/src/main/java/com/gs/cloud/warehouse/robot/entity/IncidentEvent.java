package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.enums.TaskStatusEnum;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IncidentEvent extends FactEntity {

  @JsonProperty("id")
  private String id;

  @JsonProperty("event_id")
  private String eventId;

  @JsonProperty("status")
  private String status;

  @JsonProperty("product_id")
  private String productId;

  @JsonProperty("subject_type")
  private String subjectType;

  @JsonProperty("subject_model")
  private String subjectModel;

  @JsonProperty("incident_code")
  private String incidentCode;

  @JsonProperty("incident_start_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date incidentStartTime;

  @JsonProperty("event_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date entityEventTime;

  @JsonProperty("finalized")
  private String finalized;

  @JsonProperty("create_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date createTime;

  @JsonProperty("update_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date updateTime;

  @JsonProperty("clean_type")
  private String cleanType;

  @JsonProperty("group_name")
  private String groupName;

  @JsonProperty("terminal_user_name")
  private String terminalUserName;

  @JsonProperty("customer_grade")
  private String customerGrade;

  @JsonProperty("robot_family_code")
  private String robotFamilyCode;

  @JsonProperty("business_area")
  private String businessArea;

  private String incidentTitle;

  private String incidentLevel;

  private int isBlockingTask;

  private String otherEffective;

  public String getIncidentTitle() {
    return incidentTitle;
  }

  public void setIncidentTitle(String incidentTitle) {
    this.incidentTitle = incidentTitle;
  }

  public String getIncidentLevel() {
    return incidentLevel;
  }

  public void setIncidentLevel(String incidentLevel) {
    this.incidentLevel = incidentLevel;
  }

  public int getIsBlockingTask() {
    return isBlockingTask;
  }

  public void setIsBlockingTask(int isBlockingTask) {
    this.isBlockingTask = isBlockingTask;
  }

  public String getOtherEffective() {
    return otherEffective;
  }

  public void setOtherEffective(String otherEffective) {
    this.otherEffective = otherEffective;
  }

  private boolean createTicket;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public String getSubjectType() {
    return subjectType;
  }

  public void setSubjectType(String subjectType) {
    this.subjectType = subjectType;
  }

  public String getSubjectModel() {
    return subjectModel;
  }

  public void setSubjectModel(String subjectModel) {
    this.subjectModel = subjectModel;
  }

  public String getIncidentCode() {
    return incidentCode;
  }

  public void setIncidentCode(String incidentCode) {
    this.incidentCode = incidentCode;
  }

  public String getFinalized() {
    return finalized;
  }

  public void setFinalized(String finalized) {
    this.finalized = finalized;
  }

  public String getCleanType() {
    return cleanType;
  }

  public void setCleanType(String cleanType) {
    this.cleanType = cleanType;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public String getTerminalUserName() {
    return terminalUserName;
  }

  public void setTerminalUserName(String terminalUserName) {
    this.terminalUserName = terminalUserName;
  }

  public String getCustomerGrade() {
    return customerGrade;
  }

  public void setCustomerGrade(String customerGrade) {
    this.customerGrade = customerGrade;
  }

  public String getRobotFamilyCode() {
    return robotFamilyCode;
  }

  public void setRobotFamilyCode(String robotFamilyCode) {
    this.robotFamilyCode = robotFamilyCode;
  }

  public String getBusinessArea() {
    return businessArea;
  }

  public void setBusinessArea(String businessArea) {
    this.businessArea = businessArea;
  }

  public Date getIncidentStartTime() {
    return incidentStartTime;
  }

  public void setIncidentStartTime(Date incidentStartTime) {
    this.incidentStartTime = incidentStartTime;
  }

  public Date getEntityEventTime() {
    return entityEventTime;
  }

  public void setEntityEventTime(Date entityEventTime) {
    this.entityEventTime = entityEventTime;
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

  public boolean isFault() {
    return "1".equals(getStatus()) || "11".equals(getStatus());
  }

  public boolean isTaskStatus() {
    return TaskStatusEnum.TASK_START_20102.getCode().equals(getIncidentCode())
        || TaskStatusEnum.TASK_START_50001.getCode().equals(getIncidentCode())
        || TaskStatusEnum.TASK_END_20103.getCode().equals(getIncidentCode())
        || TaskStatusEnum.TASK_END_50002.getCode().equals(getIncidentCode());
  }

  public boolean isTaskStart() {
    return TaskStatusEnum.TASK_START_20102.getCode().equals(getIncidentCode())
        || TaskStatusEnum.TASK_START_50001.getCode().equals(getIncidentCode());
  }

  public boolean isTaskEnd() {
    return TaskStatusEnum.TASK_END_20103.getCode().equals(getIncidentCode())
        || TaskStatusEnum.TASK_END_50002.getCode().equals(getIncidentCode());
  }

  public boolean isCreateTicket() {
    return createTicket;
  }

  public void setCreateTicket(boolean createTicket) {
    this.createTicket = createTicket;
  }

  @Override
  public CommonFormat<IncidentEvent> getSerDeserializer() {
    return new CommonFormat<>(IncidentEvent.class);
  }


  @Override
  public CommonKeySerialization<IncidentEvent> getKeySerializer() {
    return null;
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.dwd.maint.incidentEvent";
  }

  @Override
  public String getKafkaGroupId() {
    return "kafka.group.id.dwd.maint.incidentEvent";
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

  public Long getOffset() {
    return getUpdateTime().getTime() - getEntityEventTime().getTime();
  }

  @Override
  public String toString() {
    return "IncidentEvent{" +
        "id='" + id + '\'' +
        ", eventId='" + eventId + '\'' +
        ", status='" + status + '\'' +
        ", productId='" + productId + '\'' +
        ", subjectType='" + subjectType + '\'' +
        ", subjectModel='" + subjectModel + '\'' +
        ", incidentCode='" + incidentCode + '\'' +
        ", incidentStartTime=" + incidentStartTime +
        ", entityEventTime=" + entityEventTime +
        ", finalized='" + finalized + '\'' +
        ", createTime=" + createTime +
        ", updateTime=" + updateTime +
        ", cleanType='" + cleanType + '\'' +
        ", groupName='" + groupName + '\'' +
        ", terminalUserName='" + terminalUserName + '\'' +
        ", customerGrade='" + customerGrade + '\'' +
        ", robotFamilyCode='" + robotFamilyCode + '\'' +
        ", businessArea='" + businessArea + '\'' +
        ", incidentTitle='" + incidentTitle +'\'' +
        ", incidentLevel='"  +incidentLevel +'\'' +
        ", isBlockingTask="  +isBlockingTask +
        ", otherEffective='" +otherEffective +
          '}';
  }
}
