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
public class EventTicket extends FactEntity {

  @JsonProperty("ticket_num")
  private String ticketNum;

  @JsonProperty("ticket_state")
  private String ticketState;

  @JsonProperty("create_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date createTime;

  @JsonProperty("update_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date updateTime;

  @JsonProperty("product_id")
  private String productId;

  @JsonProperty("alias")
  private String alias;

  @JsonProperty("terminal_customer")
  private String terminalCustomer;

  @JsonProperty("task_abnormal_duration")
  private Long taskAbnormalDuration;

  @JsonProperty("incident_count")
  private Integer incidentCount;

  @JsonProperty("software_version")
  private String softwareVersion;

  @JsonProperty("robot_family_code")
  private String robotFamilyCode;

  @JsonProperty("business_area")
  private String businessArea;

  @JsonProperty("handler_name")
  private String handlerName;

  @JsonProperty("handle_start_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date handleStartTime;

  @JsonProperty("handle_end_time")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date handleEndTime;

  @JsonProperty("opening_duration")
  private Long openingDuration;

  @JsonProperty("pending_duration")
  private Long pendingDuration;

  @JsonProperty("remote_processed_duration")
  private Long remoteProcessedDuration;

  @JsonProperty("incident_minis")
  private String incidentMinis;

  @JsonProperty("remark")
  private String remark;

  @JsonProperty("assignee_or_dealWay")
  private String assigneeOrDealWay;

  @JsonProperty("assignReason_or_solution")
  private String assignReasonOrSolution;

  @JsonProperty("metas")
  private String metas;

  @JsonProperty("tags")
  private String tags;

  @JsonProperty("category")
  private String category;

  @JsonProperty("robot_stop_incident_count")
  private Integer robotStopIncidentCount;

  public String getTicketNum() {
    return ticketNum;
  }

  public void setTicketNum(String ticketNum) {
    this.ticketNum = ticketNum;
  }

  public String getTicketState() {
    return ticketState;
  }

  public void setTicketState(String ticketState) {
    this.ticketState = ticketState;
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

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getTerminalCustomer() {
    return terminalCustomer;
  }

  public void setTerminalCustomer(String terminalCustomer) {
    this.terminalCustomer = terminalCustomer;
  }

  public Long getTaskAbnormalDuration() {
    return taskAbnormalDuration;
  }

  public void setTaskAbnormalDuration(Long taskAbnormalDuration) {
    this.taskAbnormalDuration = taskAbnormalDuration;
  }

  public Integer getIncidentCount() {
    return incidentCount;
  }

  public void setIncidentCount(Integer incidentCount) {
    this.incidentCount = incidentCount;
  }

  public String getSoftwareVersion() {
    return softwareVersion;
  }

  public void setSoftwareVersion(String softwareVersion) {
    this.softwareVersion = softwareVersion;
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

  public String getHandlerName() {
    return handlerName;
  }

  public void setHandlerName(String handlerName) {
    this.handlerName = handlerName;
  }

  public Date getHandleStartTime() {
    return handleStartTime;
  }

  public void setHandleStartTime(Date handleStartTime) {
    this.handleStartTime = handleStartTime;
  }

  public Date getHandleEndTime() {
    return handleEndTime;
  }

  public void setHandleEndTime(Date handleEndTime) {
    this.handleEndTime = handleEndTime;
  }

  public Long getOpeningDuration() {
    return openingDuration;
  }

  public void setOpeningDuration(Long openingDuration) {
    this.openingDuration = openingDuration;
  }

  public Long getPendingDuration() {
    return pendingDuration;
  }

  public void setPendingDuration(Long pendingDuration) {
    this.pendingDuration = pendingDuration;
  }

  public Long getRemoteProcessedDuration() {
    return remoteProcessedDuration;
  }

  public void setRemoteProcessedDuration(Long remoteProcessedDuration) {
    this.remoteProcessedDuration = remoteProcessedDuration;
  }

  public String getIncidentMinis() {
    return incidentMinis;
  }

  public void setIncidentMinis(String incidentMinis) {
    this.incidentMinis = incidentMinis;
  }

  public String getRemark() {
    return remark;
  }

  public void setRemark(String remark) {
    this.remark = remark;
  }

  public String getAssigneeOrDealWay() {
    return assigneeOrDealWay;
  }

  public void setAssigneeOrDealWay(String assigneeOrDealWay) {
    this.assigneeOrDealWay = assigneeOrDealWay;
  }

  public String getAssignReasonOrSolution() {
    return assignReasonOrSolution;
  }

  public void setAssignReasonOrSolution(String assignReasonOrSolution) {
    this.assignReasonOrSolution = assignReasonOrSolution;
  }

  public String getMetas() {
    return metas;
  }

  public void setMetas(String metas) {
    this.metas = metas;
  }

  public String getTags() {
    return tags;
  }

  public void setTags(String tags) {
    this.tags = tags;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public Integer getRobotStopIncidentCount() {
    return robotStopIncidentCount;
  }

  public void setRobotStopIncidentCount(Integer robotStopIncidentCount) {
    this.robotStopIncidentCount = robotStopIncidentCount;
  }

  public boolean isCreated() {
    return "PENDING".equals(getTicketState());
  }

  public boolean isClosed() {
    return "RESOLVED".equals(getTicketState());
  }

  public boolean isSystemClosed() {
    return "RESOLVED".equals(getTicketState()) && "系统".equals(getHandlerName());
  }

  @Override
  public CommonFormat<EventTicket> getSerDeserializer() {
    return new CommonFormat<>(EventTicket.class);
  }

  @Override
  public CommonKeySerialization<EventTicket> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.dwd.maint.eventTicket";
  }

  @Override
  public String getKafkaGroupId() {
    return "kafka.group.id.dwd.maint.eventTicket";
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
    return "EventTicket{" +
        "ticketNum='" + ticketNum + '\'' +
        ", ticketState='" + ticketState + '\'' +
        ", createTime=" + createTime +
        ", updateTime=" + updateTime +
        ", productId='" + productId + '\'' +
        ", alias='" + alias + '\'' +
        ", terminalCustomer='" + terminalCustomer + '\'' +
        ", taskAbnormalDuration=" + taskAbnormalDuration +
        ", incidentCount=" + incidentCount +
        ", softwareVersion='" + softwareVersion + '\'' +
        ", robotFamilyCode='" + robotFamilyCode + '\'' +
        ", businessArea='" + businessArea + '\'' +
        ", handlerName='" + handlerName + '\'' +
        ", handleStartTime=" + handleStartTime +
        ", handleEndTime=" + handleEndTime +
        ", openingDuration=" + openingDuration +
        ", pendingDuration=" + pendingDuration +
        ", remoteProcessedDuration=" + remoteProcessedDuration +
        ", incidentMinis='" + incidentMinis + '\'' +
        ", remark='" + remark + '\'' +
        ", assigneeOrDealWay='" + assigneeOrDealWay + '\'' +
        ", assignReasonOrSolution='" + assignReasonOrSolution + '\'' +
        ", metas='" + metas + '\'' +
        ", tags='" + tags + '\'' +
        ", category='" + category + '\'' +
        ", robotStopIncidentCount=" + robotStopIncidentCount +
        '}';
  }
}
