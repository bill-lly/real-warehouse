package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;

import java.util.Date;

public class RobotDiagnosisData extends FactEntity {

  //数据id
  private String id;
  //机器人sn
  private String productId;
  //车端时间
  private Date timestampUtc;
  //云端时间
  private Date cldTimestampUtc;
  // name
  private String name;
  //故障 code
  private String code;
  //故障level desc
  private String level;
  // 状态开始时间
  private Date startTime;
  // 状态开始时间
  private Date endTime;
  //在离线状态
  private String attachedOnlineStatus;
  // 工作状态
  private String attachedWorkStatus;
  // 当前任务状态
  private String attachedTask;
  // 急停状态
  private String attachedEssStatus;
  // 事件名称
  private String eventType;
  // 事件名称
  private String eventName;
  // priority null 排班 0 在离线 状态数据 1 任务开始 2 任务结束 3 告警开始 4 告警结束 6 事件单 7 机器人状态2.0
  private int priority;
  // Data
  private String data;
  // Source 1 在离线 2 状态数据 3 告警代替的任务时间线 4 告警 5 排班 6 事件单 7 机器人状态2.0
  private int source;
  // 状态2.0 key
  private String stateKeyCn;
  // 状态2.0 value
  private String stateValueCn;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getAttachedTask() {
    return attachedTask;
  }

  public void setAttachedTask(String attachedTask) {
    this.attachedTask = attachedTask;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
  public String getAttachedOnlineStatus() {
    return attachedOnlineStatus;
  }

  public void setAttachedOnlineStatus(String attachedOnlineStatus) {
    this.attachedOnlineStatus = attachedOnlineStatus;
  }
  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public String getAttachedWorkStatus() {
    return attachedWorkStatus;
  }

  public void setAttachedWorkStatus(String attachedWorkStatus) {
    this.attachedWorkStatus = attachedWorkStatus;
  }

  public String getAttachedEssStatus() {
    return attachedEssStatus;
  }

  public void setAttachedEssStatus(String attachedEssStatus) {
    this.attachedEssStatus = attachedEssStatus;
  }

  public String getEventName() {
    return eventName;
  }

  public void setEventName(String eventName) {
    this.eventName = eventName;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
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

  public int getSource() {
    return source;
  }

  public void setSource(int source) {
    this.source = source;
  }

  public String getStateKeyCn() {
    return stateKeyCn;
  }

  public void setStateKeyCn(String stateKeyCn) {
    this.stateKeyCn = stateKeyCn;
  }

  public String getStateValueCn() {
    return stateValueCn;
  }

  public void setStateValueCn(String stateValueCn) {
    this.stateValueCn = stateValueCn;
  }

  @Override
  public Date getEventTime() {
    return timestampUtc;
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
  public CommonFormat<RobotDiagnosisData> getSerDeserializer() {
    return new CommonFormat<>(RobotDiagnosisData.class);
  }

  @Override
  public CommonKeySerialization<RobotDiagnosisData> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers.bigdata";
  }

  @Override
  public String getKafkaTopic() {
    return "bigdata.dws.robot.diagnosis.timeline";
  }

  @Override
  public String getKafkaGroupId() {
    return null;
  }

  @Override
  public String toString() {
    return "RobotDiagnosisData{" +
            "id='" + id + '\'' +
            ", productId='" + productId + '\'' +
            ", timestampUtc=" + timestampUtc +
            ", cldTimestampUtc=" + cldTimestampUtc +
            ", name='" + name + '\'' +
            ", code='" + code + '\'' +
            ", level='" + level + '\'' +
            ", startTime=" + startTime +
            ", endTime=" + endTime +
            ", attachedOnlineStatus='" + attachedOnlineStatus + '\'' +
            ", attachedWorkStatus='" + attachedWorkStatus + '\'' +
            ", attachedTask='" + attachedTask + '\'' +
            ", attachedEssStatus='" + attachedEssStatus + '\'' +
            ", eventType='" + eventType + '\'' +
            ", eventName='" + eventName + '\'' +
            ", priority=" + priority +
            ", data='" + data + '\'' +
            ", source=" + source +
            ", stateKeyCn='" + stateKeyCn + '\'' +
            ", stateValueCn='" + stateValueCn + '\'' +
            '}';
  }
}
