package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;

import java.util.Calendar;
import java.util.Date;

public class RobotStateMapping extends BaseEntity {

  private String productId;
  private Date startTime;
  private Date endTime;
  private String workStateCode;
  private String workState;
  private Long duration;
  private Integer isOnline;
  private Date recvStartTime;
  private Date recvEndTime;
  //增加维度
  private String robotFamilyCode;
  private String alias;
  private String customerCategory;
  private String scene;
  private String groupName;
  private String terminalUserName;
  private String customerGrade;
  private String deliveryStatus;
  private String businessArea;
  private String udeskMaintGroupName;
  private String udeskMaintLevel;
  private String udeskIcsPromotion;
  private String udeskProjectProperty;

  private static final long t2999;
  static {
    Calendar t = Calendar.getInstance();
    t.set(2999, Calendar.DECEMBER, 31, 15, 59, 59);
    t.set(Calendar.MILLISECOND, 0);
    t2999 = t.getTime().getTime() / 1000;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
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

  public Long getDuration() {
    return duration;
  }

  public void setDuration(Long duration) {
    this.duration = duration;
  }

  public Integer getIsOnline() {
    return isOnline;
  }

  public void setIsOnline(Integer isOnline) {
    this.isOnline = isOnline;
  }

  public Date getRecvStartTime() {
    return recvStartTime;
  }

  public void setRecvStartTime(Date recvStartTime) {
    this.recvStartTime = recvStartTime;
  }

  public Date getRecvEndTime() {
    return recvEndTime;
  }

  public void setRecvEndTime(Date recvEndTime) {
    this.recvEndTime = recvEndTime;
  }

  public boolean isClosed() {
    long endTime = this.endTime.getTime() / 1000;
    return endTime < t2999;
  }

  @Override
  public String toString() {
    return "RobotStateMapping{" +
        "productId='" + productId + '\'' +
        ", startTime=" + startTime +
        ", endTime=" + endTime +
        ", workStateCode='" + workStateCode + '\'' +
        ", workState='" + workState + '\'' +
        ", duration=" + duration +
        ", isOnline=" + isOnline +
        ", recvStartTime=" + recvStartTime +
        ", recvEndTime=" + recvEndTime +
        ", robotFamilyCode='" + robotFamilyCode + '\'' +
        ", alias='" + alias + '\'' +
        ", customerCategory='" + customerCategory + '\'' +
        ", scene='" + scene + '\'' +
        ", groupName='" + groupName + '\'' +
        ", terminalUserName='" + terminalUserName + '\'' +
        ", customerGrade='" + customerGrade + '\'' +
        ", deliveryStatus='" + deliveryStatus + '\'' +
        ", businessArea='" + businessArea + '\'' +
        ", udeskMaintGroupName='" + udeskMaintGroupName + '\'' +
        ", udeskMaintLevel='" + udeskMaintLevel + '\'' +
        ", udeskIcsPromotion='" + udeskIcsPromotion + '\'' +
        ", udeskProjectProperty='" + udeskProjectProperty + '\'' +
        '}';
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
  public int compareTo(BaseEntity o) {
    return 0;
  }

  public String getRobotFamilyCode() {
    return robotFamilyCode;
  }

  public void setRobotFamilyCode(String robotFamilyCode) {
    this.robotFamilyCode = robotFamilyCode;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getCustomerCategory() {
    return customerCategory;
  }

  public void setCustomerCategory(String customerCategory) {
    this.customerCategory = customerCategory;
  }

  public String getScene() {
    return scene;
  }

  public void setScene(String scene) {
    this.scene = scene;
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

  public String getDeliveryStatus() {
    return deliveryStatus;
  }

  public void setDeliveryStatus(String deliveryStatus) {
    this.deliveryStatus = deliveryStatus;
  }

  public String getBusinessArea() {
    return businessArea;
  }

  public void setBusinessArea(String businessArea) {
    this.businessArea = businessArea;
  }

  public String getUdeskMaintGroupName() {
    return udeskMaintGroupName;
  }

  public void setUdeskMaintGroupName(String udeskMaintGroupName) {
    this.udeskMaintGroupName = udeskMaintGroupName;
  }

  public String getUdeskMaintLevel() {
    return udeskMaintLevel;
  }

  public void setUdeskMaintLevel(String udeskMaintLevel) {
    this.udeskMaintLevel = udeskMaintLevel;
  }

  public String getUdeskIcsPromotion() {
    return udeskIcsPromotion;
  }

  public void setUdeskIcsPromotion(String udeskIcsPromotion) {
    this.udeskIcsPromotion = udeskIcsPromotion;
  }

  public String getUdeskProjectProperty() {
    return udeskProjectProperty;
  }

  public void setUdeskProjectProperty(String udeskProjectProperty) {
    this.udeskProjectProperty = udeskProjectProperty;
  }
}

