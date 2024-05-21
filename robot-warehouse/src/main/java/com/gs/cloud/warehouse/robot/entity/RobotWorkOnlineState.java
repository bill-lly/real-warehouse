package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.enums.OnlineEnum;

import java.util.Date;

public class RobotWorkOnlineState extends BaseEntity {

  //机器人sn
  private String productId;

  //车端时间
  private Date timestampUtc;

  //云端时间
  private Date cldTimestampUtc;

  //工作状态Code
  private String state;

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

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public boolean isWorkState() {
    return !state.equals(OnlineEnum.ONLINE.getCode()) && !state.equals(OnlineEnum.OFFLINE.getCode());
  }

  public boolean isOnline() {
    return state.equals(OnlineEnum.ONLINE.getCode());
  }

  public boolean isOffline() {
    return state.equals(OnlineEnum.OFFLINE.getCode());
  }

  public Long getOffset() {
    if (isWorkState()) {
      return getCldTimestampUtc().getTime() - getTimestampUtc().getTime();
    }
    return null;
  }

  @Override
  public Date getEventTime() {
    return getCldTimestampUtc();
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
    return "RobotWorkOnlineState{" +
        "productId='" + productId + '\'' +
        ", timestampUtc=" + timestampUtc +
        ", cldTimestampUtc=" + cldTimestampUtc +
        ", state='" + state + '\'' +
        '}';
  }
}
