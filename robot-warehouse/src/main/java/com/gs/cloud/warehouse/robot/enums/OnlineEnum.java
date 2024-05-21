package com.gs.cloud.warehouse.robot.enums;

public enum OnlineEnum {

  ONLINE("online"),
  OFFLINE("offline"),
  ONLINE_OFFLINE("online_offline");

  private String code;

  OnlineEnum(String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }
}
