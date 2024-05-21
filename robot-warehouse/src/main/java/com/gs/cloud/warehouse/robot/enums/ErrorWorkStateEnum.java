package com.gs.cloud.warehouse.robot.enums;

public enum ErrorWorkStateEnum {

  AUTO_TASK_PAUSED("220", "自动任务暂停"),
  UNINT("110", "定位丢失"),
  ELEVATOR_PAUSE("260", "乘梯暂停"),
  NAVIGATING_PAUSED("240", "导航暂停"),
  MULTI_TASK_PAUSE("290", "梯控任务暂停");

  private String code;
  private String desc;

  ErrorWorkStateEnum(String code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }
}
