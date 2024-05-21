package com.gs.cloud.warehouse.robot.enums;

//11---
public enum WorkStateLimitationEnum {

  //候梯中
  WAITING_FOR_ELEVATOR("101001","320", "候梯中",1200),
  //乘梯中
  IN_ELEVATOR("101002", "330", "乘梯中", 1200);

  private String code;
  private String workStateCode;
  private String workState;
  private Integer limitation;

  WorkStateLimitationEnum(String code, String workStateCode, String workState, Integer limitation) {
    this.code = code;
    this.workStateCode = workStateCode;
    this.workState = workState;
    this.limitation = limitation;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
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

  public Integer getLimitation() {
    return limitation;
  }

  public void setLimitation(Integer limitation) {
    this.limitation = limitation;
  }
}
