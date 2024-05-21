package com.gs.cloud.warehouse.robot.enums;

public enum TaskStatusEnum {

  TASK_START_20102("20102", "任务开始"),
  TASK_START_50001("50001", "任务开始"),
  TASK_END_20103("20103", "任务结束"),
  TASK_END_50002("50002", "任务结束");

  private String code;
  private String status;

  TaskStatusEnum(String code, String status) {
    this.code = code;
    this.status = status;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
