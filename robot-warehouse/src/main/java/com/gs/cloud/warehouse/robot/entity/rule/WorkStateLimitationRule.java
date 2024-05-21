package com.gs.cloud.warehouse.robot.entity.rule;

public class WorkStateLimitationRule extends Rule{

  private String workStateCode;

  private String workState;

  private Integer threshold;

  public WorkStateLimitationRule(String code, String workStateCode, String workState, Integer threshold) {
    super(code);
    this.workStateCode = workStateCode;
    this.workState = workState;
    this.threshold = threshold;
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

  public Integer getThreshold() {
    return threshold;
  }

  public void setThreshold(Integer threshold) {
    this.threshold = threshold;
  }

  @Override
  public String getProcessor() {
    return "WorkStateLimitationRule";
  }
}
