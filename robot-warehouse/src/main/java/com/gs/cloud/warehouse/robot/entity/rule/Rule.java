package com.gs.cloud.warehouse.robot.entity.rule;

public abstract class Rule {
  private String code;

  public Rule(String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  abstract public String getProcessor();
}
