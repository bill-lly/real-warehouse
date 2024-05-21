package com.gs.cloud.warehouse.robot.entity;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleResult {

  @JsonProperty("code")
  private String code;
  @JsonProperty("hasError")
  private boolean hasError;
  @JsonProperty("msg")
  private String msg;

  public RuleResult(String code, boolean hasError, String msg) {
    this.code = code;
    this.hasError = hasError;
    this.msg = msg;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public boolean isHasError() {
    return hasError;
  }

  public void setHasError(boolean hasError) {
    this.hasError = hasError;
  }

  public static RuleResult create(String code) {
    return new RuleResult(code, false, null);
  }

  public static RuleResult createError(String code, String msg) {
    return new RuleResult(code, true, msg);
  }

  @Override
  public String toString() {
    return "RuleResult{" +
        "code='" + code + '\'' +
        ", hasError=" + hasError +
        ", msg='" + msg + '\'' +
        '}';
  }
}
