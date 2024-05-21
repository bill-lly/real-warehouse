package com.gs.cloud.warehouse.robot.enums;

//21---
public enum IncidentCntEnum {

  //异常停车
  ABNORMAL_PARKING("102001" ,"10064, 20136, 20063", "异常停车", 4),
  ABNORMAL_MACHINE_RESTART("102002", "10143, 10200", "整机异常重启", 2);

  private String code;
  private String incidentCode;
  private String title;
  private Integer cnt;

  IncidentCntEnum(String code, String incidentCodes, String title, Integer cnt) {
    this.code = code;
    this.incidentCode = incidentCodes;
    this.title = title;
    this.cnt = cnt;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getIncidentCode() {
    return incidentCode;
  }

  public void setIncidentCode(String incidentCode) {
    this.incidentCode = incidentCode;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Integer getCnt() {
    return cnt;
  }

  public void setCnt(Integer cnt) {
    this.cnt = cnt;
  }
}
