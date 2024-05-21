package com.gs.cloud.warehouse.robot.entity.rule;

import java.util.Arrays;
import java.util.List;

public class IncidentCntRule extends Rule{

  private List<String> incidentCodeList;
  private String title;
  private Integer cnt;

  public IncidentCntRule(String code, String incidentCodes, String title, Integer cnt) {
    super(code);
    this.incidentCodeList = Arrays.asList(incidentCodes.split(",").clone());
    this.title = title;
    this.cnt = cnt;
  }

  public List<String> getIncidentCodeList() {
    return incidentCodeList;
  }

  public void setIncidentCodeList(List<String> incidentCodeList) {
    this.incidentCodeList = incidentCodeList;
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

  @Override
  public String getProcessor() {
    return "IncidentCntRule";
  }
}
