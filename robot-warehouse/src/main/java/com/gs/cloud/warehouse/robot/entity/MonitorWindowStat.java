package com.gs.cloud.warehouse.robot.entity;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class MonitorWindowStat {

  private String productId;

  private boolean doMonitor;

  private Date cldTimestamp;

  private Date startTimestamp;

  private Long lastRobotUnixTimestamp;

  private Long lastCheckUnixTimestamp;

  private Date endTimestamp;

  private Long avgOffset;

  private List<RobotWorkOnlineState> workStates;

  private Map<String, Long> durationMap;

  private Map<String, Long> maxDurationMap;

  private TreeMap<Long, Double> displacementMap;

  private Map<String, Integer> incidentMap;

  private Tuple2<Long, String> taskStatus;

  private Integer incidentCnt;

  private Map<String, Integer> codeIncidentCnt;

  private Double totalDisplacement;

  private List<RuleResult> ruleResultList = new ArrayList<>();

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public void setStartTimestamps(List<Long> startUnixTimestamps) {
    if (startUnixTimestamps != null && !startUnixTimestamps.isEmpty()) {
     this.startTimestamp = new Date(startUnixTimestamps.get(0));
    }
  }

  public Date getCldTimestamp() {
    return cldTimestamp;
  }

  public void setCldTimestamp(Date cldTimestamp) {
    this.cldTimestamp = cldTimestamp;
  }

  public Integer getIncidentCnt() {
    return incidentCnt;
  }

  public void setIncidentCnt(Integer incidentCnt) {
    this.incidentCnt = incidentCnt;
  }

  public Map<String, Integer> getIncidentMap() {
    return incidentMap;
  }

  public void setIncidentMap(Map<String, Integer> incidentMap) {
    this.incidentMap = incidentMap;
  }

  public TreeMap<Long, Double> getDisplacementMap() {
    return displacementMap;
  }

  public void setDisplacementMap(TreeMap<Long, Double> displacementMap) {
    this.displacementMap = displacementMap;
  }

  public Map<String, Integer> getCodeIncidentCnt() {
    return codeIncidentCnt;
  }

  public void setCodeIncidentCnt(Map<String, Integer> codeIncidentCnt) {
    this.codeIncidentCnt = codeIncidentCnt;
  }

  public boolean isDoMonitor() {
    return doMonitor;
  }

  public void setDoMonitor(boolean doMonitor) {
    this.doMonitor = doMonitor;
  }

  public Long getLastRobotUnixTimestamp() {
    return lastRobotUnixTimestamp;
  }

  public void setLastRobotUnixTimestamp(Long lastRobotUnixTimestamp) {
    this.lastRobotUnixTimestamp = lastRobotUnixTimestamp;
  }

  public Long getAvgOffset() {
    return avgOffset;
  }

  public void setAvgOffset(Long avgOffset) {
    this.avgOffset = avgOffset;
  }

  public List<RobotWorkOnlineState> getWorkStates() {
    return workStates;
  }

  public void setWorkStates(List<RobotWorkOnlineState> workStates) {
    this.workStates = workStates;
  }

  public Map<String, Long> getMaxDurationMap() {
    return maxDurationMap;
  }

  public void setMaxDurationMap(Map<String, Long> maxDurationMap) {
    this.maxDurationMap = maxDurationMap;
  }

  public Date getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(Date startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public Date getEndTimestamp() {
    return endTimestamp;
  }

  public void setEndTimestamp(Date endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  public Double getTotalDisplacement() {
    return totalDisplacement;
  }

  public void setTotalDisplacement(Double totalDisplacement) {
    this.totalDisplacement = totalDisplacement;
  }

  public Long getLastCheckUnixTimestamp() {
    return lastCheckUnixTimestamp;
  }

  public void setLastCheckUnixTimestamp(Long lastCheckUnixTimestamp) {
    this.lastCheckUnixTimestamp = lastCheckUnixTimestamp;
  }

  public Tuple2<Long, String> getTaskStatus() {
    return taskStatus;
  }

  public void setTaskStatus(Tuple2<Long, String> taskStatus) {
    this.taskStatus = taskStatus;
  }

  public List<RuleResult> getRuleResultList() {
    return ruleResultList;
  }

  public void setRuleResultList(List<RuleResult> ruleResultList) {
    this.ruleResultList = ruleResultList;
  }

  public Map<String, Long> getDurationMap() {
    return durationMap;
  }

  public void setDurationMap(Map<String, Long> durationMap) {
    this.durationMap = durationMap;
  }

  public Tuple2<Long, String> getCurrentWorkState() {
    if (workStates.isEmpty()) {
      return null;
    }
    RobotWorkOnlineState last = workStates.get(workStates.size() - 1);
    if (last.isWorkState()) {
      return Tuple2.of(last.getTimestampUtc().getTime(), last.getState());
    } else {
      return Tuple2.of(last.getCldTimestampUtc().getTime(), last.getState());
    }
  }

  public static MonitorWindowStat createUndoMonitor(String productId) {
    MonitorWindowStat stat = new MonitorWindowStat();
    stat.setProductId(productId);
    stat.setDoMonitor(false);
    return stat;
  }

  public static MonitorWindowStat createDoMonitor(String productId,
                                                  List<Long> monitorStartTimes,
                                                  Date cldTimestamp,
                                                  Long lastRobotTime,
                                                  Long lastCheckTime,
                                                  Long avgOffset) {
    MonitorWindowStat stat = new MonitorWindowStat();
    stat.setProductId(productId);
    stat.setDoMonitor(true);
    stat.setCldTimestamp(cldTimestamp);
    stat.setStartTimestamps(monitorStartTimes);
    stat.setLastRobotUnixTimestamp(lastRobotTime);
    stat.setLastCheckUnixTimestamp(lastCheckTime);
    stat.setAvgOffset(avgOffset);
    return stat;
  }

  @Override
  public String toString() {
    return "MonitorWindowStat{" +
        "productId='" + productId + '\'' +
        ", startTimestamp=" + startTimestamp +
        ", endTimestamp=" + endTimestamp +
        ", avgOffset=" + avgOffset +
        ", maxDurationMap=" + maxDurationMap +
        ", displacementMap=" + displacementMap +
        ", incidentCnt=" + incidentCnt +
        ", codeIncidentCnt=" + codeIncidentCnt +
        ", totalDisplacement=" + totalDisplacement +
        ", ruleResultList=" + ruleResultList +
        '}';
  }
}
