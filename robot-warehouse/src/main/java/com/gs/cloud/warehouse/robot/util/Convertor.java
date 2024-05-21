package com.gs.cloud.warehouse.robot.util;

import com.gs.cloud.warehouse.robot.entity.FiredIncident;
import com.gs.cloud.warehouse.robot.entity.MonitorResult;
import com.gs.cloud.warehouse.robot.entity.RobotState;
import com.gs.cloud.warehouse.robot.entity.RobotWorkOnlineState;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Convertor {

  public static RobotWorkOnlineState robotState2workState(RobotState robotState) {
    RobotWorkOnlineState state = new RobotWorkOnlineState();
    state.setProductId(robotState.getProductId());
    state.setTimestampUtc(new Date(robotState.getLocalTimestamp()*1000));
    state.setCldTimestampUtc(robotState.getLastMqttOnlineUtc());
    state.setState(robotState.getWorkStatus());
    return state;
  }

  public static  RobotWorkOnlineState robotState2isOnline(RobotState robotState) {
    RobotWorkOnlineState state = new RobotWorkOnlineState();
    state.setProductId(robotState.getProductId());
    state.setState(robotState.getIsOnlineCode());
    Date timeStamp = robotState.getIsOnline() == 1 ? robotState.getLastOnlineUtc() : robotState.getLastOfflineUtc();
    state.setCldTimestampUtc(timeStamp);
    return state;
  }

  public static FiredIncident monitorResult2firedIncident(MonitorResult result) {
    FiredIncident incident = new FiredIncident();
    incident.setEventId(UUID.randomUUID().toString());
    incident.setIncidentCode(result.getCode());
    incident.setEntityEventTime(result.getTriggerTimestamp());
    incident.setIncidentStartTime(result.getTriggerTimestamp());
    incident.setFinalized(false);
    incident.setCleanType("CLEAN_TYPE_UNSPECIFIED");
    incident.setSubjectId(result.getProductId());
    incident.setSubjectType("BOT");
    incident.setSubjectModel(result.getModelTypeCode());
    Map<String, String> metas = new HashMap<>();
    Map<String, String> bigdataMonitor = new HashMap<>();
    Map<String, Map<String, String>> ext = new HashMap<>();
    metas.put("alias", result.getAlias());
    metas.put("status", "11");
    metas.put("softwareVersion", result.getSoftwareVersion());
    metas.put("contractedCustomer", result.getCustomerCode());
    metas.put("maintainedRegion", result.getMaintenanceRegionCode());
    bigdataMonitor.put("description", result.getMsg());
    ext.put("bigdataMonitor", bigdataMonitor);
    incident.setMetas(metas);
    incident.setExt(ext);
    return incident;
  }
}
