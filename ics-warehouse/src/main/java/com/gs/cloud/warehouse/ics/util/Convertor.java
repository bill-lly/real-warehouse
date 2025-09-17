package com.gs.cloud.warehouse.ics.util;

import com.gs.cloud.warehouse.ics.entity.ItemCrewCardReportTriggerIncident;
import com.gs.cloud.warehouse.ics.entity.TxCardTriggerIncident;

public class Convertor {
  public static ItemCrewCardReportTriggerIncident trigIncidResult2ItemCrewCardRepTrigIncid(TxCardTriggerIncident result) {
    ItemCrewCardReportTriggerIncident incident = new ItemCrewCardReportTriggerIncident();
    incident.setCardSn(result.getCardSn());
    incident.setItemId(result.getItemId());
    incident.setItemName(result.getItemName());
    incident.setCrewId(result.getCrewId());
    incident.setCrewName(result.getCrewName());
    incident.setCloudTime(result.getCloudTime());
    incident.setReportTime(result.getReportTime());
    incident.setTriggerIncidentName(result.getTriggerIncidentName());
    incident.setTriggerIncidentValue(result.getTriggerIncidentValue());
    incident.setTrigIncidThresholdValue(result.getTrigIncidThresholdValue());
    incident.setTrigIncidLastValue(result.getTrigIncidLastValue());
    return incident;
  }
}
