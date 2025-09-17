package com.gs.cloud.warehouse.ics.entity;

import java.util.Date;

public class TxCardTriggerIncident {
    private String cardSn;
    private Long itemId;
    private String itemName;
    private Long crewId;
    private String crewName;
    private Date cloudTime;
    private Date reportTime;
    private String triggerIncidentName;//触发事件名称
    private String triggerIncidentValue;//触发事件值
    private String trigIncidThresholdValue;//触发阈值
    private String trigIncidLastValue;//触发事件上一次值


    public String getCardSn() { return cardSn; }

    public void setCardSn(String cardSn) { this.cardSn = cardSn; }

    public Long getItemId() { return itemId; }

    public void setItemId(Long itemId) { this.itemId = itemId; }

    public String getItemName() { return itemName; }

    public void setItemName(String itemName) { this.itemName = itemName; }

    public Long getCrewId() { return crewId; }

    public void setCrewId(Long crewId) { this.crewId = crewId; }

    public String getCrewName() { return crewName; }

    public void setCrewName(String crewName) { this.crewName = crewName; }

    public Date getCloudTime() { return cloudTime; }

    public void setCloudTime(Date cloudTime) { this.cloudTime = cloudTime; }

    public Date getReportTime() { return reportTime; }

    public void setReportTime(Date reportTime) { this.reportTime = reportTime; }

    public String getTriggerIncidentName() { return triggerIncidentName; }

    public void setTriggerIncidentName(String triggerIncidentName) { this.triggerIncidentName = triggerIncidentName; }

    public String getTriggerIncidentValue() { return triggerIncidentValue; }

    public void setTriggerIncidentValue(String triggerIncidentValue) { this.triggerIncidentValue = triggerIncidentValue; }

    public String getTrigIncidThresholdValue() { return trigIncidThresholdValue; }

    public void setTrigIncidThresholdValue(String trigIncidThresholdValue) { this.trigIncidThresholdValue = trigIncidThresholdValue; }

    public String getTrigIncidLastValue() { return trigIncidLastValue; }

    public void setTrigIncidLastValue(String trigIncidLastValue) { this.trigIncidLastValue = trigIncidLastValue; }

    @Override
    public String toString() {
        return "TxCardTriggerIncident{" +
                "cardSn='" + cardSn + '\'' +
                ", itemId=" + itemId +
                ", itemName='" + itemName + '\'' +
                ", crewId=" + crewId +
                ", crewName='" + crewName + '\'' +
                ", cloudTime=" + cloudTime +
                ", reportTime=" + reportTime +
                ", triggerIncidentName='" + triggerIncidentName + '\'' +
                ", triggerIncidentValue='" + triggerIncidentValue + '\'' +
                ", trigIncidThresholdValue='" + trigIncidThresholdValue + '\'' +
                ", trigIncidLastValue='" + trigIncidLastValue + '\'' +
                '}';
    }
}
