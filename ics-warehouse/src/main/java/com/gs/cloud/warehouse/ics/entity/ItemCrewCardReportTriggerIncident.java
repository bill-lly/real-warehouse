package com.gs.cloud.warehouse.ics.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;

public class ItemCrewCardReportTriggerIncident extends FactEntity {
    @JsonProperty("card_sn")
    private String cardSn;

    @JsonProperty("item_id")
    private Long itemId;

    @JsonProperty("item_name")
    private String itemName;

    @JsonProperty("crew_id")
    private Long crewId;

    @JsonProperty("crew_name")
    private String crewName;

    @JsonProperty("cloud_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date cloudTime;

    @JsonProperty("report_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date reportTime;

    @JsonProperty("trig_incid_name")
    private String triggerIncidentName;//触发事件名称

    @JsonProperty("trig_incid_value")
    private String triggerIncidentValue;//触发事件值

    @JsonProperty("trig_incid_thsd_value")
    private String trigIncidThresholdValue;//触发阈值

    @JsonProperty("trig_incid_last_value")
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

    public String getTrigIncidLastValue() { return trigIncidLastValue;}

    public void setTrigIncidLastValue(String trigIncidLastValue) { this.trigIncidLastValue = trigIncidLastValue; }

    @Override
    public String toString() {
        return "ItemCrewCardReportTriggerIncident{" +
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

    @Override
    public CommonFormat getSerDeserializer() { return new CommonFormat<>(ItemCrewCardReportTriggerIncident.class); }

    @Override
    public CommonKeySerialization getKeySerializer() { return new CommonKeySerialization<>(); }

    @Override
    public String getKafkaServer() { return "kafka.bootstrap.servers.bigdata"; }

    @Override
    public String getKafkaTopic() { return "kafka.topic.dws.ics.item.crew.card.rep.trig.incid"; }

    @Override
    public String getKafkaGroupId() { return "kafka.group.id.dws.ics.item.crew.card.rep.trig.incid"; }

    @Override
    public Date getEventTime() { return getReportTime(); }

    @Override
    public String getKey() { return getCardSn(); }

    @Override
    public int compareTo(BaseEntity o) { return this.getEventTime().compareTo(o.getEventTime()); }
}
