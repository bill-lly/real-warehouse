package com.gs.cloud.warehouse.ics.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class ItemCrewCardDetail extends FactEntity {
    @JsonProperty("item_id")
    private Long itemId;

    @JsonProperty("item_name")
    private String itemName;

    @JsonProperty("crew_id")
    private Long crewId;

    @JsonProperty("crew_name")
    private String crewName;

    @JsonProperty("card_sn")
    private String cardSn;

    @JsonProperty("position_ids")
    private String positionIdList;

    @JsonProperty("position_names")
    private String positionNameList;

    @JsonProperty("space_code")
    private String spaceCode;

    @JsonProperty("space_name")
    private String spaceName;

    @JsonProperty("beacon_code")
    private String beaconCode;

    @JsonProperty("trigger_incident_name")
    private String triggerIncidentName;

    @JsonProperty("electricity")
    private Double electricity;

    @JsonProperty("signal_strength")
    private Long signalStrength;

    @JsonProperty("cloud_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date cloudTime;

    @JsonProperty("report_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date reportTime;

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public Long getCrewId() {
        return crewId;
    }

    public void setCrewId(Long crewId) {
        this.crewId = crewId;
    }

    public String getCrewName() {
        return crewName;
    }

    public void setCrewName(String crewName) {
        this.crewName = crewName;
    }

    public String getCardSn() {
        return cardSn;
    }

    public void setCardSn(String cardSn) {
        this.cardSn = cardSn;
    }

    public String getPositionIdList() {
        return positionIdList;
    }

    public void setPositionIdList(String positionIdList) {
        this.positionIdList = positionIdList;
    }

    public String getPositionNameList() {
        return positionNameList;
    }

    public void setPositionNameList(String positionNameList) {
        this.positionNameList = positionNameList;
    }

    public String getSpaceCode() {
        return spaceCode;
    }

    public void setSpaceCode(String spaceCode) {
        this.spaceCode = spaceCode;
    }

    public String getSpaceName() {
        return spaceName;
    }

    public void setSpaceName(String spaceName) {
        this.spaceName = spaceName;
    }

    public String getBeaconCode() {
        return beaconCode;
    }

    public void setBeaconCode(String beaconCode) {
        this.beaconCode = beaconCode;
    }

    public String getTriggerIncidentName() {
        return triggerIncidentName;
    }

    public void setTriggerIncidentName(String triggerIncidentName) {
        this.triggerIncidentName = triggerIncidentName;
    }

    public Double getElectricity() {
        return electricity;
    }

    public void setElectricity(Double electricity) {
        this.electricity = electricity;
    }

    public Long getSignalStrength() {
        return signalStrength;
    }

    public void setSignalStrength(Long signalStrength) {
        this.signalStrength = signalStrength;
    }

    public Date getCloudTime() {
        return cloudTime;
    }

    public void setCloudTime(Date cloudTime) {
        this.cloudTime = cloudTime;
    }

    public Date getReportTime() {
        return reportTime;
    }

    public void setReportTime(Date reportTime) {
        this.reportTime = reportTime;
    }

    @Override
    public String toString() {
        return "ItemCrewCardDetail{" +
                "itemId=" + itemId +
                ", itemName='" + itemName + '\'' +
                ", crewId=" + crewId +
                ", crewName='" + crewName + '\'' +
                ", cardSn='" + cardSn + '\'' +
                ", positionIdList='" + positionIdList + '\'' +
                ", positionNameList='" + positionNameList + '\'' +
                ", spaceCode='" + spaceCode + '\'' +
                ", spaceName='" + spaceName + '\'' +
                ", beaconCode='" + beaconCode + '\'' +
                ", triggerIncidentName='" + triggerIncidentName + '\'' +
                ", electricity=" + electricity +
                ", signalStrength=" + signalStrength +
                ", cloudTime=" + cloudTime +
                ", reportTime=" + reportTime +
                '}';
    }

    @Override
    public CommonFormat getSerDeserializer() { return new CommonFormat<>(ItemCrewCardDetail.class); }

    @Override
    public CommonKeySerialization getKeySerializer() { return new CommonKeySerialization<>(); }

    @Override
    public String getKafkaServer() {
        return "kafka.bootstrap.servers.bigdata";
    }

    @Override
    public String getKafkaTopic() { return "kafka.topic.dws.ics.item.crew.card.detail"; }

    @Override
    public String getKafkaGroupId() {
        return null;
    }

    @Override
    public Date getEventTime() {
        return getReportTime();
    }

    @Override
    public String getKey() {
        return getCardSn();
    }

    @Override
    public int compareTo(BaseEntity o) { return this.getEventTime().compareTo(o.getEventTime()); }
}
