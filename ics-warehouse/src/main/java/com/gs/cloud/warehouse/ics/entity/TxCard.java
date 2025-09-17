package com.gs.cloud.warehouse.ics.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TxCard extends FactEntity {
    //工牌sn
    @JsonProperty("card_sn")
    private String cardSn;
    //项目id
    @JsonProperty("item_id")
    private Long itemId;
    //项目名称
    @JsonProperty("item_name")
    private String itemName;
    //员工id
    @JsonProperty("crew_id")
    private Long crewId;
    //员工名称
    @JsonProperty("crew_name")
    private String crewName;
    //电池电量
    @JsonProperty("electricity")
    private Double electricity;
    //上报时间戳毫秒
    @JsonProperty("rep_timestamp_ms")
    private String repTimestampMs;
    //上报时间
    @JsonProperty("rep_time_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date repTimeT8;
    //beacon列表
    @JsonProperty("beacon_list")
    private String beaconList;
    //最强信号beaconcode
    @JsonProperty("beacon_code")
    private String beaconCode;
    //数据包序号
    @JsonProperty("packet_number")
    private Long packetNumber;
    //基站信号强度
    @JsonProperty("signal_strength")
    private Long signalStrength;
    //停止标志
    @JsonProperty("flag_stop")
    private Long flagStop;
    //云端时间(来自于kafka接收时间)
    @JsonProperty("cloud_recv_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date cloudRecvTime;
    //数据定义名称，流join操作用于区分数据来源于哪个kafka
    @JsonProperty("entity_name")
    private String entityName;

    //累计统计
    //前一笔电池电量信息
    private Double preElectricity;
    //工牌静止上报连续次数
    private Long stopCount;
    //工牌运动上报连续次数
    private Long moveCount;
    //工牌运动标志位
    private Boolean isMove;
    //记录工牌运动或静止时间,对应上面工牌运动标志位
    private Date stopOrMoveTime;
    //工牌上报beacon列表为空数量
    private Long beaconListNullCount;
    //工牌上报beacon列表不为空数量
    private Long beaconListNotNullCount;
    //工牌上报定位正常标志位
    private Boolean isLocNormal;

    private String crewIdStr;

    public String getCrewIdStr() { return crewIdStr; }

    public void setCrewIdStr(String crewIdStr) { this.crewIdStr = crewIdStr; }

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

    public Double getElectricity() { return electricity; }

    public void setElectricity(Double electricity) { this.electricity = electricity; }

    public String getRepTimestampMs() { return repTimestampMs; }

    public void setRepTimestampMs(String repTimestampMs) { this.repTimestampMs = repTimestampMs; }

    public Date getRepTimeT8() { return repTimeT8; }

    public void setRepTimeT8(Date repTimeT8) { this.repTimeT8 = repTimeT8; }

    public String getBeaconList() { return beaconList; }

    public void setBeaconList(String beaconList) { this.beaconList = beaconList; }

    public String getBeaconCode() { return beaconCode; }

    public void setBeaconCode(String beaconCode) { this.beaconCode = beaconCode; }

    public Long getPacketNumber() { return packetNumber; }

    public void setPacketNumber(Long packetNumber) { this.packetNumber = packetNumber; }

    public Long getSignalStrength() { return signalStrength; }

    public void setSignalStrength(Long signalStrength) { this.signalStrength = signalStrength; }

    public Long getFlagStop() { return flagStop; }

    public void setFlagStop(Long flagStop) { this.flagStop = flagStop; }

    public String getEntityName() { return entityName; }

    public void setEntityName(String entityName) { this.entityName = entityName; }

    public Double getPreElectricity() { return preElectricity; }

    public void setPreElectricity(Double preElectricity) { this.preElectricity = preElectricity; }

    public Long getStopCount() { return stopCount; }

    public void setStopCount(Long stopCount) { this.stopCount = stopCount; }

    public Long getMoveCount() { return moveCount; }

    public void setMoveCount(Long moveCount) { this.moveCount = moveCount; }

    public Long getBeaconListNullCount() { return beaconListNullCount; }

    public void setBeaconListNullCount(Long beaconListNullCount) { this.beaconListNullCount = beaconListNullCount; }

    public Long getBeaconListNotNullCount() { return beaconListNotNullCount; }

    public void setBeaconListNotNullCount(Long beaconListNotNullCount) { this.beaconListNotNullCount = beaconListNotNullCount; }

    public Date getCloudRecvTime() { return cloudRecvTime; }

    public void setCloudRecvTime(Date cloudRecvTime) { this.cloudRecvTime = cloudRecvTime; }

    public Boolean getMove() { return isMove; }

    public void setMove(Boolean move) { isMove = move; }

    public Date getStopOrMoveTime() { return stopOrMoveTime; }

    public void setStopOrMoveTime(Date stopOrMoveTime) { this.stopOrMoveTime = stopOrMoveTime; }

    public Boolean getLocNormal() { return isLocNormal; }

    public void setLocNormal(Boolean locNormal) { isLocNormal = locNormal; }

    @Override
    public CommonFormat<TxCard> getSerDeserializer() {
        return new CommonFormat<>(TxCard.class);
    }

    @Override
    public CommonKeySerialization<TxCard> getKeySerializer() {
        return new CommonKeySerialization<>();
    }

    @Override
    public String getKafkaServer() {
        return "kafka.bootstrap.servers.bigdata";
    }

    @Override
    public String getKafkaTopic() {
        return "kafka.topic.dwd.ics.tx.card";
    }

    @Override
    public String getKafkaGroupId() {
        return "kafka.group.id.dwd.ics.tx.card";
    }

    @Override
    public Date getEventTime() {
        return repTimeT8;
    }

    @Override
    public String getKey() { return getCardSn(); }

    @Override
    public int compareTo(BaseEntity o) {
        return this.getEventTime().compareTo(o.getEventTime());
    }

    @Override
    public String toString() {
        return "TxCard{" +
                "cardSn='" + cardSn + '\'' +
                ", itemId=" + itemId +
                ", itemName='" + itemName + '\'' +
                ", crewId=" + crewId +
                ", crewName='" + crewName + '\'' +
                ", electricity=" + electricity +
                ", repTimestampMs='" + repTimestampMs + '\'' +
                ", repTimeT8=" + repTimeT8 +
                ", beaconList='" + beaconList + '\'' +
                ", beaconCode='" + beaconCode + '\'' +
                ", packetNumber=" + packetNumber +
                ", signalStrength=" + signalStrength +
                ", flagStop=" + flagStop +
                ", cloudRecvTime=" + cloudRecvTime +
                ", entityName='" + entityName + '\'' +
                ", preElectricity=" + preElectricity +
                ", stopCount=" + stopCount +
                ", moveCount=" + moveCount +
                ", isMove=" + isMove +
                ", stopOrMoveTime=" + stopOrMoveTime +
                ", beaconListNullCount=" + beaconListNullCount +
                ", beaconListNotNullCount=" + beaconListNotNullCount +
                ", isLocNormal=" + isLocNormal +
                '}';
    }
}
