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
public class Incident extends FactEntity {
    //消息id
    @JsonProperty("msg_id")
    private String msgId;
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
    //工牌sn
    @JsonProperty("card_sn")
    private String cardSn;
    //事件id
    @JsonProperty("incid_id")
    private String incidId;
    //事件触发时间戳毫秒
    @JsonProperty("incid_timestamp_ms")
    private String incidTimestampMs;
    //事件触发时间
    @JsonProperty("incid_time_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date incidTimeT8;
    //据定义名称，流join操作用于区分数据来源于哪个kafka
    @JsonProperty("entity_name")
    private String entityName;

    public String getMsgId() { return msgId; }

    public void setMsgId(String msgId) { this.msgId = msgId; }

    public Long getItemId() { return itemId; }

    public void setItemId(Long itemId) { this.itemId = itemId; }

    public Long getCrewId() { return crewId; }

    public void setCrewId(Long crewId) { this.crewId = crewId; }

    public String getIncidId() { return incidId; }

    public void setIncidId(String incidId) { this.incidId = incidId; }

    public String getIncidTimestampMs() { return incidTimestampMs; }

    public void setIncidTimestampMs(String incidTimestampMs) { this.incidTimestampMs = incidTimestampMs; }

    public Date getIncidTimeT8() { return incidTimeT8; }

    public void setIncidTimeT8(Date incidTimeT8) { this.incidTimeT8 = incidTimeT8; }

    public String getEntityName() { return entityName; }

    public void setEntityName(String entityName) { this.entityName = entityName; }

    public String getItemName() { return itemName; }

    public void setItemName(String itemName) { this.itemName = itemName; }

    public String getCrewName() { return crewName; }

    public void setCrewName(String crewName) { this.crewName = crewName; }

    public String getCardSn() { return cardSn; }

    public void setCardSn(String cardSn) { this.cardSn = cardSn; }

    @Override
    public CommonFormat getSerDeserializer() { return new CommonFormat<>(Incident.class); }

    @Override
    public CommonKeySerialization getKeySerializer() {
        return new CommonKeySerialization<>();
    }

    @Override
    public String getKafkaServer() { return "kafka.bootstrap.servers.bigdata"; }

    @Override
    public String getKafkaTopic() { return "kafka.topic.dwd.ics.incident"; }

    @Override
    public String getKafkaGroupId() { return "kafka.group.id.dwd.ics.incident"; }

    @Override
    public Date getEventTime() { return incidTimeT8; }

    @Override
    public String getKey() { return getCardSn(); }

    @Override
    public int compareTo(BaseEntity o) { return this.getEventTime().compareTo(o.getEventTime()); }

    @Override
    public String toString() {
        return "Incident{" +
                "msgId='" + msgId + '\'' +
                ", itemId=" + itemId +
                ", crewId=" + crewId +
                ", incidId='" + incidId + '\'' +
                ", incidTimestampMs='" + incidTimestampMs + '\'' +
                ", incidTimeT8=" + incidTimeT8 +
                ", entityName='" + entityName + '\'' +
                '}';
    }
}
