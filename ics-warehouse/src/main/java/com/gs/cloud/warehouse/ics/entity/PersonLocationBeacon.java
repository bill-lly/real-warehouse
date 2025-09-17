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
public class PersonLocationBeacon extends FactEntity {
    //请求id
    @JsonProperty("request_id")
    private String requestId;
    //人员id
    @JsonProperty("person_id")
    private String personId;
    //人员姓名
    @JsonProperty("person_name")
    private String personName;
    //上报时间戳
    @JsonProperty("report_timestamp")
    private String reportTimestamp;
    //上报时间
    @JsonProperty("report_timestamp_t")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date reportTimestampT;
    //云端时间(来自于kafka接收时间)
    @JsonProperty("cloud_recv_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date cloudRecvTime;
    //空间code
    @JsonProperty("space_code")
    private String spaceCode;
    //空间名称
    @JsonProperty("space_name")
    private String spaceName;
    //保洁员id
    @JsonProperty("crew_id")
    private String crewId;
    //项目id
    @JsonProperty("item_id")
    private String itemId;
    //自定义属性
    private Long sliceTimestamp;//切片开始时间戳

    public String getRequestId() { return requestId; }

    public void setRequestId(String requestId) { this.requestId = requestId; }

    public String getPersonId() { return personId; }

    public void setPersonId(String personId) { this.personId = personId; }

    public String getPersonName() { return personName; }

    public void setPersonName(String personName) { this.personName = personName; }

    public String getReportTimestamp() { return reportTimestamp; }

    public void setReportTimestamp(String reportTimestamp) { this.reportTimestamp = reportTimestamp; }

    public Date getReportTimestampT() { return reportTimestampT; }

    public void setReportTimestampT(Date reportTimestampT) { this.reportTimestampT = reportTimestampT; }

    public Date getCloudRecvTime() { return cloudRecvTime; }

    public void setCloudRecvTime(Date cloudRecvTime) { this.cloudRecvTime = cloudRecvTime; }

    public String getSpaceCode() { return spaceCode; }

    public void setSpaceCode(String spaceCode) { this.spaceCode = spaceCode; }

    public String getSpaceName() { return spaceName; }

    public void setSpaceName(String spaceName) { this.spaceName = spaceName; }

    public String getCrewId() { return crewId; }

    public void setCrewId(String crewId) { this.crewId = crewId; }

    public String getItemId() { return itemId; }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Long getSliceTimestamp() { return sliceTimestamp; }

    public void setSliceTimestamp(Long sliceTimestamp) { this.sliceTimestamp = sliceTimestamp; }

    @Override
    public CommonFormat<PersonLocationBeacon> getSerDeserializer() { return new CommonFormat<>(PersonLocationBeacon.class); }

    @Override
    public CommonKeySerialization<PersonLocationBeacon> getKeySerializer() { return new CommonKeySerialization<>(); }

    @Override
    public String getKafkaServer() { return "kafka.bootstrap.servers.bigdata"; }

    @Override
    public String getKafkaTopic() { return "kafka.topic.dwd.ics.person.location.beacon"; }

    @Override
    public String getKafkaGroupId() { return "kafka.group.id.dwd.ics.person.location.beacon"; }

    @Override
    public String toString() {
        return "PersonLocationBeacon{" +
                "requestId='" + requestId + '\'' +
                ", personId='" + personId + '\'' +
                ", personName='" + personName + '\'' +
                ", reportTimestamp='" + reportTimestamp + '\'' +
                ", reportTimestampT=" + reportTimestampT +
                ", cloudRecvTime=" + cloudRecvTime +
                ", spaceCode='" + spaceCode + '\'' +
                ", spaceName='" + spaceName + '\'' +
                ", crewId='" + crewId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", sliceTimestamp=" + sliceTimestamp +
                '}';
    }

    @Override
    public Date getEventTime() { return getReportTimestampT(); }

    @Override
    public String getKey() { return getRequestId(); }

    @Override
    public int compareTo(BaseEntity o) { return this.getEventTime().compareTo(o.getEventTime()); }
}
