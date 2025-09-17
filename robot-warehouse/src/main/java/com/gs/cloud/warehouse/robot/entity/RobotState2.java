package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RobotState2 extends FactEntity {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("create_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonProperty("update_time")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date updateTime;

    @JsonProperty("local_timestamp")
    private Long localTimestamp;

    @JsonProperty("local_timestamp_t")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date localTimestampT;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("cloud_status_version")
    private String cloudStatusVersion;

    @JsonProperty("cloud_status_key")
    private String cloudStatusKey;

    @JsonProperty("cloud_status_value")
    private String cloudStatusValue;

    @JsonProperty("cloud_status_key_cn")
    private String cloudStatusKeyCn;

    @JsonProperty("cloud_status_value_cn")
    private String cloudStatusValueCn;

    @JsonProperty("group_name")
    private String groupName;

    @JsonProperty("terminal_user_name")
    private String terminalUserName;

    @JsonProperty("customer_grade")
    private String customerGrade;

    @JsonProperty("robot_family_code")
    private String robotFamilyCode;

    @JsonProperty("business_area")
    private String businessArea;

    @JsonProperty("entity_name")
    private String entityName;

    @Override
    public CommonFormat<RobotState2> getSerDeserializer() {
        return new CommonFormat<>(RobotState2.class);
    }

    @Override
    public CommonKeySerialization<RobotState2> getKeySerializer() {
        return new CommonKeySerialization<>();
    }

    @Override
    public String getKafkaServer() {
        return "kafka.bootstrap.servers.bigdata";
    }

    @Override
    public String getKafkaTopic() {
        return "kafka.topic.dwd.maint.robotState2dot0";
    }

    @Override
    public String getKafkaGroupId() {
        return "kafka.group.id.dwd.maint.robotState2dot0";
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Long getLocalTimestamp() {
        return localTimestamp;
    }

    public void setLocalTimestamp(Long localTimestamp) {
        this.localTimestamp = localTimestamp;
    }

    public Date getLocalTimestampT() {
        return localTimestampT;
    }

    public void setLocalTimestampT(Date localTimestampT) {
        this.localTimestampT = localTimestampT;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCloudStatusVersion() {
        return cloudStatusVersion;
    }

    public void setCloudStatusVersion(String cloudStatusVersion) {
        this.cloudStatusVersion = cloudStatusVersion;
    }

    public String getCloudStatusKey() {
        return cloudStatusKey;
    }

    public void setCloudStatusKey(String cloudStatusKey) {
        this.cloudStatusKey = cloudStatusKey;
    }

    public String getCloudStatusValue() {
        return cloudStatusValue;
    }

    public void setCloudStatusValue(String cloudStatusValue) {
        this.cloudStatusValue = cloudStatusValue;
    }

    public String getCloudStatusKeyCn() {
        return cloudStatusKeyCn;
    }

    public void setCloudStatusKeyCn(String cloudStatusKeyCn) {
        this.cloudStatusKeyCn = cloudStatusKeyCn;
    }

    public String getCloudStatusValueCn() {
        return cloudStatusValueCn;
    }

    public void setCloudStatusValueCn(String cloudStatusValueCn) {
        this.cloudStatusValueCn = cloudStatusValueCn;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTerminalUserName() {
        return terminalUserName;
    }

    public void setTerminalUserName(String terminalUserName) {
        this.terminalUserName = terminalUserName;
    }

    public String getCustomerGrade() {
        return customerGrade;
    }

    public void setCustomerGrade(String customerGrade) {
        this.customerGrade = customerGrade;
    }

    public String getRobotFamilyCode() {
        return robotFamilyCode;
    }

    public void setRobotFamilyCode(String robotFamilyCode) {
        this.robotFamilyCode = robotFamilyCode;
    }

    public String getBusinessArea() {
        return businessArea;
    }

    public void setBusinessArea(String businessArea) {
        this.businessArea = businessArea;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    @Override
    public Date getEventTime() {
        return getUpdateTime();
    }

    @Override
    public String getKey() {
        return getProductId();
    }

    public String getProductId() {
        return productId;
    }

    @Override
    public int compareTo(BaseEntity o) {
        return this.getEventTime().compareTo(o.getEventTime());
    }

    @Override
    public String toString() {
        return "RobotState2{" +
                "id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", localTimestamp=" + localTimestamp +
                ", localTimestampT=" + localTimestampT +
                ", productId='" + productId + '\'' +
                ", cloudStatusVersion='" + cloudStatusVersion + '\'' +
                ", cloudStatusKey='" + cloudStatusKey + '\'' +
                ", cloudStatusValue='" + cloudStatusValue + '\'' +
                ", cloudStatusKeyCn='" + cloudStatusKeyCn + '\'' +
                ", cloudStatusValueCn='" + cloudStatusValueCn + '\'' +
                ", groupName='" + groupName + '\'' +
                ", terminalUserName='" + terminalUserName + '\'' +
                ", customerGrade='" + customerGrade + '\'' +
                ", robotFamilyCode='" + robotFamilyCode + '\'' +
                ", businessArea='" + businessArea + '\'' +
                ", entityName='" + entityName + '\'' +
                '}';
    }
}
