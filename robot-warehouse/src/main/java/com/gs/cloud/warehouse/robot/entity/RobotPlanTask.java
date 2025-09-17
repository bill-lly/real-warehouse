package com.gs.cloud.warehouse.robot.entity;


import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

public class RobotPlanTask extends FactEntity {
    private Long id;
    private String productId;
    private String PlanTaskName;
    private String TimeZone;
    private Long planTaskStartTimeMS;
    private Date planTaskStartTimeUTC;
    private Date planTaskStartTimeLocal;
    private Date cldTimestampUTC;
    private String taskInfo;

    public String getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(String taskInfo) {
        this.taskInfo = taskInfo;
    }

    public Date getCldTimestampUTC() {
        return cldTimestampUTC;
    }

    public void setCldTimestampUTC(Date cldTimestampUTC) {
        this.cldTimestampUTC = cldTimestampUTC;
    }

    private Date eventTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getPlanTaskStartTimeUTC() {
        return planTaskStartTimeUTC;
    }
    public void setPlanTaskStartTimeUTC(Date planTaskStartTimeUTC) {
        this.planTaskStartTimeUTC = planTaskStartTimeUTC;
    }

    public Long getPlanTaskStartTimeMS() {
        return planTaskStartTimeMS;
    }

    public void setPlanTaskStartTimeMS(Long planTaskStartTimeMS) {
        this.planTaskStartTimeMS = planTaskStartTimeMS;
    }

    public Date getPlanTaskStartTimeLocal() {
        return planTaskStartTimeLocal;
    }

    public void setPlanTaskStartTimeLocal(Date planTaskStartTimeLocal) {
        this.planTaskStartTimeLocal = planTaskStartTimeLocal;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }
    public String getKey() {
        return getProductId();
    }

    public String getPlanTaskName() {
        return PlanTaskName;
    }

    public void setPlanTaskName(String planTaskName) {
        PlanTaskName = planTaskName;
    }

    public String getTimeZone() {
        return TimeZone;
    }

    public void setTimeZone(String timeZone) {
        TimeZone = timeZone;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public Date getEventTime() {
        return eventTime;
    }

    @Override
    public CommonFormat getSerDeserializer() {
        return null;
    }

    @Override
    public CommonKeySerialization getKeySerializer() {
        return null;
    }

    @Override
    public String getKafkaServer() {
        return null;
    }

    @Override
    public String getKafkaTopic() {
        return null;
    }

    @Override
    public String getKafkaGroupId() {
        return null;
    }

    @Override
    public int compareTo(@NotNull BaseEntity o) {
        return this.planTaskStartTimeUTC.compareTo(o.getEventTime());
    }


    @Override
    public String toString() {
        return "RobotPlanTask{" +
                "productId='" + productId + '\'' +
                ", PlanTaskName='" + PlanTaskName + '\'' +
                ", TimeZone='" + TimeZone + '\'' +
                ", planTaskStartTimeMS=" + planTaskStartTimeMS +
                ", planTaskStartTimeUTC=" + planTaskStartTimeUTC +
                ", planTaskStartTimeLocal=" + planTaskStartTimeLocal +
                ", eventTime=" + eventTime +
                '}';
    }
}
