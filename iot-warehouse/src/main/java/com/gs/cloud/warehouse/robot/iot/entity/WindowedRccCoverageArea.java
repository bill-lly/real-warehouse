package com.gs.cloud.warehouse.robot.iot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class WindowedRccCoverageArea extends FactEntity {

    @JsonProperty("productId")
    private String productId;

    @JsonProperty("windowStartTimeUtc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date windowStartTimeUtc;

    @JsonProperty("windowEndTimeUtc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date windowEndTimeUtc;

    @JsonProperty("cldStartTimeUtc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date cldStartTimeUtc;

    @JsonProperty("cldEndTimeUtc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date cldEndTimeUtc;

    @JsonProperty("startTimeUtc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date startTimeUtc;

    @JsonProperty("endTimeUtc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date endTimeUtc;

    @JsonProperty("firstCoverageArea")
    private Double firstCoverageArea;

    @JsonProperty("lastCoverageArea")
    private Double lastCoverageArea;

    @JsonProperty("increasedCoverageArea")
    private Double increasedCoverageArea;

    @JsonProperty("runningDuration")
    private int runningDuration;

    @JsonProperty("robotFamilyCode")
    private String robotFamilyCode;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Date getWindowStartTimeUtc() {
        return windowStartTimeUtc;
    }

    public void setWindowStartTimeUtc(Date windowStartTimeUtc) {
        this.windowStartTimeUtc = windowStartTimeUtc;
    }

    public Date getWindowEndTimeUtc() {
        return windowEndTimeUtc;
    }

    public void setWindowEndTimeUtc(Date windowEndTimeUtc) {
        this.windowEndTimeUtc = windowEndTimeUtc;
    }

    public Date getCldStartTimeUtc() {
        return cldStartTimeUtc;
    }

    public void setCldStartTimeUtc(Date cldStartTimeUtc) {
        this.cldStartTimeUtc = cldStartTimeUtc;
    }

    public Date getCldEndTimeUtc() {
        return cldEndTimeUtc;
    }

    public void setCldEndTimeUtc(Date cldEndTimeUtc) {
        this.cldEndTimeUtc = cldEndTimeUtc;
    }

    public Date getStartTimeUtc() {
        return startTimeUtc;
    }

    public void setStartTimeUtc(Date startTimeUtc) {
        this.startTimeUtc = startTimeUtc;
    }

    public Date getEndTimeUtc() {
        return endTimeUtc;
    }

    public void setEndTimeUtc(Date endTimeUtc) {
        this.endTimeUtc = endTimeUtc;
    }

    public Double getFirstCoverageArea() {
        return firstCoverageArea;
    }

    public void setFirstCoverageArea(Double firstCoverageArea) {
        this.firstCoverageArea = firstCoverageArea;
    }

    public Double getLastCoverageArea() {
        return lastCoverageArea;
    }

    public void setLastCoverageArea(Double lastCoverageArea) {
        this.lastCoverageArea = lastCoverageArea;
    }

    public Double getIncreasedCoverageArea() {
        return increasedCoverageArea;
    }

    public void setIncreasedCoverageArea(Double increasedCoverageArea) {
        this.increasedCoverageArea = increasedCoverageArea;
    }

    public int getRunningDuration() {
        return runningDuration;
    }

    public void setRobotFamilyCode(String robotFamilyCode) {
        this.robotFamilyCode = robotFamilyCode;
    }
    public String getRobotFamilyCode() {
        return robotFamilyCode;
    }

    public void setRunningDuration(int runningDuration) {
        this.runningDuration = runningDuration;
    }

    @Override
    public CommonFormat<WindowedRccCoverageArea> getSerDeserializer() {
        return new CommonFormat<>(WindowedRccCoverageArea.class);
    }

    @Override
    public CommonKeySerialization<WindowedRccCoverageArea> getKeySerializer() {
        return new CommonKeySerialization<>();
    }

    @Override
    public String getKafkaServer() {
        return "";
    }

    @Override
    public String getKafkaTopic() {
        return "";
    }

    @Override
    public String getKafkaGroupId() {
        return "";
    }

    @Override
    public Date getEventTime() {
        return null;
    }

    @Override
    public String getKey() {
        return "";
    }

    @Override
    public int compareTo(BaseEntity o) {
        return 0;
    }


    @Override
    public String toString() {
        return "WindowedRccCoverageArea{" +
                "productId='" + productId + '\'' +
                ", windowStartTimeUtc=" + windowStartTimeUtc +
                ", windowEndTimeUtc=" + windowEndTimeUtc +
                ", cldStartTimeUtc=" + cldStartTimeUtc +
                ", cldEndTimeUtc=" + cldEndTimeUtc +
                ", startTimeUtc=" + startTimeUtc +
                ", endTimeUtc=" + endTimeUtc +
                ", firstCoverageArea=" + firstCoverageArea +
                ", lastCoverageArea=" + lastCoverageArea +
                ", increasedCoverageArea=" + increasedCoverageArea +
                ", runningDuration=" + runningDuration +
                ", robotFamilyCode='" + robotFamilyCode +
                '}';
    }
}
