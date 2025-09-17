package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

import java.util.Date;

public class BmsBatteryInfo extends BaseEntity {
    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("report_time_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date reportTimeUtc;

    @JsonProperty("publish_time_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date publishTimeUtc;

    @JsonProperty("robot_family_code")
    private String robotFamilyCode;

    @JsonProperty("hardware_version_6")
    private String hardwareVersion6;

    @JsonProperty("hardware_version_14")
    private String hardwareVersion14;

    @JsonProperty("battery_voltage")
    private Long batteryVoltage;

    @JsonProperty("battery_current")
    private Double batteryCurrent;

    @JsonProperty("batt_full_cap")
    private Long battFullCap;

    @JsonProperty("battery")
    private Integer battery;

    @JsonProperty("batt_soh")
    private Integer battSoh;

    @JsonProperty("batt_cycle_times")
    private Long battCycleTimes;

    @JsonProperty("batt_protector_status")
    private Integer battProtectorStatus;

    @JsonProperty("batt_temp1")
    private Integer battTemp1;
    @JsonProperty("batt_temp2")
    private Integer battTemp2;
    @JsonProperty("batt_temp3")
    private Integer battTemp3;
    @JsonProperty("batt_temp4")
    private Integer battTemp4;
    @JsonProperty("batt_temp5")
    private Integer battTemp5;
    @JsonProperty("batt_temp6")
    private Integer battTemp6;
    @JsonProperty("batt_temp7")
    private Integer battTemp7;

    @JsonProperty("batt_volt1")
    private Long battVolt1;
    @JsonProperty("batt_volt2")
    private Long battVolt2;
    @JsonProperty("batt_volt3")
    private Long battVolt3;
    @JsonProperty("batt_volt4")
    private Long battVolt4;
    @JsonProperty("batt_volt5")
    private Long battVolt5;
    @JsonProperty("batt_volt6")
    private Long battVolt6;
    @JsonProperty("batt_volt7")
    private Long battVolt7;
    @JsonProperty("batt_volt8")
    private Long battVolt8;
    @JsonProperty("batt_volt9")
    private Long battVolt9;
    @JsonProperty("batt_volt10")
    private Long battVolt10;
    @JsonProperty("batt_volt11")
    private Long battVolt11;
    @JsonProperty("batt_volt12")
    private Long battVolt12;
    @JsonProperty("batt_volt13")
    private Long battVolt13;
    @JsonProperty("batt_volt14")
    private Long battVolt14;
    @JsonProperty("batt_volt15")
    private Long battVolt15;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Date getReportTimeUtc() {
        return reportTimeUtc;
    }

    public void setReportTimeUtc(Date reportTimeUtc) {
        this.reportTimeUtc = reportTimeUtc;
    }

    public Date getPublishTimeUtc() {
        return publishTimeUtc;
    }

    public void setPublishTimeUtc(Date publishTimeUtc) {
        this.publishTimeUtc = publishTimeUtc;
    }

    public String getRobotFamilyCode() {
        return robotFamilyCode;
    }

    public void setRobotFamilyCode(String robotFamilyCode) {
        this.robotFamilyCode = robotFamilyCode;
    }

    public String getHardwareVersion6() {
        return hardwareVersion6;
    }

    public void setHardwareVersion6(String hardwareVersion6) {
        this.hardwareVersion6 = hardwareVersion6;
    }

    public String getHardwareVersion14() {
        return hardwareVersion14;
    }

    public void setHardwareVersion14(String hardwareVersion14) {
        this.hardwareVersion14 = hardwareVersion14;
    }

    public Long getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(Long batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public Double getBatteryCurrent() {
        return batteryCurrent;
    }

    public void setBatteryCurrent(Double batteryCurrent) {
        this.batteryCurrent = batteryCurrent;
    }

    public Long getBattFullCap() {
        return battFullCap;
    }

    public void setBattFullCap(Long battFullCap) {
        this.battFullCap = battFullCap;
    }

    public Integer getBattery() {
        return battery;
    }

    public void setBattery(Integer battery) {
        this.battery = battery;
    }

    public Integer getBattSoh() {
        return battSoh;
    }

    public void setBattSoh(Integer battSoh) {
        this.battSoh = battSoh;
    }

    public Long getBattCycleTimes() {
        return battCycleTimes;
    }

    public void setBattCycleTimes(Long battCycleTimes) {
        this.battCycleTimes = battCycleTimes;
    }

    public Integer getBattProtectorStatus() {
        return battProtectorStatus;
    }

    public void setBattProtectorStatus(Integer battProtectorStatus) {
        this.battProtectorStatus = battProtectorStatus;
    }

    public Integer getBattTemp1() {
        return battTemp1;
    }

    public void setBattTemp1(Integer battTemp1) {
        this.battTemp1 = battTemp1;
    }

    public Integer getBattTemp2() {
        return battTemp2;
    }

    public void setBattTemp2(Integer battTemp2) {
        this.battTemp2 = battTemp2;
    }

    public Integer getBattTemp3() {
        return battTemp3;
    }

    public void setBattTemp3(Integer battTemp3) {
        this.battTemp3 = battTemp3;
    }

    public Integer getBattTemp4() {
        return battTemp4;
    }

    public void setBattTemp4(Integer battTemp4) {
        this.battTemp4 = battTemp4;
    }

    public Integer getBattTemp5() {
        return battTemp5;
    }

    public void setBattTemp5(Integer battTemp5) {
        this.battTemp5 = battTemp5;
    }

    public Integer getBattTemp6() {
        return battTemp6;
    }

    public void setBattTemp6(Integer battTemp6) {
        this.battTemp6 = battTemp6;
    }

    public Integer getBattTemp7() {
        return battTemp7;
    }

    public void setBattTemp7(Integer battTemp7) {
        this.battTemp7 = battTemp7;
    }

    public Long getBattVolt1() {
        return battVolt1;
    }

    public void setBattVolt1(Long battVolt1) {
        this.battVolt1 = battVolt1;
    }

    public Long getBattVolt2() {
        return battVolt2;
    }

    public void setBattVolt2(Long battVolt2) {
        this.battVolt2 = battVolt2;
    }

    public Long getBattVolt3() {
        return battVolt3;
    }

    public void setBattVolt3(Long battVolt3) {
        this.battVolt3 = battVolt3;
    }

    public Long getBattVolt4() {
        return battVolt4;
    }

    public void setBattVolt4(Long battVolt4) {
        this.battVolt4 = battVolt4;
    }

    public Long getBattVolt5() {
        return battVolt5;
    }

    public void setBattVolt5(Long battVolt5) {
        this.battVolt5 = battVolt5;
    }

    public Long getBattVolt6() {
        return battVolt6;
    }

    public void setBattVolt6(Long battVolt6) {
        this.battVolt6 = battVolt6;
    }

    public Long getBattVolt7() {
        return battVolt7;
    }

    public void setBattVolt7(Long battVolt7) {
        this.battVolt7 = battVolt7;
    }

    public Long getBattVolt8() {
        return battVolt8;
    }

    public void setBattVolt8(Long battVolt8) {
        this.battVolt8 = battVolt8;
    }

    public Long getBattVolt9() {
        return battVolt9;
    }

    public void setBattVolt9(Long battVolt9) {
        this.battVolt9 = battVolt9;
    }

    public Long getBattVolt10() {
        return battVolt10;
    }

    public void setBattVolt10(Long battVolt10) {
        this.battVolt10 = battVolt10;
    }

    public Long getBattVolt11() {
        return battVolt11;
    }

    public void setBattVolt11(Long battVolt11) {
        this.battVolt11 = battVolt11;
    }

    public Long getBattVolt12() {
        return battVolt12;
    }

    public void setBattVolt12(Long battVolt12) {
        this.battVolt12 = battVolt12;
    }

    public Long getBattVolt13() {
        return battVolt13;
    }

    public void setBattVolt13(Long battVolt13) {
        this.battVolt13 = battVolt13;
    }

    public Long getBattVolt14() {
        return battVolt14;
    }

    public void setBattVolt14(Long battVolt14) {
        this.battVolt14 = battVolt14;
    }

    public Long getBattVolt15() {
        return battVolt15;
    }

    public void setBattVolt15(Long battVolt15) {
        this.battVolt15 = battVolt15;
    }

    @Override
    public String toString() {
        return "BmsBatteryInfo{" +
                "productId='" + productId + '\'' +
                ", reportTimeUtc=" + reportTimeUtc +
                ", publishTimeUtc=" + publishTimeUtc +
                ", robotFamilyCode='" + robotFamilyCode + '\'' +
                ", hardwareVersion6='" + hardwareVersion6 + '\'' +
                ", hardwareVersion14='" + hardwareVersion14 + '\'' +
                ", batteryVoltage=" + batteryVoltage +
                ", batteryCurrent=" + batteryCurrent +
                ", battFullCap=" + battFullCap +
                ", battery=" + battery +
                ", battSoh=" + battSoh +
                ", battCycleTimes=" + battCycleTimes +
                ", battProtectorStatus=" + battProtectorStatus +
                ", battTemp1=" + battTemp1 +
                ", battTemp2=" + battTemp2 +
                ", battTemp3=" + battTemp3 +
                ", battTemp4=" + battTemp4 +
                ", battTemp5=" + battTemp5 +
                ", battTemp6=" + battTemp6 +
                ", battTemp7=" + battTemp7 +
                ", battVolt1=" + battVolt1 +
                ", battVolt2=" + battVolt2 +
                ", battVolt3=" + battVolt3 +
                ", battVolt4=" + battVolt4 +
                ", battVolt5=" + battVolt5 +
                ", battVolt6=" + battVolt6 +
                ", battVolt7=" + battVolt7 +
                ", battVolt8=" + battVolt8 +
                ", battVolt9=" + battVolt9 +
                ", battVolt10=" + battVolt10 +
                ", battVolt11=" + battVolt11 +
                ", battVolt12=" + battVolt12 +
                ", battVolt13=" + battVolt13 +
                ", battVolt14=" + battVolt14 +
                ", battVolt15=" + battVolt15 +
                '}';
    }

    @Override
    public Date getEventTime() {
        return getPublishTimeUtc();
    }

    @Override
    public String getKey() {
        return getProductId();
    }

    @Override
    public int compareTo(@NotNull BaseEntity o) {
        return this.getPublishTimeUtc().compareTo(o.getEventTime());
    }
}
