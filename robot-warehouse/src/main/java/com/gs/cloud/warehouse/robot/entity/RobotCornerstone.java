package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RobotCornerstone extends FactEntity {

    @JsonProperty("product_id")
    private String productId;
    @JsonProperty("report_timestamp_ms")
    private String reportTimestampMs;
    @JsonProperty("report_time_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date reportTimeUtc;
    @JsonProperty("report_time_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date reportTimeT8;
    @JsonProperty("setting_revision_mark")
    private String settingRevisionMark;
    @JsonProperty("protocol_version")
    private String protocolVersion;
    @JsonProperty("task_sub_type")
    private String taskSubType;
    @JsonProperty("task_id")
    private String taskId;
    @JsonProperty("task_revision_mark")
    private String taskRevisionMark;
    @JsonProperty("collect_timestamp_ms")
    private String collectTimestampMs;
    @JsonProperty("collect_time_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date collectTimeUtc;
    @JsonProperty("collect_time_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date collectTimeT8;
    @JsonProperty("version")
    private String version;
    @JsonProperty("odom_position_x")
    private String odomPositionX;
    @JsonProperty("odom_position_y")
    private String odomPositionY;
    @JsonProperty("odom_position_z")
    private String odomPositionZ;
    @JsonProperty("odom_orientation_x")
    private String odomOrientationX;
    @JsonProperty("odom_orientation_y")
    private String odomOrientationY;
    @JsonProperty("odom_orientation_z")
    private String odomOrientationZ;
    @JsonProperty("odom_orientation_w")
    private String odomOrientationW;
    @JsonProperty("odom_v")
    private String odomV;
    @JsonProperty("odom_w")
    private String odomW;
    @JsonProperty("unbiased_imu_pry_pitch")
    private String unbiasedImuPryPitch;
    @JsonProperty("unbiased_imu_pry_roll")
    private String unbiasedImuPryRoll;
    @JsonProperty("time_startup")
    private String timeStartup;
    @JsonProperty("time_task_start")
    private String timeTaskStart;
    @JsonProperty("time_current")
    private String timeCurrent;
    @JsonProperty("wifi_intensity_level")
    private String wifiIntensityLevel;
    @JsonProperty("mobile_intensity_level")
    private String mobileIntensityLevel;
    @JsonProperty("wifi_traffic")
    private String wifiTraffic;
    @JsonProperty("mobile_traffic")
    private String mobileTraffic;
    @JsonProperty("wifi_speed")
    private String wifiSpeed;
    @JsonProperty("wifi_speed_rx")
    private String wifiSpeedRx;
    @JsonProperty("wifi_speed_tx")
    private String wifiSpeedTx;
    @JsonProperty("mobile_speed")
    private String mobileSpeed;
    @JsonProperty("mobile_speed_rx")
    private String mobileSpeedRx;
    @JsonProperty("mobile_speed_tx")
    private String mobileSpeedTx;
    @JsonProperty("month_traffic")
    private String monthTraffic;
    @JsonProperty("location_status")
    private String locationStatus;
    @JsonProperty("location_map_name")
    private String locationMapName;
    @JsonProperty("location_map_origin_x")
    private String locationMapOriginX;
    @JsonProperty("location_map_origin_y")
    private String locationMapOriginY;
    @JsonProperty("location_map_resolution")
    private String locationMapResolution;
    @JsonProperty("location_map_grid_width")
    private String locationMapGridWidth;
    @JsonProperty("location_map_grid_height")
    private String locationMapGridHeight;
    @JsonProperty("location_x")
    private String locationX;
    @JsonProperty("location_y")
    private String locationY;
    @JsonProperty("location_yaw")
    private String locationYaw;
    @JsonProperty("location_x1")
    private String locationX1;
    @JsonProperty("location_y1")
    private String locationY1;
    @JsonProperty("location_yaw1")
    private String locationYaw1;
    @JsonProperty("sleep_mode")
    private String sleepMode;
    @JsonProperty("rebooting")
    private String rebooting;
    @JsonProperty("manual_controlling")
    private String manualControlling;
    @JsonProperty("ramp_assist_status")
    private String rampAssistStatus;
    @JsonProperty("ota_status")
    private String otaStatus;
    @JsonProperty("auto_mode")
    private String autoMode;
    @JsonProperty("emergency_stop")
    private String emergencyStop;
    @JsonProperty("manual_charging")
    private String manualCharging;
    @JsonProperty("manual_working")
    private String manualWorking;
    @JsonProperty("wakeup_mode")
    private String wakeupMode;
    @JsonProperty("maintain_mode")
    private String maintainMode;
    @JsonProperty("scheduler_pause_flags")
    private String schedulerPauseFlags;
    @JsonProperty("scheduler_arranger")
    private String schedulerArranger;
    @JsonProperty("scaning_map_status")
    private String scaningMapStatus;
    @JsonProperty("scaning_map_name")
    private String scaningMapName;
    @JsonProperty("record_path_status")
    private String recordPathStatus;
    @JsonProperty("record_path_name")
    private String recordPathName;
    @JsonProperty("navi_status")
    private String naviStatus;
    @JsonProperty("navi_instance_id")
    private String naviInstanceId;
    @JsonProperty("navi_map_name")
    private String naviMapName;
    @JsonProperty("navi_pos_name")
    private String naviPosName;
    @JsonProperty("navi_pos_type")
    private String naviPosType;
    @JsonProperty("navi_pos_function")
    private String naviPosFunction;
    @JsonProperty("task_status")
    private String taskStatus;
    @JsonProperty("task_instance_id")
    private String taskInstanceId;
    @JsonProperty("multi_task_name")
    private String multiTaskName;
    @JsonProperty("multi_task_list_count")
    private String multiTaskListCount;
    @JsonProperty("multi_task_loop_count")
    private String multiTaskLoopCount;
    @JsonProperty("task_queue_name")
    private String taskQueueName;
    @JsonProperty("task_queue_list_count")
    private String taskQueueListCount;
    @JsonProperty("task_queue_loop_count")
    private String taskQueueLoopCount;
    @JsonProperty("task_queue_map_name")
    private String taskQueueMapName;
    @JsonProperty("multi_task_list_index")
    private String multiTaskListIndex;
    @JsonProperty("multi_task_loop_index")
    private String multiTaskLoopIndex;
    @JsonProperty("task_queue_list_index")
    private String taskQueueListIndex;
    @JsonProperty("task_queue_loop_index")
    private String taskQueueLoopIndex;
    @JsonProperty("task_queue_progress")
    private String taskQueueProgress;
    @JsonProperty("sub_task_progress")
    private String subTaskProgress;
    @JsonProperty("sub_task_type")
    private String subTaskType;
    @JsonProperty("task_expect_cleaning_type")
    private String taskExpectCleaningType;
    @JsonProperty("task_current_cleaning_type")
    private String taskCurrentCleaningType;
    @JsonProperty("take_elevator_status")
    private String takeElevatorStatus;
    @JsonProperty("take_elevator_from")
    private String takeElevatorFrom;
    @JsonProperty("take_elevator_to")
    private String takeElevatorTo;
    @JsonProperty("take_elevator_state")
    private String takeElevatorState;
    @JsonProperty("station_status")
    private String stationStatus;
    @JsonProperty("station_state")
    private String stationState;
    @JsonProperty("station_num_in_queue")
    private String stationNumInQueue;
    @JsonProperty("station_available_items")
    private String stationAvailableItems;
    @JsonProperty("station_supplying_items")
    private String stationSupplyingItems;
    @JsonProperty("station_finished_items")
    private String stationFinishedItems;
    @JsonProperty("station_pos_name")
    private String stationPosName;
    @JsonProperty("station_pos_type")
    private String stationPosType;
    @JsonProperty("station_pos_function")
    private String stationPosFunction;
    @JsonProperty("battery_voltage")
    private String batteryVoltage;
    @JsonProperty("charger_voltage")
    private String chargerVoltage;
    @JsonProperty("charger_current")
    private String chargerCurrent;
    @JsonProperty("battery_current")
    private String batteryCurrent;
    @JsonProperty("battery")
    private String battery;
    @JsonProperty("wheel_driver_data8")
    private String wheelDriverData8;
    @JsonProperty("wheel_driver_data9")
    private String wheelDriverData9;
    @JsonProperty("wheel_driver_datae")
    private String wheelDriverDataE;
    @JsonProperty("wheel_driver_dataf")
    private String wheelDriverDataF;
    @JsonProperty("wheel_driver_data10")
    private String wheelDriverData10;
    @JsonProperty("wheel_driver_data11")
    private String wheelDriverData11;
    @JsonProperty("wheel_driver_data12")
    private String wheelDriverData12;
    @JsonProperty("wheel_driver_data13")
    private String wheelDriverData13;
    @JsonProperty("hybrid_driver_data32")
    private String hybridDriverData32;
    @JsonProperty("hybrid_driver_data33")
    private String hybridDriverData33;
    @JsonProperty("hybrid_driver_data34")
    private String hybridDriverData34;
    @JsonProperty("hybrid_driver_data35")
    private String hybridDriverData35;
    @JsonProperty("hybrid_driver_data36")
    private String hybridDriverData36;
    @JsonProperty("hybrid_driver_data37")
    private String hybridDriverData37;
    @JsonProperty("hybrid_driver_data38")
    private String hybridDriverData38;
    @JsonProperty("hybrid_driver_data39")
    private String hybridDriverData39;
    @JsonProperty("rolling_brush_motor_working")
    private String rollingBrushMotorWorking;
    @JsonProperty("brush_motor_working")
    private String brushMotorWorking;
    @JsonProperty("left_brush_motor_working")
    private String leftBrushMotorWorking;
    @JsonProperty("spray_motor")
    private String sprayMotor;
    @JsonProperty("fan_level")
    private String fanLevel;
    @JsonProperty("squeegee_down")
    private String squeegeeDown;
    @JsonProperty("front_rolling_brush_motor_current")
    private String frontRollingBrushMotorCurrent;
    @JsonProperty("rear_rolling_brush_motor_current")
    private String rearRollingBrushMotorCurrent;
    @JsonProperty("rolling_brush_motor_front")
    private String rollingBrushMotorFront;
    @JsonProperty("rolling_brush_motor_after")
    private String rollingBrushMotorAfter;
    @JsonProperty("brush_spin_level")
    private String brushSpinLevel;
    @JsonProperty("side_brush_spin_level")
    private String sideBrushSpinLevel;
    @JsonProperty("brush_down_position")
    private String brushDownPosition;
    @JsonProperty("water_level")
    private String waterLevel;
    @JsonProperty("left_brush_spin_level")
    private String leftBrushSpinLevel;
    @JsonProperty("filter_level")
    private String filterLevel;
    @JsonProperty("spray_detergent")
    private String sprayDetergent;
    @JsonProperty("valve")
    private String valve;
    @JsonProperty("clean_water_level")
    private String cleanWaterLevel;
    @JsonProperty("sewage_level")
    private String sewageLevel;
    @JsonProperty("rolling_brush_motor_front_feed_back")
    private String rollingBrushMotorFrontFeedBack;
    @JsonProperty("rolling_brush_motor_after_feed_back")
    private String rollingBrushMotorAfterFeedBack;
    @JsonProperty("left_side_brush_current_feed_back")
    private String leftSideBrushCurrentFeedBack;
    @JsonProperty("right_side_brush_current_feed_back")
    private String rightSideBrushCurrentFeedBack;
    @JsonProperty("xds_driver_info")
    private String xdsDriverInfo;
    @JsonProperty("brush_down_position_feed_back")
    private String brushDownPositionFeedBack;
    @JsonProperty("suction_pressure_voltage")
    private String suctionPressureVoltage;
    @JsonProperty("left_side_brush_motor_current")
    private String leftSideBrushMotorCurrent;
    @JsonProperty("right_side_brush_motor_current")
    private String rightSideBrushMotorCurrent;
    @JsonProperty("spray_motor_current")
    private String sprayMotorCurrent;
    @JsonProperty("vacuum_motor_current")
    private String vacuumMotorCurrent;
    @JsonProperty("squeegee_lift_motor_current")
    private String squeegeeLiftMotorCurrent;
    @JsonProperty("filter_motor_current")
    private String filterMotorCurrent;
    @JsonProperty("time_startup_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date timeStartupUtc;
    @JsonProperty("time_startup_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date timeStartupT8;
    @JsonProperty("time_task_start_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date timeTaskStartUtc;
    @JsonProperty("time_task_start_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date timeTaskStartT8;
    @JsonProperty("time_current_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date timeCurrentUtc;
    @JsonProperty("time_current_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date timeCurrentT8;
    @JsonProperty("publish_timestamp_ms")
    private Long publishTimestampMs;
    @JsonProperty("publish_time_utc")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date publishTimeUtc;
    @JsonProperty("publish_time_t8")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date publishTimeT8;
    @JsonProperty("rolling_brush_motor_front_pwm_feed_back")
    private String rollingBrushMotorFrontPwmFeedBack;
    @JsonProperty("rolling_brush_motor_after_pwm_feed_back")
    private String rollingBrushMotorAfterPwmFeedBack;

    @Override
    public Date getEventTime() {
        return getReportTimeUtc();
    }

    @Override
    public String getKey() {
        return getProductId();
    }

    @Override
    public CommonFormat<RobotCornerstone> getSerDeserializer() {
        return new CommonFormat<>(RobotCornerstone.class);
    }

    @Override
    public CommonKeySerialization<RobotCornerstone> getKeySerializer() {
        return new CommonKeySerialization<>();
    }

    @Override
    public String getKafkaServer() {
        return "kafka.bootstrap.servers.bigdata";
    }

    @Override
    public String getKafkaTopic() {
        return "kafka.topic.robot.cornerstone";
    }

    @Override
    public String getKafkaGroupId() {
        return "kafka.group.id.ods.beep.cornerStone";
    }

    @Override
    public int compareTo(@NotNull BaseEntity o) {
        return 0;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getReportTimestampMs() {
        return reportTimestampMs;
    }

    public void setReportTimestampMs(String reportTimestampMs) {
        this.reportTimestampMs = reportTimestampMs;
    }

    public Date getReportTimeUtc() {
        return reportTimeUtc;
    }

    public void setReportTimeUtc(Date reportTimeUtc) {
        this.reportTimeUtc = reportTimeUtc;
    }

    public Date getReportTimeT8() {
        return reportTimeT8;
    }

    public void setReportTimeT8(Date reportTimeT8) {
        this.reportTimeT8 = reportTimeT8;
    }

    public String getSettingRevisionMark() {
        return settingRevisionMark;
    }

    public void setSettingRevisionMark(String settingRevisionMark) {
        this.settingRevisionMark = settingRevisionMark;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getTaskSubType() {
        return taskSubType;
    }

    public void setTaskSubType(String taskSubType) {
        this.taskSubType = taskSubType;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskRevisionMark() {
        return taskRevisionMark;
    }

    public void setTaskRevisionMark(String taskRevisionMark) {
        this.taskRevisionMark = taskRevisionMark;
    }

    public String getCollectTimestampMs() {
        return collectTimestampMs;
    }

    public void setCollectTimestampMs(String collectTimestampMs) {
        this.collectTimestampMs = collectTimestampMs;
    }

    public Date getCollectTimeUtc() {
        return collectTimeUtc;
    }

    public void setCollectTimeUtc(Date collectTimeUtc) {
        this.collectTimeUtc = collectTimeUtc;
    }

    public Date getCollectTimeT8() {
        return collectTimeT8;
    }

    public void setCollectTimeT8(Date collectTimeT8) {
        this.collectTimeT8 = collectTimeT8;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getOdomPositionX() {
        return odomPositionX;
    }

    public void setOdomPositionX(String odomPositionX) {
        this.odomPositionX = odomPositionX;
    }

    public String getOdomPositionY() {
        return odomPositionY;
    }

    public void setOdomPositionY(String odomPositionY) {
        this.odomPositionY = odomPositionY;
    }

    public String getOdomPositionZ() {
        return odomPositionZ;
    }

    public void setOdomPositionZ(String odomPositionZ) {
        this.odomPositionZ = odomPositionZ;
    }

    public String getOdomOrientationX() {
        return odomOrientationX;
    }

    public void setOdomOrientationX(String odomOrientationX) {
        this.odomOrientationX = odomOrientationX;
    }

    public String getOdomOrientationY() {
        return odomOrientationY;
    }

    public void setOdomOrientationY(String odomOrientationY) {
        this.odomOrientationY = odomOrientationY;
    }

    public String getOdomOrientationZ() {
        return odomOrientationZ;
    }

    public void setOdomOrientationZ(String odomOrientationZ) {
        this.odomOrientationZ = odomOrientationZ;
    }

    public String getOdomOrientationW() {
        return odomOrientationW;
    }

    public void setOdomOrientationW(String odomOrientationW) {
        this.odomOrientationW = odomOrientationW;
    }

    public String getOdomV() {
        return odomV;
    }

    public void setOdomV(String odomV) {
        this.odomV = odomV;
    }

    public String getOdomW() {
        return odomW;
    }

    public void setOdomW(String odomW) {
        this.odomW = odomW;
    }

    public String getUnbiasedImuPryPitch() {
        return unbiasedImuPryPitch;
    }

    public void setUnbiasedImuPryPitch(String unbiasedImuPryPitch) {
        this.unbiasedImuPryPitch = unbiasedImuPryPitch;
    }

    public String getUnbiasedImuPryRoll() {
        return unbiasedImuPryRoll;
    }

    public void setUnbiasedImuPryRoll(String unbiasedImuPryRoll) {
        this.unbiasedImuPryRoll = unbiasedImuPryRoll;
    }

    public String getTimeStartup() {
        return timeStartup;
    }

    public void setTimeStartup(String timeStartup) {
        this.timeStartup = timeStartup;
    }

    public String getTimeTaskStart() {
        return timeTaskStart;
    }

    public void setTimeTaskStart(String timeTaskStart) {
        this.timeTaskStart = timeTaskStart;
    }

    public String getTimeCurrent() {
        return timeCurrent;
    }

    public void setTimeCurrent(String timeCurrent) {
        this.timeCurrent = timeCurrent;
    }

    public String getWifiIntensityLevel() {
        return wifiIntensityLevel;
    }

    public void setWifiIntensityLevel(String wifiIntensityLevel) {
        this.wifiIntensityLevel = wifiIntensityLevel;
    }

    public String getMobileIntensityLevel() {
        return mobileIntensityLevel;
    }

    public void setMobileIntensityLevel(String mobileIntensityLevel) {
        this.mobileIntensityLevel = mobileIntensityLevel;
    }

    public String getWifiTraffic() {
        return wifiTraffic;
    }

    public void setWifiTraffic(String wifiTraffic) {
        this.wifiTraffic = wifiTraffic;
    }

    public String getMobileTraffic() {
        return mobileTraffic;
    }

    public void setMobileTraffic(String mobileTraffic) {
        this.mobileTraffic = mobileTraffic;
    }

    public String getWifiSpeed() {
        return wifiSpeed;
    }

    public void setWifiSpeed(String wifiSpeed) {
        this.wifiSpeed = wifiSpeed;
    }

    public String getWifiSpeedRx() {
        return wifiSpeedRx;
    }

    public void setWifiSpeedRx(String wifiSpeedRx) {
        this.wifiSpeedRx = wifiSpeedRx;
    }

    public String getWifiSpeedTx() {
        return wifiSpeedTx;
    }

    public void setWifiSpeedTx(String wifiSpeedTx) {
        this.wifiSpeedTx = wifiSpeedTx;
    }

    public String getMobileSpeed() {
        return mobileSpeed;
    }

    public void setMobileSpeed(String mobileSpeed) {
        this.mobileSpeed = mobileSpeed;
    }

    public String getMobileSpeedRx() {
        return mobileSpeedRx;
    }

    public void setMobileSpeedRx(String mobileSpeedRx) {
        this.mobileSpeedRx = mobileSpeedRx;
    }

    public String getMobileSpeedTx() {
        return mobileSpeedTx;
    }

    public void setMobileSpeedTx(String mobileSpeedTx) {
        this.mobileSpeedTx = mobileSpeedTx;
    }

    public String getMonthTraffic() {
        return monthTraffic;
    }

    public void setMonthTraffic(String monthTraffic) {
        this.monthTraffic = monthTraffic;
    }

    public String getLocationStatus() {
        return locationStatus;
    }

    public void setLocationStatus(String locationStatus) {
        this.locationStatus = locationStatus;
    }

    public String getLocationMapName() {
        return locationMapName;
    }

    public void setLocationMapName(String locationMapName) {
        this.locationMapName = locationMapName;
    }

    public String getLocationMapOriginX() {
        return locationMapOriginX;
    }

    public void setLocationMapOriginX(String locationMapOriginX) {
        this.locationMapOriginX = locationMapOriginX;
    }

    public String getLocationMapOriginY() {
        return locationMapOriginY;
    }

    public void setLocationMapOriginY(String locationMapOriginY) {
        this.locationMapOriginY = locationMapOriginY;
    }

    public String getLocationMapResolution() {
        return locationMapResolution;
    }

    public void setLocationMapResolution(String locationMapResolution) {
        this.locationMapResolution = locationMapResolution;
    }

    public String getLocationMapGridWidth() {
        return locationMapGridWidth;
    }

    public void setLocationMapGridWidth(String locationMapGridWidth) {
        this.locationMapGridWidth = locationMapGridWidth;
    }

    public String getLocationMapGridHeight() {
        return locationMapGridHeight;
    }

    public void setLocationMapGridHeight(String locationMapGridHeight) {
        this.locationMapGridHeight = locationMapGridHeight;
    }

    public String getLocationX() {
        return locationX;
    }

    public void setLocationX(String locationX) {
        this.locationX = locationX;
    }

    public String getLocationY() {
        return locationY;
    }

    public void setLocationY(String locationY) {
        this.locationY = locationY;
    }

    public String getLocationYaw() {
        return locationYaw;
    }

    public void setLocationYaw(String locationYaw) {
        this.locationYaw = locationYaw;
    }

    public String getLocationX1() {
        return locationX1;
    }

    public void setLocationX1(String locationX1) {
        this.locationX1 = locationX1;
    }

    public String getLocationY1() {
        return locationY1;
    }

    public void setLocationY1(String locationY1) {
        this.locationY1 = locationY1;
    }

    public String getLocationYaw1() {
        return locationYaw1;
    }

    public void setLocationYaw1(String locationYaw1) {
        this.locationYaw1 = locationYaw1;
    }

    public String getSleepMode() {
        return sleepMode;
    }

    public void setSleepMode(String sleepMode) {
        this.sleepMode = sleepMode;
    }

    public String getRebooting() {
        return rebooting;
    }

    public void setRebooting(String rebooting) {
        this.rebooting = rebooting;
    }

    public String getManualControlling() {
        return manualControlling;
    }

    public void setManualControlling(String manualControlling) {
        this.manualControlling = manualControlling;
    }

    public String getRampAssistStatus() {
        return rampAssistStatus;
    }

    public void setRampAssistStatus(String rampAssistStatus) {
        this.rampAssistStatus = rampAssistStatus;
    }

    public String getOtaStatus() {
        return otaStatus;
    }

    public void setOtaStatus(String otaStatus) {
        this.otaStatus = otaStatus;
    }

    public String getAutoMode() {
        return autoMode;
    }

    public void setAutoMode(String autoMode) {
        this.autoMode = autoMode;
    }

    public String getEmergencyStop() {
        return emergencyStop;
    }

    public void setEmergencyStop(String emergencyStop) {
        this.emergencyStop = emergencyStop;
    }

    public String getManualCharging() {
        return manualCharging;
    }

    public void setManualCharging(String manualCharging) {
        this.manualCharging = manualCharging;
    }

    public String getManualWorking() {
        return manualWorking;
    }

    public void setManualWorking(String manualWorking) {
        this.manualWorking = manualWorking;
    }

    public String getWakeupMode() {
        return wakeupMode;
    }

    public void setWakeupMode(String wakeupMode) {
        this.wakeupMode = wakeupMode;
    }

    public String getMaintainMode() {
        return maintainMode;
    }

    public void setMaintainMode(String maintainMode) {
        this.maintainMode = maintainMode;
    }

    public String getSchedulerPauseFlags() {
        return schedulerPauseFlags;
    }

    public void setSchedulerPauseFlags(String schedulerPauseFlags) {
        this.schedulerPauseFlags = schedulerPauseFlags;
    }

    public String getSchedulerArranger() {
        return schedulerArranger;
    }

    public void setSchedulerArranger(String schedulerArranger) {
        this.schedulerArranger = schedulerArranger;
    }

    public String getScaningMapStatus() {
        return scaningMapStatus;
    }

    public void setScaningMapStatus(String scaningMapStatus) {
        this.scaningMapStatus = scaningMapStatus;
    }

    public String getScaningMapName() {
        return scaningMapName;
    }

    public void setScaningMapName(String scaningMapName) {
        this.scaningMapName = scaningMapName;
    }

    public String getRecordPathStatus() {
        return recordPathStatus;
    }

    public void setRecordPathStatus(String recordPathStatus) {
        this.recordPathStatus = recordPathStatus;
    }

    public String getRecordPathName() {
        return recordPathName;
    }

    public void setRecordPathName(String recordPathName) {
        this.recordPathName = recordPathName;
    }

    public String getNaviStatus() {
        return naviStatus;
    }

    public void setNaviStatus(String naviStatus) {
        this.naviStatus = naviStatus;
    }

    public String getNaviInstanceId() {
        return naviInstanceId;
    }

    public void setNaviInstanceId(String naviInstanceId) {
        this.naviInstanceId = naviInstanceId;
    }

    public String getNaviMapName() {
        return naviMapName;
    }

    public void setNaviMapName(String naviMapName) {
        this.naviMapName = naviMapName;
    }

    public String getNaviPosName() {
        return naviPosName;
    }

    public void setNaviPosName(String naviPosName) {
        this.naviPosName = naviPosName;
    }

    public String getNaviPosType() {
        return naviPosType;
    }

    public void setNaviPosType(String naviPosType) {
        this.naviPosType = naviPosType;
    }

    public String getNaviPosFunction() {
        return naviPosFunction;
    }

    public void setNaviPosFunction(String naviPosFunction) {
        this.naviPosFunction = naviPosFunction;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getTaskInstanceId() {
        return taskInstanceId;
    }

    public void setTaskInstanceId(String taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
    }

    public String getMultiTaskName() {
        return multiTaskName;
    }

    public void setMultiTaskName(String multiTaskName) {
        this.multiTaskName = multiTaskName;
    }

    public String getMultiTaskListCount() {
        return multiTaskListCount;
    }

    public void setMultiTaskListCount(String multiTaskListCount) {
        this.multiTaskListCount = multiTaskListCount;
    }

    public String getMultiTaskLoopCount() {
        return multiTaskLoopCount;
    }

    public void setMultiTaskLoopCount(String multiTaskLoopCount) {
        this.multiTaskLoopCount = multiTaskLoopCount;
    }

    public String getTaskQueueName() {
        return taskQueueName;
    }

    public void setTaskQueueName(String taskQueueName) {
        this.taskQueueName = taskQueueName;
    }

    public String getTaskQueueListCount() {
        return taskQueueListCount;
    }

    public void setTaskQueueListCount(String taskQueueListCount) {
        this.taskQueueListCount = taskQueueListCount;
    }

    public String getTaskQueueLoopCount() {
        return taskQueueLoopCount;
    }

    public void setTaskQueueLoopCount(String taskQueueLoopCount) {
        this.taskQueueLoopCount = taskQueueLoopCount;
    }

    public String getTaskQueueMapName() {
        return taskQueueMapName;
    }

    public void setTaskQueueMapName(String taskQueueMapName) {
        this.taskQueueMapName = taskQueueMapName;
    }

    public String getMultiTaskListIndex() {
        return multiTaskListIndex;
    }

    public void setMultiTaskListIndex(String multiTaskListIndex) {
        this.multiTaskListIndex = multiTaskListIndex;
    }

    public String getMultiTaskLoopIndex() {
        return multiTaskLoopIndex;
    }

    public void setMultiTaskLoopIndex(String multiTaskLoopIndex) {
        this.multiTaskLoopIndex = multiTaskLoopIndex;
    }

    public String getTaskQueueListIndex() {
        return taskQueueListIndex;
    }

    public void setTaskQueueListIndex(String taskQueueListIndex) {
        this.taskQueueListIndex = taskQueueListIndex;
    }

    public String getTaskQueueLoopIndex() {
        return taskQueueLoopIndex;
    }

    public void setTaskQueueLoopIndex(String taskQueueLoopIndex) {
        this.taskQueueLoopIndex = taskQueueLoopIndex;
    }

    public String getTaskQueueProgress() {
        return taskQueueProgress;
    }

    public void setTaskQueueProgress(String taskQueueProgress) {
        this.taskQueueProgress = taskQueueProgress;
    }

    public String getSubTaskProgress() {
        return subTaskProgress;
    }

    public void setSubTaskProgress(String subTaskProgress) {
        this.subTaskProgress = subTaskProgress;
    }

    public String getSubTaskType() {
        return subTaskType;
    }

    public void setSubTaskType(String subTaskType) {
        this.subTaskType = subTaskType;
    }

    public String getTaskExpectCleaningType() {
        return taskExpectCleaningType;
    }

    public void setTaskExpectCleaningType(String taskExpectCleaningType) {
        this.taskExpectCleaningType = taskExpectCleaningType;
    }

    public String getTaskCurrentCleaningType() {
        return taskCurrentCleaningType;
    }

    public void setTaskCurrentCleaningType(String taskCurrentCleaningType) {
        this.taskCurrentCleaningType = taskCurrentCleaningType;
    }

    public String getTakeElevatorStatus() {
        return takeElevatorStatus;
    }

    public void setTakeElevatorStatus(String takeElevatorStatus) {
        this.takeElevatorStatus = takeElevatorStatus;
    }

    public String getTakeElevatorFrom() {
        return takeElevatorFrom;
    }

    public void setTakeElevatorFrom(String takeElevatorFrom) {
        this.takeElevatorFrom = takeElevatorFrom;
    }

    public String getTakeElevatorTo() {
        return takeElevatorTo;
    }

    public void setTakeElevatorTo(String takeElevatorTo) {
        this.takeElevatorTo = takeElevatorTo;
    }

    public String getTakeElevatorState() {
        return takeElevatorState;
    }

    public void setTakeElevatorState(String takeElevatorState) {
        this.takeElevatorState = takeElevatorState;
    }

    public String getStationStatus() {
        return stationStatus;
    }

    public void setStationStatus(String stationStatus) {
        this.stationStatus = stationStatus;
    }

    public String getStationState() {
        return stationState;
    }

    public void setStationState(String stationState) {
        this.stationState = stationState;
    }

    public String getStationNumInQueue() {
        return stationNumInQueue;
    }

    public void setStationNumInQueue(String stationNumInQueue) {
        this.stationNumInQueue = stationNumInQueue;
    }

    public String getStationAvailableItems() {
        return stationAvailableItems;
    }

    public void setStationAvailableItems(String stationAvailableItems) {
        this.stationAvailableItems = stationAvailableItems;
    }

    public String getStationSupplyingItems() {
        return stationSupplyingItems;
    }

    public void setStationSupplyingItems(String stationSupplyingItems) {
        this.stationSupplyingItems = stationSupplyingItems;
    }

    public String getStationFinishedItems() {
        return stationFinishedItems;
    }

    public void setStationFinishedItems(String stationFinishedItems) {
        this.stationFinishedItems = stationFinishedItems;
    }

    public String getStationPosName() {
        return stationPosName;
    }

    public void setStationPosName(String stationPosName) {
        this.stationPosName = stationPosName;
    }

    public String getStationPosType() {
        return stationPosType;
    }

    public void setStationPosType(String stationPosType) {
        this.stationPosType = stationPosType;
    }

    public String getStationPosFunction() {
        return stationPosFunction;
    }

    public void setStationPosFunction(String stationPosFunction) {
        this.stationPosFunction = stationPosFunction;
    }

    public String getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(String batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public String getChargerVoltage() {
        return chargerVoltage;
    }

    public void setChargerVoltage(String chargerVoltage) {
        this.chargerVoltage = chargerVoltage;
    }

    public String getChargerCurrent() {
        return chargerCurrent;
    }

    public void setChargerCurrent(String chargerCurrent) {
        this.chargerCurrent = chargerCurrent;
    }

    public String getBatteryCurrent() {
        return batteryCurrent;
    }

    public void setBatteryCurrent(String batteryCurrent) {
        this.batteryCurrent = batteryCurrent;
    }

    public String getBattery() {
        return battery;
    }

    public void setBattery(String battery) {
        this.battery = battery;
    }

    public String getWheelDriverData8() {
        return wheelDriverData8;
    }

    public void setWheelDriverData8(String wheelDriverData8) {
        this.wheelDriverData8 = wheelDriverData8;
    }

    public String getWheelDriverData9() {
        return wheelDriverData9;
    }

    public void setWheelDriverData9(String wheelDriverData9) {
        this.wheelDriverData9 = wheelDriverData9;
    }

    public String getWheelDriverDataE() {
        return wheelDriverDataE;
    }

    public void setWheelDriverDataE(String wheelDriverDataE) {
        this.wheelDriverDataE = wheelDriverDataE;
    }

    public String getWheelDriverDataF() {
        return wheelDriverDataF;
    }

    public void setWheelDriverDataF(String wheelDriverDataF) {
        this.wheelDriverDataF = wheelDriverDataF;
    }

    public String getWheelDriverData10() {
        return wheelDriverData10;
    }

    public void setWheelDriverData10(String wheelDriverData10) {
        this.wheelDriverData10 = wheelDriverData10;
    }

    public String getWheelDriverData11() {
        return wheelDriverData11;
    }

    public void setWheelDriverData11(String wheelDriverData11) {
        this.wheelDriverData11 = wheelDriverData11;
    }

    public String getWheelDriverData12() {
        return wheelDriverData12;
    }

    public void setWheelDriverData12(String wheelDriverData12) {
        this.wheelDriverData12 = wheelDriverData12;
    }

    public String getWheelDriverData13() {
        return wheelDriverData13;
    }

    public void setWheelDriverData13(String wheelDriverData13) {
        this.wheelDriverData13 = wheelDriverData13;
    }

    public String getHybridDriverData32() {
        return hybridDriverData32;
    }

    public void setHybridDriverData32(String hybridDriverData32) {
        this.hybridDriverData32 = hybridDriverData32;
    }

    public String getHybridDriverData33() {
        return hybridDriverData33;
    }

    public void setHybridDriverData33(String hybridDriverData33) {
        this.hybridDriverData33 = hybridDriverData33;
    }

    public String getHybridDriverData34() {
        return hybridDriverData34;
    }

    public void setHybridDriverData34(String hybridDriverData34) {
        this.hybridDriverData34 = hybridDriverData34;
    }

    public String getHybridDriverData35() {
        return hybridDriverData35;
    }

    public void setHybridDriverData35(String hybridDriverData35) {
        this.hybridDriverData35 = hybridDriverData35;
    }

    public String getHybridDriverData36() {
        return hybridDriverData36;
    }

    public void setHybridDriverData36(String hybridDriverData36) {
        this.hybridDriverData36 = hybridDriverData36;
    }

    public String getHybridDriverData37() {
        return hybridDriverData37;
    }

    public void setHybridDriverData37(String hybridDriverData37) {
        this.hybridDriverData37 = hybridDriverData37;
    }

    public String getHybridDriverData38() {
        return hybridDriverData38;
    }

    public void setHybridDriverData38(String hybridDriverData38) {
        this.hybridDriverData38 = hybridDriverData38;
    }

    public String getHybridDriverData39() {
        return hybridDriverData39;
    }

    public void setHybridDriverData39(String hybridDriverData39) {
        this.hybridDriverData39 = hybridDriverData39;
    }

    public String getRollingBrushMotorWorking() {
        return rollingBrushMotorWorking;
    }

    public void setRollingBrushMotorWorking(String rollingBrushMotorWorking) {
        this.rollingBrushMotorWorking = rollingBrushMotorWorking;
    }

    public String getBrushMotorWorking() {
        return brushMotorWorking;
    }

    public void setBrushMotorWorking(String brushMotorWorking) {
        this.brushMotorWorking = brushMotorWorking;
    }

    public String getLeftBrushMotorWorking() {
        return leftBrushMotorWorking;
    }

    public void setLeftBrushMotorWorking(String leftBrushMotorWorking) {
        this.leftBrushMotorWorking = leftBrushMotorWorking;
    }

    public String getSprayMotor() {
        return sprayMotor;
    }

    public void setSprayMotor(String sprayMotor) {
        this.sprayMotor = sprayMotor;
    }

    public String getFanLevel() {
        return fanLevel;
    }

    public void setFanLevel(String fanLevel) {
        this.fanLevel = fanLevel;
    }

    public String getSqueegeeDown() {
        return squeegeeDown;
    }

    public void setSqueegeeDown(String squeegeeDown) {
        this.squeegeeDown = squeegeeDown;
    }

    public String getFrontRollingBrushMotorCurrent() {
        return frontRollingBrushMotorCurrent;
    }

    public void setFrontRollingBrushMotorCurrent(String frontRollingBrushMotorCurrent) {
        this.frontRollingBrushMotorCurrent = frontRollingBrushMotorCurrent;
    }

    public String getRearRollingBrushMotorCurrent() {
        return rearRollingBrushMotorCurrent;
    }

    public void setRearRollingBrushMotorCurrent(String rearRollingBrushMotorCurrent) {
        this.rearRollingBrushMotorCurrent = rearRollingBrushMotorCurrent;
    }

    public String getRollingBrushMotorFront() {
        return rollingBrushMotorFront;
    }

    public void setRollingBrushMotorFront(String rollingBrushMotorFront) {
        this.rollingBrushMotorFront = rollingBrushMotorFront;
    }

    public String getRollingBrushMotorAfter() {
        return rollingBrushMotorAfter;
    }

    public void setRollingBrushMotorAfter(String rollingBrushMotorAfter) {
        this.rollingBrushMotorAfter = rollingBrushMotorAfter;
    }

    public String getBrushSpinLevel() {
        return brushSpinLevel;
    }

    public void setBrushSpinLevel(String brushSpinLevel) {
        this.brushSpinLevel = brushSpinLevel;
    }

    public String getSideBrushSpinLevel() {
        return sideBrushSpinLevel;
    }

    public void setSideBrushSpinLevel(String sideBrushSpinLevel) {
        this.sideBrushSpinLevel = sideBrushSpinLevel;
    }

    public String getBrushDownPosition() {
        return brushDownPosition;
    }

    public void setBrushDownPosition(String brushDownPosition) {
        this.brushDownPosition = brushDownPosition;
    }

    public String getWaterLevel() {
        return waterLevel;
    }

    public void setWaterLevel(String waterLevel) {
        this.waterLevel = waterLevel;
    }

    public String getLeftBrushSpinLevel() {
        return leftBrushSpinLevel;
    }

    public void setLeftBrushSpinLevel(String leftBrushSpinLevel) {
        this.leftBrushSpinLevel = leftBrushSpinLevel;
    }

    public String getFilterLevel() {
        return filterLevel;
    }

    public void setFilterLevel(String filterLevel) {
        this.filterLevel = filterLevel;
    }

    public String getSprayDetergent() {
        return sprayDetergent;
    }

    public void setSprayDetergent(String sprayDetergent) {
        this.sprayDetergent = sprayDetergent;
    }

    public String getValve() {
        return valve;
    }

    public void setValve(String valve) {
        this.valve = valve;
    }

    public String getCleanWaterLevel() {
        return cleanWaterLevel;
    }

    public void setCleanWaterLevel(String cleanWaterLevel) {
        this.cleanWaterLevel = cleanWaterLevel;
    }

    public String getSewageLevel() {
        return sewageLevel;
    }

    public void setSewageLevel(String sewageLevel) {
        this.sewageLevel = sewageLevel;
    }

    public String getRollingBrushMotorFrontFeedBack() {
        return rollingBrushMotorFrontFeedBack;
    }

    public void setRollingBrushMotorFrontFeedBack(String rollingBrushMotorFrontFeedBack) {
        this.rollingBrushMotorFrontFeedBack = rollingBrushMotorFrontFeedBack;
    }

    public String getRollingBrushMotorAfterFeedBack() {
        return rollingBrushMotorAfterFeedBack;
    }

    public void setRollingBrushMotorAfterFeedBack(String rollingBrushMotorAfterFeedBack) {
        this.rollingBrushMotorAfterFeedBack = rollingBrushMotorAfterFeedBack;
    }

    public String getLeftSideBrushCurrentFeedBack() {
        return leftSideBrushCurrentFeedBack;
    }

    public void setLeftSideBrushCurrentFeedBack(String leftSideBrushCurrentFeedBack) {
        this.leftSideBrushCurrentFeedBack = leftSideBrushCurrentFeedBack;
    }

    public String getRightSideBrushCurrentFeedBack() {
        return rightSideBrushCurrentFeedBack;
    }

    public void setRightSideBrushCurrentFeedBack(String rightSideBrushCurrentFeedBack) {
        this.rightSideBrushCurrentFeedBack = rightSideBrushCurrentFeedBack;
    }

    public String getXdsDriverInfo() {
        return xdsDriverInfo;
    }

    public void setXdsDriverInfo(String xdsDriverInfo) {
        this.xdsDriverInfo = xdsDriverInfo;
    }

    public String getBrushDownPositionFeedBack() {
        return brushDownPositionFeedBack;
    }

    public void setBrushDownPositionFeedBack(String brushDownPositionFeedBack) {
        this.brushDownPositionFeedBack = brushDownPositionFeedBack;
    }

    public String getSuctionPressureVoltage() {
        return suctionPressureVoltage;
    }

    public void setSuctionPressureVoltage(String suctionPressureVoltage) {
        this.suctionPressureVoltage = suctionPressureVoltage;
    }

    public String getLeftSideBrushMotorCurrent() {
        return leftSideBrushMotorCurrent;
    }

    public void setLeftSideBrushMotorCurrent(String leftSideBrushMotorCurrent) {
        this.leftSideBrushMotorCurrent = leftSideBrushMotorCurrent;
    }

    public String getRightSideBrushMotorCurrent() {
        return rightSideBrushMotorCurrent;
    }

    public void setRightSideBrushMotorCurrent(String rightSideBrushMotorCurrent) {
        this.rightSideBrushMotorCurrent = rightSideBrushMotorCurrent;
    }

    public String getSprayMotorCurrent() {
        return sprayMotorCurrent;
    }

    public void setSprayMotorCurrent(String sprayMotorCurrent) {
        this.sprayMotorCurrent = sprayMotorCurrent;
    }

    public String getVacuumMotorCurrent() {
        return vacuumMotorCurrent;
    }

    public void setVacuumMotorCurrent(String vacuumMotorCurrent) {
        this.vacuumMotorCurrent = vacuumMotorCurrent;
    }

    public String getSqueegeeLiftMotorCurrent() {
        return squeegeeLiftMotorCurrent;
    }

    public void setSqueegeeLiftMotorCurrent(String squeegeeLiftMotorCurrent) {
        this.squeegeeLiftMotorCurrent = squeegeeLiftMotorCurrent;
    }

    public String getFilterMotorCurrent() {
        return filterMotorCurrent;
    }

    public void setFilterMotorCurrent(String filterMotorCurrent) {
        this.filterMotorCurrent = filterMotorCurrent;
    }

    public Date getTimeStartupUtc() {
        return timeStartupUtc;
    }

    public void setTimeStartupUtc(Date timeStartupUtc) {
        this.timeStartupUtc = timeStartupUtc;
    }

    public Date getTimeStartupT8() {
        return timeStartupT8;
    }

    public void setTimeStartupT8(Date timeStartupT8) {
        this.timeStartupT8 = timeStartupT8;
    }

    public Date getTimeTaskStartUtc() {
        return timeTaskStartUtc;
    }

    public void setTimeTaskStartUtc(Date timeTaskStartUtc) {
        this.timeTaskStartUtc = timeTaskStartUtc;
    }

    public Date getTimeTaskStartT8() {
        return timeTaskStartT8;
    }

    public void setTimeTaskStartT8(Date timeTaskStartT8) {
        this.timeTaskStartT8 = timeTaskStartT8;
    }

    public Date getTimeCurrentUtc() {
        return timeCurrentUtc;
    }

    public void setTimeCurrentUtc(Date timeCurrentUtc) {
        this.timeCurrentUtc = timeCurrentUtc;
    }

    public Date getTimeCurrentT8() {
        return timeCurrentT8;
    }

    public void setTimeCurrentT8(Date timeCurrentT8) {
        this.timeCurrentT8 = timeCurrentT8;
    }

    public Long getPublishTimestampMs() {
        return publishTimestampMs;
    }

    public void setPublishTimestampMs(Long publishTimestampMs) {
        this.publishTimestampMs = publishTimestampMs;
    }

    public Date getPublishTimeUtc() {
        return publishTimeUtc;
    }

    public void setPublishTimeUtc(Date publishTimeUtc) {
        this.publishTimeUtc = publishTimeUtc;
    }

    public Date getPublishTimeT8() {
        return publishTimeT8;
    }

    public void setPublishTimeT8(Date publishTimeT8) {
        this.publishTimeT8 = publishTimeT8;
    }

    public String getRollingBrushMotorFrontPwmFeedBack() {
        return rollingBrushMotorFrontPwmFeedBack;
    }

    public void setRollingBrushMotorFrontPwmFeedBack(String rollingBrushMotorFrontPwmFeedBack) {
        this.rollingBrushMotorFrontPwmFeedBack = rollingBrushMotorFrontPwmFeedBack;
    }

    public String getRollingBrushMotorAfterPwmFeedBack() {
        return rollingBrushMotorAfterPwmFeedBack;
    }

    public void setRollingBrushMotorAfterPwmFeedBack(String rollingBrushMotorAfterPwmFeedBack) {
        this.rollingBrushMotorAfterPwmFeedBack = rollingBrushMotorAfterPwmFeedBack;
    }

    @Override
    public String toString() {
        return "RobotCornerstone{" +
            "productId='" + productId + '\'' +
            ", reportTimestampMs='" + reportTimestampMs + '\'' +
            ", reportTimeUtc=" + reportTimeUtc +
            ", reportTimeT8=" + reportTimeT8 +
            ", settingRevisionMark='" + settingRevisionMark + '\'' +
            ", protocolVersion='" + protocolVersion + '\'' +
            ", taskSubType='" + taskSubType + '\'' +
            ", taskId='" + taskId + '\'' +
            ", taskRevisionMark='" + taskRevisionMark + '\'' +
            ", collectTimestampMs='" + collectTimestampMs + '\'' +
            ", collectTimeUtc=" + collectTimeUtc +
            ", collectTimeT8=" + collectTimeT8 +
            ", version='" + version + '\'' +
            ", odomPositionX='" + odomPositionX + '\'' +
            ", odomPositionY='" + odomPositionY + '\'' +
            ", odomPositionZ='" + odomPositionZ + '\'' +
            ", odomOrientationX='" + odomOrientationX + '\'' +
            ", odomOrientationY='" + odomOrientationY + '\'' +
            ", odomOrientationZ='" + odomOrientationZ + '\'' +
            ", odomOrientationW='" + odomOrientationW + '\'' +
            ", odomV='" + odomV + '\'' +
            ", odomW='" + odomW + '\'' +
            ", unbiasedImuPryPitch='" + unbiasedImuPryPitch + '\'' +
            ", unbiasedImuPryRoll='" + unbiasedImuPryRoll + '\'' +
            ", timeStartup='" + timeStartup + '\'' +
            ", timeTaskStart='" + timeTaskStart + '\'' +
            ", timeCurrent='" + timeCurrent + '\'' +
            ", wifiIntensityLevel='" + wifiIntensityLevel + '\'' +
            ", mobileIntensityLevel='" + mobileIntensityLevel + '\'' +
            ", wifiTraffic='" + wifiTraffic + '\'' +
            ", mobileTraffic='" + mobileTraffic + '\'' +
            ", wifiSpeed='" + wifiSpeed + '\'' +
            ", wifiSpeedRx='" + wifiSpeedRx + '\'' +
            ", wifiSpeedTx='" + wifiSpeedTx + '\'' +
            ", mobileSpeed='" + mobileSpeed + '\'' +
            ", mobileSpeedRx='" + mobileSpeedRx + '\'' +
            ", mobileSpeedTx='" + mobileSpeedTx + '\'' +
            ", monthTraffic='" + monthTraffic + '\'' +
            ", locationStatus='" + locationStatus + '\'' +
            ", locationMapName='" + locationMapName + '\'' +
            ", locationMapOriginX='" + locationMapOriginX + '\'' +
            ", locationMapOriginY='" + locationMapOriginY + '\'' +
            ", locationMapResolution='" + locationMapResolution + '\'' +
            ", locationMapGridWidth='" + locationMapGridWidth + '\'' +
            ", locationMapGridHeight='" + locationMapGridHeight + '\'' +
            ", locationX='" + locationX + '\'' +
            ", locationY='" + locationY + '\'' +
            ", locationYaw='" + locationYaw + '\'' +
            ", locationX1='" + locationX1 + '\'' +
            ", locationY1='" + locationY1 + '\'' +
            ", locationYaw1='" + locationYaw1 + '\'' +
            ", sleepMode='" + sleepMode + '\'' +
            ", rebooting='" + rebooting + '\'' +
            ", manualControlling='" + manualControlling + '\'' +
            ", rampAssistStatus='" + rampAssistStatus + '\'' +
            ", otaStatus='" + otaStatus + '\'' +
            ", autoMode='" + autoMode + '\'' +
            ", emergencyStop='" + emergencyStop + '\'' +
            ", manualCharging='" + manualCharging + '\'' +
            ", manualWorking='" + manualWorking + '\'' +
            ", wakeupMode='" + wakeupMode + '\'' +
            ", maintainMode='" + maintainMode + '\'' +
            ", schedulerPauseFlags='" + schedulerPauseFlags + '\'' +
            ", schedulerArranger='" + schedulerArranger + '\'' +
            ", scaningMapStatus='" + scaningMapStatus + '\'' +
            ", scaningMapName='" + scaningMapName + '\'' +
            ", recordPathStatus='" + recordPathStatus + '\'' +
            ", recordPathName='" + recordPathName + '\'' +
            ", naviStatus='" + naviStatus + '\'' +
            ", naviInstanceId='" + naviInstanceId + '\'' +
            ", naviMapName='" + naviMapName + '\'' +
            ", naviPosName='" + naviPosName + '\'' +
            ", naviPosType='" + naviPosType + '\'' +
            ", naviPosFunction='" + naviPosFunction + '\'' +
            ", taskStatus='" + taskStatus + '\'' +
            ", taskInstanceId='" + taskInstanceId + '\'' +
            ", multiTaskName='" + multiTaskName + '\'' +
            ", multiTaskListCount='" + multiTaskListCount + '\'' +
            ", multiTaskLoopCount='" + multiTaskLoopCount + '\'' +
            ", taskQueueName='" + taskQueueName + '\'' +
            ", taskQueueListCount='" + taskQueueListCount + '\'' +
            ", taskQueueLoopCount='" + taskQueueLoopCount + '\'' +
            ", taskQueueMapName='" + taskQueueMapName + '\'' +
            ", multiTaskListIndex='" + multiTaskListIndex + '\'' +
            ", multiTaskLoopIndex='" + multiTaskLoopIndex + '\'' +
            ", taskQueueListIndex='" + taskQueueListIndex + '\'' +
            ", taskQueueLoopIndex='" + taskQueueLoopIndex + '\'' +
            ", taskQueueProgress='" + taskQueueProgress + '\'' +
            ", subTaskProgress='" + subTaskProgress + '\'' +
            ", subTaskType='" + subTaskType + '\'' +
            ", taskExpectCleaningType='" + taskExpectCleaningType + '\'' +
            ", taskCurrentCleaningType='" + taskCurrentCleaningType + '\'' +
            ", takeElevatorStatus='" + takeElevatorStatus + '\'' +
            ", takeElevatorFrom='" + takeElevatorFrom + '\'' +
            ", takeElevatorTo='" + takeElevatorTo + '\'' +
            ", takeElevatorState='" + takeElevatorState + '\'' +
            ", stationStatus='" + stationStatus + '\'' +
            ", stationState='" + stationState + '\'' +
            ", stationNumInQueue='" + stationNumInQueue + '\'' +
            ", stationAvailableItems='" + stationAvailableItems + '\'' +
            ", stationSupplyingItems='" + stationSupplyingItems + '\'' +
            ", stationFinishedItems='" + stationFinishedItems + '\'' +
            ", stationPosName='" + stationPosName + '\'' +
            ", stationPosType='" + stationPosType + '\'' +
            ", stationPosFunction='" + stationPosFunction + '\'' +
            ", batteryVoltage='" + batteryVoltage + '\'' +
            ", chargerVoltage='" + chargerVoltage + '\'' +
            ", chargerCurrent='" + chargerCurrent + '\'' +
            ", batteryCurrent='" + batteryCurrent + '\'' +
            ", battery='" + battery + '\'' +
            ", wheelDriverData8='" + wheelDriverData8 + '\'' +
            ", wheelDriverData9='" + wheelDriverData9 + '\'' +
            ", wheelDriverDataE='" + wheelDriverDataE + '\'' +
            ", wheelDriverDataF='" + wheelDriverDataF + '\'' +
            ", wheelDriverData10='" + wheelDriverData10 + '\'' +
            ", wheelDriverData11='" + wheelDriverData11 + '\'' +
            ", wheelDriverData12='" + wheelDriverData12 + '\'' +
            ", wheelDriverData13='" + wheelDriverData13 + '\'' +
            ", hybridDriverData32='" + hybridDriverData32 + '\'' +
            ", hybridDriverData33='" + hybridDriverData33 + '\'' +
            ", hybridDriverData34='" + hybridDriverData34 + '\'' +
            ", hybridDriverData35='" + hybridDriverData35 + '\'' +
            ", hybridDriverData36='" + hybridDriverData36 + '\'' +
            ", hybridDriverData37='" + hybridDriverData37 + '\'' +
            ", hybridDriverData38='" + hybridDriverData38 + '\'' +
            ", hybridDriverData39='" + hybridDriverData39 + '\'' +
            ", rollingBrushMotorWorking='" + rollingBrushMotorWorking + '\'' +
            ", brushMotorWorking='" + brushMotorWorking + '\'' +
            ", leftBrushMotorWorking='" + leftBrushMotorWorking + '\'' +
            ", sprayMotor='" + sprayMotor + '\'' +
            ", fanLevel='" + fanLevel + '\'' +
            ", squeegeeDown='" + squeegeeDown + '\'' +
            ", frontRollingBrushMotorCurrent='" + frontRollingBrushMotorCurrent + '\'' +
            ", rearRollingBrushMotorCurrent='" + rearRollingBrushMotorCurrent + '\'' +
            ", rollingBrushMotorFront='" + rollingBrushMotorFront + '\'' +
            ", rollingBrushMotorAfter='" + rollingBrushMotorAfter + '\'' +
            ", brushSpinLevel='" + brushSpinLevel + '\'' +
            ", sideBrushSpinLevel='" + sideBrushSpinLevel + '\'' +
            ", brushDownPosition='" + brushDownPosition + '\'' +
            ", waterLevel='" + waterLevel + '\'' +
            ", leftBrushSpinLevel='" + leftBrushSpinLevel + '\'' +
            ", filterLevel='" + filterLevel + '\'' +
            ", sprayDetergent='" + sprayDetergent + '\'' +
            ", valve='" + valve + '\'' +
            ", cleanWaterLevel='" + cleanWaterLevel + '\'' +
            ", sewageLevel='" + sewageLevel + '\'' +
            ", rollingBrushMotorFrontFeedBack='" + rollingBrushMotorFrontFeedBack + '\'' +
            ", rollingBrushMotorAfterFeedBack='" + rollingBrushMotorAfterFeedBack + '\'' +
            ", leftSideBrushCurrentFeedBack='" + leftSideBrushCurrentFeedBack + '\'' +
            ", rightSideBrushCurrentFeedBack='" + rightSideBrushCurrentFeedBack + '\'' +
            ", xdsDriverInfo='" + xdsDriverInfo + '\'' +
            ", brushDownPositionFeedBack='" + brushDownPositionFeedBack + '\'' +
            ", suctionPressureVoltage='" + suctionPressureVoltage + '\'' +
            ", leftSideBrushMotorCurrent='" + leftSideBrushMotorCurrent + '\'' +
            ", rightSideBrushMotorCurrent='" + rightSideBrushMotorCurrent + '\'' +
            ", sprayMotorCurrent='" + sprayMotorCurrent + '\'' +
            ", vacuumMotorCurrent='" + vacuumMotorCurrent + '\'' +
            ", squeegeeLiftMotorCurrent='" + squeegeeLiftMotorCurrent + '\'' +
            ", filterMotorCurrent='" + filterMotorCurrent + '\'' +
            ", timeStartupUtc=" + timeStartupUtc +
            ", timeStartupT8=" + timeStartupT8 +
            ", timeTaskStartUtc=" + timeTaskStartUtc +
            ", timeTaskStartT8=" + timeTaskStartT8 +
            ", timeCurrentUtc=" + timeCurrentUtc +
            ", timeCurrentT8=" + timeCurrentT8 +
            ", publishTimestampMs=" + publishTimestampMs +
            ", publishTimeUtc=" + publishTimeUtc +
            ", publishTimeT8=" + publishTimeT8 +
            ", rollingBrushMotorFrontPwmFeedBack='" + rollingBrushMotorFrontPwmFeedBack + '\'' +
            ", rollingBrushMotorAfterPwmFeedBack='" + rollingBrushMotorAfterPwmFeedBack + '\'' +
            '}';
    }
}
