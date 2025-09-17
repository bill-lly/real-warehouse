package com.gs.cloud.warehouse.robot;

import com.aliyun.jindodata.impl.util.StringUtils;
import com.gs.cloud.warehouse.robot.entity.BmsBatteryInfo;
import com.gs.cloud.warehouse.robot.lookup.BmsInfoLookupFunction;
import com.gs.cloud.warehouse.util.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.entity.RobotCornerstone;
import com.gs.cloud.warehouse.robot.entity.RobotCornerStoneRaw;
import com.gs.cloud.warehouse.source.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RobotCornerstoneJob {

  private final static String JOB_NAME = "RobotCornerstoneJob";
  private static final Logger LOG = LoggerFactory.getLogger(RobotCornerstoneJob.class);

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
//    env.getCheckpointConfig().setCheckpointInterval(10*1000);
    DataStream<RobotCornerStoneRaw> dataSource =
        env.fromSource(
            KafkaSourceFactory.getKafkaSource(properties, new RobotCornerStoneRaw(), JOB_NAME),
            WatermarkStrategy.noWatermarks(),
            "robot cornerstone data");

    DataStream<RobotCornerStoneRaw> dataStreamRaw = dataSource.map((MapFunction<RobotCornerStoneRaw, RobotCornerStoneRaw>) value -> {
      JsonNode jsonNodeRobot = value.getxRobot();
      String productId = JsonUtils.jsonNodeGetStrValue(jsonNodeRobot, "sn");
      if (Strings.isBlank(productId)) {
        return null;
      }
      value.setProductId(productId);
      value.setReportTimestampMS(JsonUtils.jsonNodeGetStrValue(jsonNodeRobot, "reportTimestampMS"));
      return value;
    });

    DataStream<Cornerstone> dataStream2 = dataStreamRaw.flatMap((FlatMapFunction<RobotCornerStoneRaw, Cornerstone>) (value, out) -> {
      JsonNode jsonNodeRobot = value.getxRobot();
      String settingRevisionMark = JsonUtils.jsonNodeGetStrValue(jsonNodeRobot, "settingRevisionMark");
      String protocolVersion = JsonUtils.jsonNodeGetStrValue(jsonNodeRobot, "protocolVersion");
      if (JsonUtils.jsonNodeIsBlank(value.getCollectData())){
        return;
      }
      for (JsonNode node : value.getCollectData()) {
        Cornerstone cornerstone = new Cornerstone();
        cornerstone.setProductId(value.getProductId());
        cornerstone.setReportTimestampMs(value.getReportTimestampMS());
        cornerstone.setSettingRevisionMark(settingRevisionMark);
        cornerstone.setProtocolVersion(protocolVersion);
        cornerstone.setPublishTimestamp(value.getPublishTimestamp());
        cornerstone.setCollectData(node.toString());
        out.collect(cornerstone);
      }
    }, TypeInformation.of(Cornerstone.class));


    DataStream<RobotCornerstone> dataStream = toRobotCornerstone(dataStream2);
    DataStream<RobotCornerstone> sinkStream = dataStream.filter(
            (FilterFunction<RobotCornerstone>) robotCornerstone ->
                    robotCornerstone != null && robotCornerstone.getKey() != null);

    DataStream<BmsBatteryInfo> bmsBatteryInfoDataStream = toBmsBatteryInfo(sinkStream)
            .process(new BmsInfoLookupFunction(properties))
            .filter((FilterFunction<BmsBatteryInfo>) bmsBatteryInfo ->
                    StringUtils.isNotBlank(bmsBatteryInfo.getRobotFamilyCode())
                            && StringUtils.isNotBlank(bmsBatteryInfo.getHardwareVersion6())
                            && StringUtils.isNotBlank(bmsBatteryInfo.getHardwareVersion14()));

    sinkStream.sinkTo(KafkaSinkUtils.getKafkaSink(properties, new RobotCornerstone()));
    sinkHudiOds(dataStreamRaw, properties);
    sinkHudiDwd(sinkStream, properties);
    addBmsBatteryInfoSink(properties, bmsBatteryInfoDataStream);
    env.execute(params.get("job.name"));
  }

  private static Properties initKfkProp() {
    Properties properties = new Properties();
    properties.put("request.timeout.ms", 600000);
    return properties;
  }

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("job.name"), "job.name can not be null");
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }

  public static void sinkHudiOds(DataStream<RobotCornerStoneRaw> resultStream, Properties properties) {
    String targetTable = "ods_i_beep_robot_cornerstone";
    String basePath = properties.getProperty("ods.i.beep.robot.cornerstone");

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), basePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.WRITE_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    options.put(FlinkOptions.COMPACTION_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    DataStream<RowData> dataStream = resultStream.map((MapFunction<RobotCornerStoneRaw, RowData>) value -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      String pt = sdf.format(new Date(Long.parseLong(value.getReportTimestampMS())));
      return GenericRowData.of(
          StringData.fromString(value.getProductId()),
          StringData.fromString(value.getReportTimestampMS()),
          StringData.fromString(value.getxRobot().toString()),
          StringData.fromString(value.getCollectData().toString()),
          StringData.fromString(value.getPublishTimestamp()),
          StringData.fromString(pt));
    });

    HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
        .column("product_id STRING")
        .column("report_timestamp_ms STRING")
        .column("x_robot STRING")
        .column("collect_data STRING")
        .column("publish_timestamp_ms STRING")
        .column("pt String")
        .pk("product_id")
        .partition("pt")
        .options(options);
    builder.sink(dataStream, false);
  }


  public static void sinkHudiDwd(DataStream<RobotCornerstone> resultStream, Properties properties) {
    String targetTable = "dwd_i_maint_beep_robot_cornerstone";
    String basePath = properties.getProperty("dwd.i.maint.beep.robot.cornerstone");

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), basePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.WRITE_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    options.put(FlinkOptions.COMPACTION_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    DataStream<RowData> dataStream = resultStream.map((MapFunction<RobotCornerstone, RowData>) value -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      String pt = sdf.format(new Date(Long.parseLong(value.getReportTimestampMs())));
      return GenericRowData.of(
          StringData.fromString(value.getProductId()),
          StringData.fromString(value.getProductId()),
          StringData.fromString(value.getReportTimestampMs()),
          value.getReportTimeUtc() == null ? null : TimestampData.fromEpochMillis(value.getReportTimeUtc().getTime()),
          value.getReportTimeT8() == null ? null : TimestampData.fromEpochMillis(value.getReportTimeT8().getTime()),
          StringData.fromString(value.getSettingRevisionMark()),
          StringData.fromString(value.getProtocolVersion()),
          StringData.fromString(value.getTaskSubType()),
          StringData.fromString(value.getTaskId()),
          StringData.fromString(value.getTaskRevisionMark()),
          StringData.fromString(value.getCollectTimestampMs()),
          value.getCollectTimeUtc() == null ? null : TimestampData.fromEpochMillis(value.getCollectTimeUtc().getTime()),
          value.getCollectTimeT8() == null ? null : TimestampData.fromEpochMillis(value.getCollectTimeT8().getTime()),
          StringData.fromString(value.getVersion()),
          StringData.fromString(value.getOdomPositionX()),
          StringData.fromString(value.getOdomPositionY()),
          StringData.fromString(value.getOdomPositionZ()),
          StringData.fromString(value.getOdomOrientationX()),
          StringData.fromString(value.getOdomOrientationY()),
          StringData.fromString(value.getOdomOrientationZ()),
          StringData.fromString(value.getOdomOrientationW()),
          StringData.fromString(value.getOdomV()),
          StringData.fromString(value.getOdomW()),
          StringData.fromString(value.getUnbiasedImuPryPitch()),
          StringData.fromString(value.getUnbiasedImuPryRoll()),
          StringData.fromString(value.getTimeStartup()),
          StringData.fromString(value.getTimeTaskStart()),
          StringData.fromString(value.getTimeCurrent()),
          StringData.fromString(value.getWifiIntensityLevel()),
          StringData.fromString(value.getMobileIntensityLevel()),
          StringData.fromString(value.getWifiTraffic()),
          StringData.fromString(value.getMobileTraffic()),
          StringData.fromString(value.getWifiSpeed()),
          StringData.fromString(value.getWifiSpeedRx()),
          StringData.fromString(value.getWifiSpeedTx()),
          StringData.fromString(value.getMobileSpeed()),
          StringData.fromString(value.getMobileSpeedRx()),
          StringData.fromString(value.getMobileSpeedTx()),
          StringData.fromString(value.getLocationStatus()),
          StringData.fromString(value.getLocationMapName()),
          StringData.fromString(value.getLocationMapOriginX()),
          StringData.fromString(value.getLocationMapOriginY()),
          StringData.fromString(value.getLocationMapResolution()),
          StringData.fromString(value.getLocationMapGridWidth()),
          StringData.fromString(value.getLocationMapGridHeight()),
          StringData.fromString(value.getLocationX()),
          StringData.fromString(value.getLocationY()),
          StringData.fromString(value.getLocationYaw()),
          StringData.fromString(value.getLocationX1()),
          StringData.fromString(value.getLocationY1()),
          StringData.fromString(value.getLocationYaw1()),
          StringData.fromString(value.getSleepMode()),
          StringData.fromString(value.getRebooting()),
          StringData.fromString(value.getManualControlling()),
          StringData.fromString(value.getRampAssistStatus()),
          StringData.fromString(value.getOtaStatus()),
          StringData.fromString(value.getAutoMode()),
          StringData.fromString(value.getEmergencyStop()),
          StringData.fromString(value.getManualCharging()),
          StringData.fromString(value.getManualWorking()),
          StringData.fromString(value.getWakeupMode()),
          StringData.fromString(value.getMaintainMode()),
          StringData.fromString(value.getSchedulerPauseFlags()),
          StringData.fromString(value.getSchedulerArranger()),
          StringData.fromString(value.getScaningMapStatus()),
          StringData.fromString(value.getScaningMapName()),
          StringData.fromString(value.getRecordPathStatus()),
          StringData.fromString(value.getRecordPathName()),
          StringData.fromString(value.getNaviStatus()),
          StringData.fromString(value.getNaviInstanceId()),
          StringData.fromString(value.getNaviMapName()),
          StringData.fromString(value.getNaviPosName()),
          StringData.fromString(value.getNaviPosType()),
          StringData.fromString(value.getNaviPosFunction()),
          StringData.fromString(value.getTaskStatus()),
          StringData.fromString(value.getTaskInstanceId()),
          StringData.fromString(value.getMultiTaskName()),
          StringData.fromString(value.getMultiTaskListCount()),
          StringData.fromString(value.getMultiTaskLoopCount()),
          StringData.fromString(value.getTaskQueueName()),
          StringData.fromString(value.getTaskQueueListCount()),
          StringData.fromString(value.getTaskQueueLoopCount()),
          StringData.fromString(value.getTaskQueueMapName()),
          StringData.fromString(value.getMultiTaskListIndex()),
          StringData.fromString(value.getMultiTaskLoopIndex()),
          StringData.fromString(value.getTaskQueueListIndex()),
          StringData.fromString(value.getTaskQueueLoopIndex()),
          StringData.fromString(value.getTaskQueueProgress()),
          StringData.fromString(value.getSubTaskProgress()),
          StringData.fromString(value.getSubTaskType()),
          StringData.fromString(value.getTaskExpectCleaningType()),
          StringData.fromString(value.getTaskCurrentCleaningType()),
          StringData.fromString(value.getTakeElevatorStatus()),
          StringData.fromString(value.getTakeElevatorFrom()),
          StringData.fromString(value.getTakeElevatorTo()),
          StringData.fromString(value.getTakeElevatorState()),
          StringData.fromString(value.getStationStatus()),
          StringData.fromString(value.getStationState()),
          StringData.fromString(value.getStationNumInQueue()),
          StringData.fromString(value.getStationAvailableItems()),
          StringData.fromString(value.getStationSupplyingItems()),
          StringData.fromString(value.getStationFinishedItems()),
          StringData.fromString(value.getStationPosName()),
          StringData.fromString(value.getStationPosType()),
          StringData.fromString(value.getStationPosFunction()),
          StringData.fromString(value.getBatteryVoltage()),
          StringData.fromString(value.getChargerVoltage()),
          StringData.fromString(value.getChargerCurrent()),
          StringData.fromString(value.getBatteryCurrent()),
          StringData.fromString(value.getBattery()),
          StringData.fromString(value.getWheelDriverData8()),
          StringData.fromString(value.getWheelDriverData9()),
          StringData.fromString(value.getWheelDriverDataE()),
          StringData.fromString(value.getWheelDriverDataF()),
          StringData.fromString(value.getWheelDriverData10()),
          StringData.fromString(value.getWheelDriverData11()),
          StringData.fromString(value.getWheelDriverData12()),
          StringData.fromString(value.getWheelDriverData13()),
          StringData.fromString(value.getHybridDriverData32()),
          StringData.fromString(value.getHybridDriverData33()),
          StringData.fromString(value.getHybridDriverData34()),
          StringData.fromString(value.getHybridDriverData35()),
          StringData.fromString(value.getHybridDriverData36()),
          StringData.fromString(value.getHybridDriverData37()),
          StringData.fromString(value.getHybridDriverData38()),
          StringData.fromString(value.getHybridDriverData39()),
          StringData.fromString(value.getRollingBrushMotorWorking()),
          StringData.fromString(value.getBrushMotorWorking()),
          StringData.fromString(value.getLeftBrushMotorWorking()),
          StringData.fromString(value.getSprayMotor()),
          StringData.fromString(value.getFanLevel()),
          StringData.fromString(value.getSqueegeeDown()),
          StringData.fromString(value.getFrontRollingBrushMotorCurrent()),
          StringData.fromString(value.getRearRollingBrushMotorCurrent()),
          StringData.fromString(value.getRollingBrushMotorFront()),
          StringData.fromString(value.getRollingBrushMotorAfter()),
          StringData.fromString(value.getBrushSpinLevel()),
          StringData.fromString(value.getSideBrushSpinLevel()),
          StringData.fromString(value.getBrushDownPosition()),
          StringData.fromString(value.getWaterLevel()),
          StringData.fromString(value.getLeftBrushSpinLevel()),
          StringData.fromString(value.getFilterLevel()),
          StringData.fromString(value.getSprayDetergent()),
          StringData.fromString(value.getValve()),
          StringData.fromString(value.getCleanWaterLevel()),
          StringData.fromString(value.getSewageLevel()),
          StringData.fromString(value.getRollingBrushMotorFrontFeedBack()),
          StringData.fromString(value.getRollingBrushMotorAfterFeedBack()),
          StringData.fromString(value.getLeftSideBrushCurrentFeedBack()),
          StringData.fromString(value.getRightSideBrushCurrentFeedBack()),
          StringData.fromString(value.getXdsDriverInfo()),
          StringData.fromString(value.getBrushDownPositionFeedBack()),
          StringData.fromString(value.getSuctionPressureVoltage()),
          StringData.fromString(value.getLeftSideBrushMotorCurrent()),
          StringData.fromString(value.getRightSideBrushMotorCurrent()),
          StringData.fromString(value.getSprayMotorCurrent()),
          StringData.fromString(value.getVacuumMotorCurrent()),
          StringData.fromString(value.getSqueegeeLiftMotorCurrent()),
          StringData.fromString(value.getFilterMotorCurrent()),
          value.getTimeStartupUtc() == null ? null : TimestampData.fromEpochMillis(value.getTimeStartupUtc().getTime()),
          value.getTimeStartupT8() == null ? null : TimestampData.fromEpochMillis(value.getTimeStartupT8().getTime()),
          value.getTimeTaskStartUtc() == null ? null : TimestampData.fromEpochMillis(value.getTimeTaskStartUtc().getTime()),
          value.getTimeTaskStartT8() == null ? null : TimestampData.fromEpochMillis(value.getTimeTaskStartT8().getTime()),
          value.getTimeCurrentUtc() == null ? null : TimestampData.fromEpochMillis(value.getTimeCurrentUtc().getTime()),
          value.getTimeCurrentT8() == null ? null : TimestampData.fromEpochMillis(value.getTimeCurrentT8().getTime()),
          value.getPublishTimestampMs(),
          value.getPublishTimeUtc() == null ? null : StringData.fromString(sdf1.format(value.getPublishTimeUtc())),
          value.getPublishTimeT8() == null ? null : StringData.fromString(sdf1.format(value.getPublishTimeT8())),
          StringData.fromString(value.getRollingBrushMotorFrontPwmFeedBack()),
          StringData.fromString(value.getRollingBrushMotorAfterPwmFeedBack()),
          StringData.fromString(value.getMonthTraffic()),
          StringData.fromString(value.getBattBalanceStatus()),
          StringData.fromString(value.getBattBmsStatus()),
          StringData.fromString(value.getBattCycleTimes()),
          StringData.fromString(value.getBattFullCap()),
          StringData.fromString(value.getBattHwVer()),
          StringData.fromString(value.getBattMcuE44()),
          StringData.fromString(value.getBattProtectorStatus()),
          StringData.fromString(value.getBattRebootTimes()),
          StringData.fromString(value.getBattRemainCap()),
          StringData.fromString(value.getBattSoh()),
          StringData.fromString(value.getBattSwVer()),
          StringData.fromString(value.getBattTemp1()),
          StringData.fromString(value.getBattTemp2()),
          StringData.fromString(value.getBattTemp3()),
          StringData.fromString(value.getBattTemp4()),
          StringData.fromString(value.getBattTemp5()),
          StringData.fromString(value.getBattTemp6()),
          StringData.fromString(value.getBattTemp7()),
          StringData.fromString(value.getBattTotalCap()),
          StringData.fromString(value.getBattTotalRunTime()),
          StringData.fromString(value.getBattVolt1()),
          StringData.fromString(value.getBattVolt2()),
          StringData.fromString(value.getBattVolt3()),
          StringData.fromString(value.getBattVolt4()),
          StringData.fromString(value.getBattVolt5()),
          StringData.fromString(value.getBattVolt6()),
          StringData.fromString(value.getBattVolt7()),
          StringData.fromString(value.getBattVolt8()),
          StringData.fromString(value.getBattVolt9()),
          StringData.fromString(value.getBattVolt10()),
          StringData.fromString(value.getBattVolt11()),
          StringData.fromString(value.getBattVolt12()),
          StringData.fromString(value.getBattVolt13()),
          StringData.fromString(value.getBattVolt14()),
          StringData.fromString(value.getBattVolt15()),
          StringData.fromString(value.getWmActualSpeedL()),
          StringData.fromString(value.getWmActualSpeedR()),
          StringData.fromString(value.getWmBusVolt()),
          StringData.fromString(value.getWmCountsL()),
          StringData.fromString(value.getWmCountsR()),
          StringData.fromString(value.getWmCurrentL()),
          StringData.fromString(value.getWmCurrentR()),
          StringData.fromString(value.getWmMcuE42()),
          StringData.fromString(value.getWmMcuE45()),
          StringData.fromString(value.getWmMcuE46()),
          StringData.fromString(value.getWmMcuE47()),
          StringData.fromString(value.getWmRefSpeedL()),
          StringData.fromString(value.getWmRefSpeedR()),
          StringData.fromString(value.getWmTempL()),
          StringData.fromString(value.getWmTempR()),
          StringData.fromString(value.getFmMcuE27()),
          StringData.fromString(value.getFmVacuumDriverTemp()),
          StringData.fromString(value.getFmVacuumSpeed()),
          StringData.fromString(value.getFmVacuumTemp()),
          StringData.fromString(value.getFmCurrent()),
          StringData.fromString(value.getHmBrushDown()),
          StringData.fromString(value.getHmBrushLiftMotorCurrent()),
          StringData.fromString(value.getHmFilterMotor()),
          StringData.fromString(value.getHmMcuE37()),
          StringData.fromString(value.getHmMcuE48()),
          StringData.fromString(value.getHmOutletValve()),
          StringData.fromString(value.getHmPowerBoardBusVolt()),
          StringData.fromString(value.getHmRollingBrushPressureLevel()),
          StringData.fromString(value.getHmRollingBrushSpinLevel()),
          StringData.fromString(value.getHmBrushMotorCurrent()),
          StringData.fromString(value.getLldRelay()),
          StringData.fromString(pt));
    });

    HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
        .column("id STRING")
        .column("product_id STRING")
        .column("report_timestamp_ms STRING")
        .column("report_time_utc TIMESTAMP")
        .column("report_time_t8 TIMESTAMP")
        .column("setting_revision_mark STRING")
        .column("protocol_version STRING")
        .column("task_sub_type STRING")
        .column("task_id STRING")
        .column("task_revision_mark STRING")
        .column("collect_timestamp_ms STRING")
        .column("collect_time_utc TIMESTAMP")
        .column("collect_time_t8 TIMESTAMP")
        .column("version STRING")
        .column("odom_position_x STRING")
        .column("odom_position_y STRING")
        .column("odom_position_z STRING")
        .column("odom_orientation_x STRING")
        .column("odom_orientation_y STRING")
        .column("odom_orientation_z STRING")
        .column("odom_orientation_w STRING")
        .column("odom_v STRING")
        .column("odom_w STRING")
        .column("unbiased_imu_pry_pitch STRING")
        .column("unbiased_imu_pry_roll STRING")
        .column("time_startup STRING")
        .column("time_task_start STRING")
        .column("time_current STRING")
        .column("wifi_intensity_level STRING")
        .column("mobile_intensity_level STRING")
        .column("wifi_traffic STRING")
        .column("mobile_traffic STRING")
        .column("wifi_speed STRING")
        .column("wifi_speed_rx STRING")
        .column("wifi_speed_tx STRING")
        .column("mobile_speed STRING")
        .column("mobile_speed_rx STRING")
        .column("mobile_speed_tx STRING")
        .column("location_status STRING")
        .column("location_map_name STRING")
        .column("location_map_origin_x STRING")
        .column("location_map_origin_y STRING")
        .column("location_map_resolution STRING")
        .column("location_map_grid_width STRING")
        .column("location_map_grid_height STRING")
        .column("location_x STRING")
        .column("location_y STRING")
        .column("location_yaw STRING")
        .column("location_x1 STRING")
        .column("location_y1 STRING")
        .column("location_yaw1 STRING")
        .column("sleep_mode STRING")
        .column("rebooting STRING")
        .column("manual_controlling STRING")
        .column("ramp_assist_status STRING")
        .column("ota_status STRING")
        .column("auto_mode STRING")
        .column("emergency_stop STRING")
        .column("manual_charging STRING")
        .column("manual_working STRING")
        .column("wakeup_mode STRING")
        .column("maintain_mode STRING")
        .column("scheduler_pause_flags STRING")
        .column("scheduler_arranger STRING")
        .column("scaning_map_status STRING")
        .column("scaning_map_name STRING")
        .column("record_path_status STRING")
        .column("record_path_name STRING")
        .column("navi_status STRING")
        .column("navi_instance_id STRING")
        .column("navi_map_name STRING")
        .column("navi_pos_name STRING")
        .column("navi_pos_type STRING")
        .column("navi_pos_function STRING")
        .column("task_status STRING")
        .column("task_instance_id STRING")
        .column("multi_task_name STRING")
        .column("multi_task_list_count STRING")
        .column("multi_task_loop_count STRING")
        .column("task_queue_name STRING")
        .column("task_queue_list_count STRING")
        .column("task_queue_loop_count STRING")
        .column("task_queue_map_name STRING")
        .column("multi_task_list_index STRING")
        .column("multi_task_loop_index STRING")
        .column("task_queue_list_index STRING")
        .column("task_queue_loop_index STRING")
        .column("task_queue_progress STRING")
        .column("sub_task_progress STRING")
        .column("sub_task_type STRING")
        .column("task_expect_cleaning_type STRING")
        .column("task_current_cleaning_type STRING")
        .column("take_elevator_status STRING")
        .column("take_elevator_from STRING")
        .column("take_elevator_to STRING")
        .column("take_elevator_state STRING")
        .column("station_status STRING")
        .column("station_state STRING")
        .column("station_num_in_queue STRING")
        .column("station_available_items STRING")
        .column("station_supplying_items STRING")
        .column("station_finished_items STRING")
        .column("station_pos_name STRING")
        .column("station_pos_type STRING")
        .column("station_pos_function STRING")
        .column("battery_voltage STRING")
        .column("charger_voltage STRING")
        .column("charger_current STRING")
        .column("battery_current STRING")
        .column("battery STRING")
        .column("wheel_driver_data8 STRING")
        .column("wheel_driver_data9 STRING")
        .column("wheel_driver_datae STRING")
        .column("wheel_driver_dataf STRING")
        .column("wheel_driver_data10 STRING")
        .column("wheel_driver_data11 STRING")
        .column("wheel_driver_data12 STRING")
        .column("wheel_driver_data13 STRING")
        .column("hybrid_driver_data32 STRING")
        .column("hybrid_driver_data33 STRING")
        .column("hybrid_driver_data34 STRING")
        .column("hybrid_driver_data35 STRING")
        .column("hybrid_driver_data36 STRING")
        .column("hybrid_driver_data37 STRING")
        .column("hybrid_driver_data38 STRING")
        .column("hybrid_driver_data39 STRING")
        .column("rolling_brush_motor_working STRING")
        .column("brush_motor_working STRING")
        .column("left_brush_motor_working STRING")
        .column("spray_motor STRING")
        .column("fan_level STRING")
        .column("squeegee_down STRING")
        .column("front_rolling_brush_motor_current STRING")
        .column("rear_rolling_brush_motor_current STRING")
        .column("rolling_brush_motor_front STRING")
        .column("rolling_brush_motor_after STRING")
        .column("brush_spin_level STRING")
        .column("side_brush_spin_level STRING")
        .column("brush_down_position STRING")
        .column("water_level STRING")
        .column("left_brush_spin_level STRING")
        .column("filter_level STRING")
        .column("spray_detergent STRING")
        .column("valve STRING")
        .column("clean_water_level STRING")
        .column("sewage_level STRING")
        .column("rolling_brush_motor_front_feed_back STRING")
        .column("rolling_brush_motor_after_feed_back STRING")
        .column("left_side_brush_current_feed_back STRING")
        .column("right_side_brush_current_feed_back STRING")
        .column("xds_driver_info STRING")
        .column("brush_down_position_feed_back STRING")
        .column("suction_pressure_voltage STRING")
        .column("left_side_brush_motor_current STRING")
        .column("right_side_brush_motor_current STRING")
        .column("spray_motor_current STRING")
        .column("vacuum_motor_current STRING")
        .column("squeegee_lift_motor_current STRING")
        .column("filter_motor_current STRING")
        .column("time_startup_utc TIMESTAMP")
        .column("time_startup_t8 TIMESTAMP")
        .column("time_task_start_utc TIMESTAMP")
        .column("time_task_start_t8 TIMESTAMP")
        .column("time_current_utc TIMESTAMP")
        .column("time_current_t8 TIMESTAMP")
        .column("publish_timestamp_ms BIGINT")
        .column("publish_time_utc STRING")
        .column("publish_time_t8 STRING")
        .column("rolling_brush_motor_front_pwm_feed_back STRING")
        .column("rolling_brush_motor_after_pwm_feed_back STRING")
        .column("month_traffic STRING")
        .column("batt_balance_status STRING")
        .column("batt_bms_status STRING")
        .column("batt_cycle_times STRING")
        .column("batt_full_cap STRING")
        .column("batt_hw_ver STRING")
        .column("batt_mcu_e44 STRING")
        .column("batt_protector_status STRING")
        .column("batt_reboot_times STRING")
        .column("batt_remain_cap STRING")
        .column("batt_soh STRING")
        .column("batt_sw_ver STRING")
        .column("batt_temp1 STRING")
        .column("batt_temp2 STRING")
        .column("batt_temp3 STRING")
        .column("batt_temp4 STRING")
        .column("batt_temp5 STRING")
        .column("batt_temp6 STRING")
        .column("batt_temp7 STRING")
        .column("batt_total_cap STRING")
        .column("batt_total_run_time STRING")
        .column("batt_volt1 STRING")
        .column("batt_volt2 STRING")
        .column("batt_volt3 STRING")
        .column("batt_volt4 STRING")
        .column("batt_volt5 STRING")
        .column("batt_volt6 STRING")
        .column("batt_volt7 STRING")
        .column("batt_volt8 STRING")
        .column("batt_volt9 STRING")
        .column("batt_volt10 STRING")
        .column("batt_volt11 STRING")
        .column("batt_volt12 STRING")
        .column("batt_volt13 STRING")
        .column("batt_volt14 STRING")
        .column("batt_volt15 STRING")
        .column("wm_actual_speed_l STRING")
        .column("wm_actual_speed_r STRING")
        .column("wm_bus_volt STRING")
        .column("wm_counts_l STRING")
        .column("wm_counts_r STRING")
        .column("wm_current_l STRING")
        .column("wm_current_r STRING")
        .column("wm_mcu_e42 STRING")
        .column("wm_mcu_e45 STRING")
        .column("wm_mcu_e46 STRING")
        .column("wm_mcu_e47 STRING")
        .column("wm_ref_speed_l STRING")
        .column("wm_ref_speed_r STRING")
        .column("wm_temp_l STRING")
        .column("wm_temp_r STRING")
        .column("fm_mcu_e27 STRING")
        .column("fm_vacuum_driver_temp STRING")
        .column("fm_vacuum_speed STRING")
        .column("fm_vacuum_temp STRING")
        .column("fm_current STRING")
        .column("hm_brush_down STRING")
        .column("hm_brush_lift_motor_current STRING")
        .column("hm_filter_motor STRING")
        .column("hm_mcu_e37 STRING")
        .column("hm_mcu_e48 STRING")
        .column("hm_outlet_valve STRING")
        .column("hm_power_board_bus_volt STRING")
        .column("hm_rolling_brush_pressure_level STRING")
        .column("hm_rolling_brush_spin_level STRING")
        .column("hm_brush_motor_current STRING")
        .column("lld_relay STRING")
        .column("pt STRING")
        .pk("id")
        .partition("pt")
        .options(options);
    builder.sink(dataStream, false);
  }

  public static void  addBmsBatteryInfoSink(Properties properties, DataStream<BmsBatteryInfo> resultStream) {

     resultStream.addSink(JdbcSink.sink(
            "replace into bms_battery_info " +
                    "(product_id,report_time_utc,publish_time_utc,robot_family_code,hardware_version_6,hardware_version_14,battery_voltage,battery_current,batt_full_cap,battery,batt_soh,batt_cycle_times,batt_protector_status,batt_temp1,batt_temp2,batt_temp3,batt_temp4,batt_temp5,batt_temp6,batt_temp7,batt_volt1,batt_volt2,batt_volt3,batt_volt4,batt_volt5,batt_volt6,batt_volt7,batt_volt8,batt_volt9,batt_volt10,batt_volt11,batt_volt12,batt_volt13,batt_volt14,batt_volt15)" +
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (statement, mapping) -> {
              JdbcUtils.setStatementString(statement, 1, mapping.getProductId());
              JdbcUtils.setStatementTimeStamp(statement, 2, new java.sql.Timestamp(mapping.getReportTimeUtc().getTime()));
              JdbcUtils.setStatementTimeStamp(statement, 3, new java.sql.Timestamp(mapping.getPublishTimeUtc().getTime()));
              JdbcUtils.setStatementString(statement, 4, mapping.getRobotFamilyCode());
              JdbcUtils.setStatementString(statement, 5, mapping.getHardwareVersion6());
              JdbcUtils.setStatementString(statement, 6, mapping.getHardwareVersion14());
              JdbcUtils.setStatementLong(statement, 7, mapping.getBatteryVoltage());
              JdbcUtils.setStatementDouble(statement, 8, mapping.getBatteryCurrent());
              JdbcUtils.setStatementLong(statement, 9, mapping.getBattFullCap());
              JdbcUtils.setStatementInt(statement, 10, mapping.getBattery());
              JdbcUtils.setStatementInt(statement, 11, mapping.getBattSoh());
              JdbcUtils.setStatementLong(statement, 12, mapping.getBattCycleTimes());
              JdbcUtils.setStatementInt(statement, 13, mapping.getBattProtectorStatus());
              JdbcUtils.setStatementInt(statement, 14, mapping.getBattTemp1());
              JdbcUtils.setStatementInt(statement, 15, mapping.getBattTemp2());
              JdbcUtils.setStatementInt(statement, 16, mapping.getBattTemp3());
              JdbcUtils.setStatementInt(statement, 17, mapping.getBattTemp4());
              JdbcUtils.setStatementInt(statement, 18, mapping.getBattTemp5());
              JdbcUtils.setStatementInt(statement, 19, mapping.getBattTemp6());
              JdbcUtils.setStatementInt(statement, 20, mapping.getBattTemp7());
              JdbcUtils.setStatementLong(statement, 21, mapping.getBattVolt1());
              JdbcUtils.setStatementLong(statement, 22, mapping.getBattVolt2());
              JdbcUtils.setStatementLong(statement, 23, mapping.getBattVolt3());
              JdbcUtils.setStatementLong(statement, 24, mapping.getBattVolt4());
              JdbcUtils.setStatementLong(statement, 25, mapping.getBattVolt5());
              JdbcUtils.setStatementLong(statement, 26, mapping.getBattVolt5());
              JdbcUtils.setStatementLong(statement, 27, mapping.getBattVolt7());
              JdbcUtils.setStatementLong(statement, 28, mapping.getBattVolt8());
              JdbcUtils.setStatementLong(statement, 29, mapping.getBattVolt9());
              JdbcUtils.setStatementLong(statement, 30, mapping.getBattVolt10());
              JdbcUtils.setStatementLong(statement, 31, mapping.getBattVolt11());
              JdbcUtils.setStatementLong(statement, 32, mapping.getBattVolt12());
              JdbcUtils.setStatementLong(statement, 33, mapping.getBattVolt13());
              JdbcUtils.setStatementLong(statement, 34, mapping.getBattVolt14());
              JdbcUtils.setStatementLong(statement, 35, mapping.getBattVolt15());
            },
            JdbcExecutionOptions.builder()
                    .withBatchSize(5000)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(5)
                    .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(properties.getProperty("quickbi.jdbc.url"))
                    .withDriverName("com.mysql.jdbc.Driver")
                    .withUsername(properties.getProperty("quickbi.jdbc.user.name"))
                    .withPassword(properties.getProperty("quickbi.jdbc.password"))
                    .build()
    )).setParallelism(Integer.parseInt(properties.getProperty("mysqlParallelism", "1")));
  }

  private static DataStream<RobotCornerstone> toRobotCornerstone(DataStream<Cornerstone> dataStream2) {
    return dataStream2.map(new MapFunction<Cornerstone, RobotCornerstone>() {
      private final ObjectMapper objectMapper = new ObjectMapper();
      @Override
      public RobotCornerstone map(Cornerstone value) {
        RobotCornerstone stone = new RobotCornerstone();
        JsonNode root = null;
        try {
          root = objectMapper.readTree(value.getCollectData());
        } catch (JsonProcessingException e) {
          LOG.error(String.format("product_id=%s, data=%s", value.getProductId(), value.getCollectData()), e);
          return null;
        }
        String collectTimestampMS = JsonUtils.jsonNodeGetStrValue(root, "collectTimestampMS");
        String payloadStr = JsonUtils.jsonNodeGetStrValue(root, "payload");
        if (Strings.isEmpty(payloadStr)) {
          return null;
        }
        JsonNode payload = null;
        try {
          payload = objectMapper.readTree(payloadStr);
        } catch (JsonProcessingException e) {
          LOG.error(String.format("product_id=%s, data=%s", value.getProductId(), payloadStr), e);
          return null;
        }
        if (JsonUtils.jsonNodeIsBlank(payload)) {
          return null;
        }

        //common
        stone.setProductId(value.productId);
        stone.setReportTimestampMs(value.reportTimestampMs);
        if (!UnixTimestampUtils.invalid(value.reportTimestampMs)) {
          stone.setReportTimeUtc(new Date(Long.parseLong(value.reportTimestampMs) - 8*60*60*1000));
          stone.setReportTimeT8(new Date(Long.parseLong(value.reportTimestampMs)));
        }
        stone.setCollectTimestampMs(collectTimestampMS);
        if (!UnixTimestampUtils.invalid(collectTimestampMS)) {
          stone.setCollectTimeUtc(new Date(Long.parseLong(collectTimestampMS) - 8*60*60*1000));
          stone.setCollectTimeT8(new Date(Long.parseLong(collectTimestampMS)));
        }
        stone.setPublishTimestampMs(Long.parseLong(value.publishTimestamp));
        if (!UnixTimestampUtils.invalid(value.publishTimestamp)) {
          stone.setPublishTimeUtc(new Date(Long.parseLong(value.publishTimestamp) - 8*60*60*1000));
          stone.setPublishTimeT8(new Date(Long.parseLong(value.publishTimestamp)));
        }
        stone.setSettingRevisionMark(value.settingRevisionMark);
        stone.setProtocolVersion(value.protocolVersion);

        //version
        stone.setVersion(JsonUtils.jsonNodeGetStrValue(payload, "version"));

        //task
        JsonNode task = root.get("task");
        if (!JsonUtils.jsonNodeIsBlank(task)) {
          stone.setTaskSubType(JsonUtils.jsonNodeGetStrValue(task, "taskSubtype"));
          stone.setTaskId(JsonUtils.jsonNodeGetStrValue(task, "taskId"));
          stone.setTaskRevisionMark(JsonUtils.jsonNodeGetStrValue(task, "revisionMark"));
        }

        //odom
        JsonNode odom = payload.path("odom");
        if (!JsonUtils.jsonNodeIsBlank(odom)) {
          stone.setOdomPositionX(JsonUtils.jsonNodeAtStrValue(odom, "/position/x"));
          stone.setOdomPositionY(JsonUtils.jsonNodeAtStrValue(odom, "/position/y"));
          stone.setOdomPositionZ(JsonUtils.jsonNodeAtStrValue(odom, "/position/z"));
          stone.setOdomOrientationX(JsonUtils.jsonNodeAtStrValue(odom, "/orientation/x"));
          stone.setOdomOrientationY(JsonUtils.jsonNodeAtStrValue(odom, "/orientation/y"));
          stone.setOdomOrientationZ(JsonUtils.jsonNodeAtStrValue(odom, "/orientation/z"));
          stone.setOdomOrientationW(JsonUtils.jsonNodeAtStrValue(odom, "/orientation/w"));
          stone.setOdomV(JsonUtils.jsonNodeGetStrValue(odom, "v"));
          stone.setOdomW(JsonUtils.jsonNodeGetStrValue(odom, "w"));
        }

        //unbiasedImuPry
        JsonNode unbiasedImuPry = payload.path("unbiasedImuPry");
        if (!JsonUtils.jsonNodeIsBlank(unbiasedImuPry)) {
          stone.setUnbiasedImuPryPitch(JsonUtils.jsonNodeGetStrValue(unbiasedImuPry, "pitch"));
          stone.setUnbiasedImuPryRoll(JsonUtils.jsonNodeGetStrValue(unbiasedImuPry, "roll"));
        }

        //time
        JsonNode time = payload.path("time");
        if (!JsonUtils.jsonNodeIsBlank(time)) {
          String timeStartup = JsonUtils.jsonNodeGetStrValue(time, "startup");
          String timeTaskStart = JsonUtils.jsonNodeGetStrValue(time, "taskStart");
          String timeCurrent = JsonUtils.jsonNodeGetStrValue(time, "current");
          stone.setTimeStartup(timeStartup);
          stone.setTimeTaskStart(timeTaskStart);
          stone.setTimeCurrent(timeCurrent);
          if (!UnixTimestampUtils.invalid(timeStartup)) {
            stone.setTimeStartupUtc(new Date(Long.parseLong(timeStartup) * 1000 - 8*60*60*1000));
            stone.setTimeStartupT8(new Date(Long.parseLong(timeStartup) * 1000));

          }
          if (!UnixTimestampUtils.invalid(timeTaskStart)) {
            stone.setTimeTaskStartUtc(new Date(Long.parseLong(timeTaskStart) * 1000 - 8*60*60*1000));
            stone.setTimeTaskStartT8(new Date(Long.parseLong(timeTaskStart) * 1000));

          }
          if (!UnixTimestampUtils.invalid(timeCurrent)) {
            stone.setTimeCurrentUtc(new Date(Long.parseLong(timeCurrent) * 1000 - 8*60*60*1000));
            stone.setTimeCurrentT8(new Date(Long.parseLong(timeCurrent) * 1000));
          }
        }

        //network
        JsonNode network = payload.path("network");
        if (!JsonUtils.jsonNodeIsBlank(network)) {
          stone.setWifiIntensityLevel(JsonUtils.jsonNodeGetStrValue(network, "wifiIntensityLevel"));
          stone.setMobileIntensityLevel(JsonUtils.jsonNodeGetStrValue(network, "mobileIntensityLevel"));
          stone.setWifiTraffic(JsonUtils.jsonNodeGetStrValue(network, "wifiTraffic"));
          stone.setMobileTraffic(JsonUtils.jsonNodeGetStrValue(network, "mobileTraffic"));
          stone.setWifiSpeed(JsonUtils.jsonNodeGetStrValue(network, "wifiSpeed"));
          stone.setWifiSpeedRx(JsonUtils.jsonNodeGetStrValue(network, "wifiSpeedRX"));
          stone.setWifiSpeedTx(JsonUtils.jsonNodeGetStrValue(network, "wifiSpeedTX"));
          stone.setMobileSpeed(JsonUtils.jsonNodeGetStrValue(network, "mobileSpeed"));
          stone.setMobileSpeedRx(JsonUtils.jsonNodeGetStrValue(network, "mobileSpeedRX"));
          stone.setMobileSpeedTx(JsonUtils.jsonNodeGetStrValue(network, "mobileSpeedTX"));
          stone.setMonthTraffic(JsonUtils.jsonNodeGetStrValue(network, "monthTraffic"));
        }

        //robotStatus
        JsonNode robotStatus = payload.path("robotStatus");
        if (!JsonUtils.jsonNodeIsBlank(robotStatus)) {
          //location
          stone.setLocationStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationStatus"));
          stone.setLocationMapName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationMapName"));
          stone.setLocationMapOriginX(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationMapOriginX"));
          stone.setLocationMapOriginY(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationMapOriginY"));
          stone.setLocationMapResolution(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationMapResolution"));
          stone.setLocationMapGridWidth(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationMapGridWidth"));
          stone.setLocationMapGridHeight(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationMapGridHeight"));
          stone.setLocationX(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationX"));
          stone.setLocationY(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationY"));
          stone.setLocationYaw(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationYaw"));
          stone.setLocationX1(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationX1"));
          stone.setLocationY1(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationY1"));
          stone.setLocationYaw1(JsonUtils.jsonNodeAtStrValue(robotStatus, "/location/locationYaw1"));

          //commonStatus
          stone.setSleepMode(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/sleepMode"));
          stone.setRebooting(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/rebooting"));
          stone.setManualControlling(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/manualControlling"));
          stone.setRampAssistStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/rampAssistStatus"));
          stone.setOtaStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/otaStatus"));
          stone.setAutoMode(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/autoMode"));
          stone.setEmergencyStop(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/emergencyStop"));
          stone.setManualCharging(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/manualCharging"));
          stone.setManualWorking(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/manualWorking"));
          stone.setWakeupMode(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/wakeupMode"));
          stone.setMaintainMode(JsonUtils.jsonNodeAtStrValue(robotStatus, "/commonStatus/maintainMode"));

          //scheduleStatus
          stone.setSchedulerPauseFlags(JsonUtils.jsonNodeAtStrValue(robotStatus, "/scheduleStatus/schedulerPauseFlags"));
          stone.setSchedulerArranger(JsonUtils.jsonNodeAtStrValue(robotStatus, "/scheduleStatus/schedulerArranger"));

          //scanMap
          stone.setScaningMapStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/scanMap/scaningMapStatus"));
          stone.setScaningMapName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/scanMap/scaningMapName"));

          //recordPath
          stone.setRecordPathStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/recordPath/recordPathStatus"));
          stone.setRecordPathName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/recordPath/recordPathName"));

          //navigation
          stone.setNaviStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/navigation/naviStatus"));
          stone.setNaviInstanceId(JsonUtils.jsonNodeAtStrValue(robotStatus, "/navigation/naviInstanceId"));
          stone.setNaviMapName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/navigation/naviMapName"));
          stone.setNaviPosName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/navigation/naviPosName"));
          stone.setNaviPosType(JsonUtils.jsonNodeAtStrValue(robotStatus, "/navigation/naviPosType"));
          stone.setNaviPosFunction(JsonUtils.jsonNodeAtStrValue(robotStatus, "/navigation/naviPosFunction"));

          //task
          stone.setTaskStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskStatus"));
          stone.setTaskInstanceId(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskInstanceId"));
          stone.setMultiTaskName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/multiTaskName"));
          stone.setMultiTaskListCount(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/multiTaskListCount"));
          stone.setMultiTaskLoopCount(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/multiTaskLoopCount"));
          stone.setTaskQueueName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueName"));
          stone.setTaskQueueListCount(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueListCount"));
          stone.setTaskQueueLoopCount(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueLoopCount"));
          stone.setTaskQueueMapName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueMapName"));
          stone.setMultiTaskListIndex(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/multiTaskListIndex"));
          stone.setMultiTaskLoopIndex(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/multiTaskLoopIndex"));
          stone.setTaskQueueListIndex(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueListIndex"));
          stone.setTaskQueueLoopIndex(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueLoopIndex"));
          stone.setTaskQueueProgress(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/taskQueueProgress"));
          stone.setSubTaskProgress(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/subTaskProgress"));
          stone.setSubTaskType(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/subTaskType"));
          stone.setTaskExpectCleaningType(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/expectCleaningType"));
          stone.setTaskCurrentCleaningType(JsonUtils.jsonNodeAtStrValue(robotStatus, "/task/currentCleaningType"));

          //elevator
          stone.setTakeElevatorStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/elevator/takeElevatorStatus"));
          stone.setTakeElevatorFrom(JsonUtils.jsonNodeAtStrValue(robotStatus, "/elevator/takeElevatorFrom"));
          stone.setTakeElevatorTo(JsonUtils.jsonNodeAtStrValue(robotStatus, "/elevator/takeElevatorTo"));
          stone.setTakeElevatorState(JsonUtils.jsonNodeAtStrValue(robotStatus, "/elevator/takeElevatorState"));

          //station
          stone.setStationStatus(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationStatus"));
          stone.setStationState(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationState"));
          stone.setStationNumInQueue(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationNumInQueue"));
          stone.setStationAvailableItems(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationAvailableItems"));
          stone.setStationSupplyingItems(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationSupplyingItems"));
          stone.setStationFinishedItems(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationFinishedItems"));
          stone.setStationPosName(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationPosName"));
          stone.setStationPosType(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationPosType"));
          stone.setStationPosFunction(JsonUtils.jsonNodeAtStrValue(robotStatus, "/station/stationPosFunction"));
        }

        //electronicSystem
        JsonNode electronicSystem = payload.path("electronicSystem");
        if (!JsonUtils.jsonNodeIsBlank(electronicSystem)) {
          stone.setBatteryVoltage(JsonUtils.jsonNodeGetStrValue(electronicSystem, "batteryVoltage"));
          stone.setChargerVoltage(JsonUtils.jsonNodeGetStrValue(electronicSystem, "chargerVoltage"));
          stone.setChargerCurrent(JsonUtils.jsonNodeGetStrValue(electronicSystem, "chargerCurrent"));
          stone.setBatteryCurrent(JsonUtils.jsonNodeGetStrValue(electronicSystem, "batteryCurrent"));
          stone.setBattery(JsonUtils.jsonNodeGetStrValue(electronicSystem, "battery"));
          stone.setWheelDriverData8(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverData8"));
          stone.setWheelDriverData9(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverData9"));
          stone.setWheelDriverDataE(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverDatae"));
          stone.setWheelDriverDataF(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverDataf"));
          stone.setWheelDriverData10(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverData10"));
          stone.setWheelDriverData11(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverData11"));
          stone.setWheelDriverData12(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverData12"));
          stone.setWheelDriverData13(JsonUtils.jsonNodeGetStrValue(electronicSystem, "wheelDriverData13"));
          stone.setHybridDriverData32(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData32"));
          stone.setHybridDriverData33(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData33"));
          stone.setHybridDriverData34(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData34"));
          stone.setHybridDriverData35(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData35"));
          stone.setHybridDriverData36(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData36"));
          stone.setHybridDriverData37(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData37"));
          stone.setHybridDriverData38(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData38"));
          stone.setHybridDriverData39(JsonUtils.jsonNodeGetStrValue(electronicSystem, "hybridDriverData39"));
        }

        //cleaningSystem
        JsonNode cleaningSystem = payload.path("cleaningSystem");
        if (!JsonUtils.jsonNodeIsBlank(cleaningSystem)) {
          stone.setRollingBrushMotorWorking(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorWorking"));
          stone.setBrushMotorWorking(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "brushMotorWorking"));
          stone.setLeftBrushMotorWorking(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "leftBrushMotorWorking"));
          stone.setSprayMotor(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "sprayMotor"));
          stone.setFanLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "fanLevel"));
          stone.setSqueegeeDown(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "squeegeeDown"));
          stone.setFrontRollingBrushMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "frontRollingBrushMotorCurrent"));
          stone.setRearRollingBrushMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rearRollingBrushMotorCurrent"));
          stone.setRollingBrushMotorFront(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorFront"));
          stone.setRollingBrushMotorAfter(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorAfter"));
          stone.setBrushSpinLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "brushSpinLevel"));
          stone.setSideBrushSpinLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "sideBrushSpinLevel"));
          stone.setBrushDownPosition(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "brushDownPosition"));
          stone.setWaterLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "waterLevel"));
          stone.setLeftBrushSpinLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "leftBrushSpinLevel"));
          stone.setFilterLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "filterLevel"));
          stone.setSprayDetergent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "sprayDetergent"));
          stone.setValve(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "valve"));
          stone.setCleanWaterLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "cleanWaterLevel"));
          stone.setSewageLevel(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "sewageLevel"));
          stone.setRollingBrushMotorFrontFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorFrontFeedBack"));
          stone.setRollingBrushMotorAfterFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorAfterFeedBack"));
          stone.setLeftSideBrushCurrentFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "leftSideBrushCurrentFeedBack"));
          stone.setRightSideBrushCurrentFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rightSideBrushCurrentFeedBack"));
          stone.setXdsDriverInfo(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "xdsDriverInfo"));
          stone.setBrushDownPositionFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "brushDownPositionFeedBack"));
          stone.setSuctionPressureVoltage(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "suctionPressureVoltage"));
          stone.setLeftSideBrushMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "leftSideBrushMotorCurrent"));
          stone.setRightSideBrushMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rightSideBrushMotorCurrent"));
          stone.setSprayMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "sprayMotorCurrent"));
          stone.setVacuumMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "vacuumMotorCurrent"));
          stone.setSqueegeeLiftMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "squeegeeLiftMotorCurrent"));
          stone.setFilterMotorCurrent(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "filterMotorCurrent"));
          stone.setRollingBrushMotorFrontPwmFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorFrontPwmFeedBack"));
          stone.setRollingBrushMotorAfterPwmFeedBack(JsonUtils.jsonNodeGetStrValue(cleaningSystem, "rollingBrushMotorAfterPwmFeedBack"));
        }

        //battery
        JsonNode battery = payload.path("battery");
        if (!JsonUtils.jsonNodeIsBlank(battery)) {
          stone.setBattBalanceStatus(JsonUtils.jsonNodeGetStrValue(battery, "balanceStatus"));
          stone.setBattBmsStatus(JsonUtils.jsonNodeGetStrValue(battery, "bmsStatus"));
          stone.setBattCycleTimes(JsonUtils.jsonNodeGetStrValue(battery, "cycleTimes"));
          stone.setBattFullCap(JsonUtils.jsonNodeGetStrValue(battery, "fullCap"));
          stone.setBattHwVer(JsonUtils.jsonNodeGetStrValue(battery, "hwVer"));
          stone.setBattMcuE44(JsonUtils.jsonNodeGetStrValue(battery, "mcuE44"));
          stone.setBattProtectorStatus(JsonUtils.jsonNodeGetStrValue(battery, "protectorStatus"));
          stone.setBattRebootTimes(JsonUtils.jsonNodeGetStrValue(battery, "rebootTimes"));
          stone.setBattRemainCap(JsonUtils.jsonNodeGetStrValue(battery, "remainCap"));
          stone.setBattSoh(JsonUtils.jsonNodeGetStrValue(battery, "soh"));
          stone.setBattSwVer(JsonUtils.jsonNodeGetStrValue(battery, "swVer"));
          stone.setBattTemp1(JsonUtils.jsonNodeGetStrValue(battery, "temp1"));
          stone.setBattTemp2(JsonUtils.jsonNodeGetStrValue(battery, "temp2"));
          stone.setBattTemp3(JsonUtils.jsonNodeGetStrValue(battery, "temp3"));
          stone.setBattTemp4(JsonUtils.jsonNodeGetStrValue(battery, "temp4"));
          stone.setBattTemp5(JsonUtils.jsonNodeGetStrValue(battery, "temp5"));
          stone.setBattTemp6(JsonUtils.jsonNodeGetStrValue(battery, "temp6"));
          stone.setBattTemp7(JsonUtils.jsonNodeGetStrValue(battery, "temp7"));
          stone.setBattTotalCap(JsonUtils.jsonNodeGetStrValue(battery, "totalCap"));
          stone.setBattTotalRunTime(JsonUtils.jsonNodeGetStrValue(battery, "totalRunTime"));
          stone.setBattVolt1(JsonUtils.jsonNodeGetStrValue(battery, "volt1"));
          stone.setBattVolt10(JsonUtils.jsonNodeGetStrValue(battery, "volt10"));
          stone.setBattVolt11(JsonUtils.jsonNodeGetStrValue(battery, "volt11"));
          stone.setBattVolt12(JsonUtils.jsonNodeGetStrValue(battery, "volt12"));
          stone.setBattVolt13(JsonUtils.jsonNodeGetStrValue(battery, "volt13"));
          stone.setBattVolt14(JsonUtils.jsonNodeGetStrValue(battery, "volt14"));
          stone.setBattVolt15(JsonUtils.jsonNodeGetStrValue(battery, "volt15"));
          stone.setBattVolt2(JsonUtils.jsonNodeGetStrValue(battery, "volt2"));
          stone.setBattVolt3(JsonUtils.jsonNodeGetStrValue(battery, "volt3"));
          stone.setBattVolt4(JsonUtils.jsonNodeGetStrValue(battery, "volt4"));
          stone.setBattVolt5(JsonUtils.jsonNodeGetStrValue(battery, "volt5"));
          stone.setBattVolt6(JsonUtils.jsonNodeGetStrValue(battery, "volt6"));
          stone.setBattVolt7(JsonUtils.jsonNodeGetStrValue(battery, "volt7"));
          stone.setBattVolt8(JsonUtils.jsonNodeGetStrValue(battery, "volt8"));
          stone.setBattVolt9(JsonUtils.jsonNodeGetStrValue(battery, "volt9"));
        }

        //wheelMotor
        JsonNode wheelMotor = payload.path("wheelMotor");
        if (!JsonUtils.jsonNodeIsBlank(wheelMotor)) {
          stone.setWmActualSpeedL(JsonUtils.jsonNodeGetStrValue(wheelMotor, "actualSpeedL"));
          stone.setWmActualSpeedR(JsonUtils.jsonNodeGetStrValue(wheelMotor, "actualSpeedR"));
          stone.setWmBusVolt(JsonUtils.jsonNodeGetStrValue(wheelMotor, "busVolt"));
          stone.setWmCountsL(JsonUtils.jsonNodeGetStrValue(wheelMotor, "countsL"));
          stone.setWmCountsR(JsonUtils.jsonNodeGetStrValue(wheelMotor, "countsR"));
          stone.setWmCurrentL(JsonUtils.jsonNodeGetStrValue(wheelMotor, "currentL"));
          stone.setWmCurrentR(JsonUtils.jsonNodeGetStrValue(wheelMotor, "currentR"));
          stone.setWmMcuE42(JsonUtils.jsonNodeGetStrValue(wheelMotor, "mcuE42"));
          stone.setWmMcuE45(JsonUtils.jsonNodeGetStrValue(wheelMotor, "mcuE45"));
          stone.setWmMcuE46(JsonUtils.jsonNodeGetStrValue(wheelMotor, "mcuE46"));
          stone.setWmMcuE47(JsonUtils.jsonNodeGetStrValue(wheelMotor, "mcuE47"));
          stone.setWmRefSpeedL(JsonUtils.jsonNodeGetStrValue(wheelMotor, "refSpeedL"));
          stone.setWmRefSpeedR(JsonUtils.jsonNodeGetStrValue(wheelMotor, "refSpeedR"));
          stone.setWmTempL(JsonUtils.jsonNodeGetStrValue(wheelMotor, "tempL"));
          stone.setWmTempR(JsonUtils.jsonNodeGetStrValue(wheelMotor, "tempR"));
        }

        //fanMotor
        JsonNode fanMotor = payload.path("fanMotor");
        if (!JsonUtils.jsonNodeIsBlank(fanMotor)) {
          stone.setFmMcuE27(JsonUtils.jsonNodeGetStrValue(fanMotor, "mcuE27"));
          stone.setFmVacuumDriverTemp(JsonUtils.jsonNodeGetStrValue(fanMotor, "vacuumDriverTemp"));
          stone.setFmVacuumSpeed(JsonUtils.jsonNodeGetStrValue(fanMotor, "vacuumSpeed"));
          stone.setFmVacuumTemp(JsonUtils.jsonNodeGetStrValue(fanMotor, "vacuumTemp"));
          stone.setFmCurrent(JsonUtils.jsonNodeGetStrValue(fanMotor, "current"));
        }

        //hybridMotor
        JsonNode hybridMotor = payload.path("hybridMotor");
        if (!JsonUtils.jsonNodeIsBlank(hybridMotor)) {
          stone.setHmBrushDown(JsonUtils.jsonNodeGetStrValue(hybridMotor, "brushDown"));
          stone.setHmBrushLiftMotorCurrent(JsonUtils.jsonNodeGetStrValue(hybridMotor, "brushLiftMotorCurrent"));
          stone.setHmFilterMotor(JsonUtils.jsonNodeGetStrValue(hybridMotor, "filterMotor"));
          stone.setHmMcuE37(JsonUtils.jsonNodeGetStrValue(hybridMotor, "mcuE37"));
          stone.setHmMcuE48(JsonUtils.jsonNodeGetStrValue(hybridMotor, "mcuE48"));
          stone.setHmOutletValve(JsonUtils.jsonNodeGetStrValue(hybridMotor, "outletValve"));
          stone.setHmPowerBoardBusVolt(JsonUtils.jsonNodeGetStrValue(hybridMotor, "powerBoardBusVolt"));
          stone.setHmRollingBrushPressureLevel(JsonUtils.jsonNodeGetStrValue(hybridMotor, "rollingBrushPressureLevel"));
          stone.setHmRollingBrushSpinLevel(JsonUtils.jsonNodeGetStrValue(hybridMotor, "rollingBrushSpinLevel"));
          stone.setHmBrushMotorCurrent(JsonUtils.jsonNodeGetStrValue(hybridMotor, "brushMotorCurrent"));
        }

        //lowerLevelDevice
        JsonNode lowerLevelDevice = payload.path("lowerLevelDevice");
        if (!JsonUtils.jsonNodeIsBlank(lowerLevelDevice)) {
          stone.setLldRelay(JsonUtils.jsonNodeGetStrValue(lowerLevelDevice, "relay"));
        }
        return stone;
      }
    });
  }

  private static DataStream<BmsBatteryInfo> toBmsBatteryInfo(DataStream<RobotCornerstone> sinkStream) {

    return sinkStream.flatMap((FlatMapFunction<RobotCornerstone, BmsBatteryInfo>)
            (stone, collector) -> {
      if (Strings.isBlank(stone.getVersion()) || Utils.compare(stone.getVersion(), "1.3.0") < 0) {
          return;
      }
      BmsBatteryInfo info = new BmsBatteryInfo();
      info.setProductId(stone.getProductId());
      info.setReportTimeUtc(stone.getReportTimeUtc());
      info.setPublishTimeUtc(stone.getPublishTimeUtc());
      info.setBatteryVoltage(Utils.parseLong(stone.getBatteryVoltage()));
      info.setBatteryCurrent(Utils.multiply(Utils.parseDouble(stone.getBatteryCurrent()), 1000));
      info.setBattFullCap(Utils.parseLong(stone.getBattFullCap()));
      info.setBattery(Utils.parseInt(stone.getBattery()));
      info.setBattSoh(Utils.parseInt(stone.getBattSoh()));
      info.setBattCycleTimes(Utils.parseLong(stone.getBattCycleTimes()));
      info.setBattProtectorStatus(Utils.parseInt(stone.getBattProtectorStatus()));
      info.setBattTemp1(Utils.parseInt(stone.getBattTemp1()));
      info.setBattTemp2(Utils.parseInt(stone.getBattTemp2()));
      info.setBattTemp3(Utils.parseInt(stone.getBattTemp3()));
      info.setBattTemp4(Utils.parseInt(stone.getBattTemp4()));
      info.setBattTemp5(Utils.parseInt(stone.getBattTemp5()));
      info.setBattTemp6(Utils.parseInt(stone.getBattTemp6()));
      info.setBattTemp7(Utils.parseInt(stone.getBattTemp7()));
      info.setBattVolt1(Utils.parseLong(stone.getBattVolt1()));
      info.setBattVolt2(Utils.parseLong(stone.getBattVolt2()));
      info.setBattVolt3(Utils.parseLong(stone.getBattVolt3()));
      info.setBattVolt4(Utils.parseLong(stone.getBattVolt4()));
      info.setBattVolt5(Utils.parseLong(stone.getBattVolt5()));
      info.setBattVolt6(Utils.parseLong(stone.getBattVolt6()));
      info.setBattVolt7(Utils.parseLong(stone.getBattVolt7()));
      info.setBattVolt8(Utils.parseLong(stone.getBattVolt8()));
      info.setBattVolt9(Utils.parseLong(stone.getBattVolt9()));
      info.setBattVolt10(Utils.parseLong(stone.getBattVolt10()));
      info.setBattVolt11(Utils.parseLong(stone.getBattVolt11()));
      info.setBattVolt12(Utils.parseLong(stone.getBattVolt12()));
      info.setBattVolt13(Utils.parseLong(stone.getBattVolt13()));
      info.setBattVolt14(Utils.parseLong(stone.getBattVolt14()));
      info.setBattVolt15(Utils.parseLong(stone.getBattVolt15()));
      collector.collect(info);
    }, TypeInformation.of(BmsBatteryInfo.class));
  }

  public static class Cornerstone {

    private String productId;
    private String reportTimestampMs;
    private String settingRevisionMark;
    private String protocolVersion;
    private String publishTimestamp;
    private String collectData;

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

    public String getCollectData() {
      return collectData;
    }

    public void setCollectData(String collectData) {
      this.collectData = collectData;
    }

    public String getPublishTimestamp() {
      return publishTimestamp;
    }

    public void setPublishTimestamp(String publishTimestamp) {
      this.publishTimestamp = publishTimestamp;
    }
  }




}
