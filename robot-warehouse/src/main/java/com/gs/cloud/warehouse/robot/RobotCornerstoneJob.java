package com.gs.cloud.warehouse.robot;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.robot.entity.RobotCornerstone;
import com.gs.cloud.warehouse.robot.entity.RobotCornerStoneRaw;
import com.gs.cloud.warehouse.robot.source.KafkaSourceFactory;
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
import com.gs.cloud.warehouse.util.JsonUtils;
import com.gs.cloud.warehouse.util.KafkaSinkUtils;
import com.gs.cloud.warehouse.util.UnixTimestampUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RobotCornerstoneJob {

  private final static String JOB_NAME = "RobotCornerstoneJob";

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

    DataStream<RobotCornerStoneRaw> dataStream1 = dataSource.map((MapFunction<RobotCornerStoneRaw, RobotCornerStoneRaw>) value -> {
      JsonNode jsonNodeRobot = value.getxRobot();
      String productId = JsonUtils.jsonNodeGetValue(jsonNodeRobot, "sn");
      if (Strings.isBlank(productId)) {
        return null;
      }
      value.setProductId(productId);
      value.setReportTimestampMS(JsonUtils.jsonNodeGetValue(jsonNodeRobot, "reportTimestampMS"));
      return value;
    });

    DataStream<Cornerstone> dataStream2 = dataStream1.flatMap((FlatMapFunction<RobotCornerStoneRaw, Cornerstone>) (value, out) -> {
      JsonNode jsonNodeRobot = value.getxRobot();
      String settingRevisionMark = JsonUtils.jsonNodeGetValue(jsonNodeRobot, "settingRevisionMark");
      String protocolVersion = JsonUtils.jsonNodeGetValue(jsonNodeRobot, "protocolVersion");
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


    DataStream<RobotCornerstone> dataStream =  dataStream2.map(new MapFunction<Cornerstone, RobotCornerstone>() {
      private final ObjectMapper objectMapper = new ObjectMapper();
      @Override
      public RobotCornerstone map(Cornerstone value) throws Exception {
        RobotCornerstone stone = new RobotCornerstone();
        JsonNode root = objectMapper.readTree(value.getCollectData());
        String collectTimestampMS = JsonUtils.jsonNodeGetValue(root, "collectTimestampMS");
        String payloadStr = JsonUtils.jsonNodeGetValue(root, "payload");
        if (Strings.isEmpty(payloadStr)) {
          return null;
        }
        JsonNode payload = objectMapper.readTree(payloadStr);
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
        stone.setVersion(JsonUtils.jsonNodeGetValue(payload, "version"));

        //task
        JsonNode task = root.get("task");
        if (!JsonUtils.jsonNodeIsBlank(task)) {
          stone.setTaskSubType(JsonUtils.jsonNodeGetValue(task, "taskSubtype"));
          stone.setTaskId(JsonUtils.jsonNodeGetValue(task, "taskId"));
          stone.setTaskRevisionMark(JsonUtils.jsonNodeGetValue(task, "revisionMark"));
        }

        //odom
        JsonNode odom = payload.path("odom");
        if (!JsonUtils.jsonNodeIsBlank(odom)) {
          stone.setOdomPositionX(JsonUtils.jsonNodeAtValue(odom, "/position/x"));
          stone.setOdomPositionY(JsonUtils.jsonNodeAtValue(odom, "/position/y"));
          stone.setOdomPositionZ(JsonUtils.jsonNodeAtValue(odom, "/position/z"));
          stone.setOdomOrientationX(JsonUtils.jsonNodeAtValue(odom, "/orientation/x"));
          stone.setOdomOrientationY(JsonUtils.jsonNodeAtValue(odom, "/orientation/y"));
          stone.setOdomOrientationZ(JsonUtils.jsonNodeAtValue(odom, "/orientation/z"));
          stone.setOdomOrientationW(JsonUtils.jsonNodeAtValue(odom, "/orientation/w"));
          stone.setOdomV(JsonUtils.jsonNodeGetValue(odom, "v"));
          stone.setOdomW(JsonUtils.jsonNodeGetValue(odom, "w"));
        }

        //unbiasedImuPry
        JsonNode unbiasedImuPry = payload.path("unbiasedImuPry");
        if (!JsonUtils.jsonNodeIsBlank(unbiasedImuPry)) {
          stone.setUnbiasedImuPryPitch(JsonUtils.jsonNodeGetValue(unbiasedImuPry, "pitch"));
          stone.setUnbiasedImuPryRoll(JsonUtils.jsonNodeGetValue(unbiasedImuPry, "roll"));
        }

        //time
        JsonNode time = payload.path("time");
        if (!JsonUtils.jsonNodeIsBlank(time)) {
          String timeStartup = JsonUtils.jsonNodeGetValue(time, "startup");
          String timeTaskStart = JsonUtils.jsonNodeGetValue(time, "taskStart");
          String timeCurrent = JsonUtils.jsonNodeGetValue(time, "current");
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
          stone.setWifiIntensityLevel(JsonUtils.jsonNodeGetValue(network, "wifiIntensityLevel"));
          stone.setMobileIntensityLevel(JsonUtils.jsonNodeGetValue(network, "mobileIntensityLevel"));
          stone.setWifiTraffic(JsonUtils.jsonNodeGetValue(network, "wifiTraffic"));
          stone.setMobileTraffic(JsonUtils.jsonNodeGetValue(network, "mobileTraffic"));
          stone.setWifiSpeed(JsonUtils.jsonNodeGetValue(network, "wifiSpeed"));
          stone.setWifiSpeedRx(JsonUtils.jsonNodeGetValue(network, "wifiSpeedRX"));
          stone.setWifiSpeedTx(JsonUtils.jsonNodeGetValue(network, "wifiSpeedTX"));
          stone.setMobileSpeed(JsonUtils.jsonNodeGetValue(network, "mobileSpeed"));
          stone.setMobileSpeedRx(JsonUtils.jsonNodeGetValue(network, "mobileSpeedRX"));
          stone.setMobileSpeedTx(JsonUtils.jsonNodeGetValue(network, "mobileSpeedTX"));
          stone.setMonthTraffic(JsonUtils.jsonNodeGetValue(network, "monthTraffic"));
        }

        //robotStatus
        JsonNode robotStatus = payload.path("robotStatus");
        if (!JsonUtils.jsonNodeIsBlank(robotStatus)) {
          //location
          stone.setLocationStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationStatus"));
          stone.setLocationMapName(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationMapName"));
          stone.setLocationMapOriginX(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationMapOriginX"));
          stone.setLocationMapOriginY(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationMapOriginY"));
          stone.setLocationMapResolution(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationMapResolution"));
          stone.setLocationMapGridWidth(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationMapGridWidth"));
          stone.setLocationMapGridHeight(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationMapGridHeight"));
          stone.setLocationX(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationX"));
          stone.setLocationY(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationY"));
          stone.setLocationYaw(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationYaw"));
          stone.setLocationX1(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationX1"));
          stone.setLocationY1(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationY1"));
          stone.setLocationYaw1(JsonUtils.jsonNodeAtValue(robotStatus, "/location/locationYaw1"));

          //commonStatus
          stone.setSleepMode(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/sleepMode"));
          stone.setRebooting(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/rebooting"));
          stone.setManualControlling(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/manualControlling"));
          stone.setRampAssistStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/rampAssistStatus"));
          stone.setOtaStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/otaStatus"));
          stone.setAutoMode(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/autoMode"));
          stone.setEmergencyStop(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/emergencyStop"));
          stone.setManualCharging(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/manualCharging"));
          stone.setManualWorking(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/manualWorking"));
          stone.setWakeupMode(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/wakeupMode"));
          stone.setMaintainMode(JsonUtils.jsonNodeAtValue(robotStatus, "/commonStatus/maintainMode"));

          //scheduleStatus
          stone.setSchedulerPauseFlags(JsonUtils.jsonNodeAtValue(robotStatus, "/scheduleStatus/schedulerPauseFlags"));
          stone.setSchedulerArranger(JsonUtils.jsonNodeAtValue(robotStatus, "/scheduleStatus/schedulerArranger"));

          //scanMap
          stone.setScaningMapStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/scanMap/scaningMapStatus"));
          stone.setScaningMapName(JsonUtils.jsonNodeAtValue(robotStatus, "/scanMap/scaningMapName"));

          //recordPath
          stone.setRecordPathStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/recordPath/recordPathStatus"));
          stone.setRecordPathName(JsonUtils.jsonNodeAtValue(robotStatus, "/recordPath/recordPathName"));

          //navigation
          stone.setNaviStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/navigation/naviStatus"));
          stone.setNaviInstanceId(JsonUtils.jsonNodeAtValue(robotStatus, "/navigation/naviInstanceId"));
          stone.setNaviMapName(JsonUtils.jsonNodeAtValue(robotStatus, "/navigation/naviMapName"));
          stone.setNaviPosName(JsonUtils.jsonNodeAtValue(robotStatus, "/navigation/naviPosName"));
          stone.setNaviPosType(JsonUtils.jsonNodeAtValue(robotStatus, "/navigation/naviPosType"));
          stone.setNaviPosFunction(JsonUtils.jsonNodeAtValue(robotStatus, "/navigation/naviPosFunction"));

          //task
          stone.setTaskStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskStatus"));
          stone.setTaskInstanceId(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskInstanceId"));
          stone.setMultiTaskName(JsonUtils.jsonNodeAtValue(robotStatus, "/task/multiTaskName"));
          stone.setMultiTaskListCount(JsonUtils.jsonNodeAtValue(robotStatus, "/task/multiTaskListCount"));
          stone.setMultiTaskLoopCount(JsonUtils.jsonNodeAtValue(robotStatus, "/task/multiTaskLoopCount"));
          stone.setTaskQueueName(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueName"));
          stone.setTaskQueueListCount(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueListCount"));
          stone.setTaskQueueLoopCount(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueLoopCount"));
          stone.setTaskQueueMapName(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueMapName"));
          stone.setMultiTaskListIndex(JsonUtils.jsonNodeAtValue(robotStatus, "/task/multiTaskListIndex"));
          stone.setMultiTaskLoopIndex(JsonUtils.jsonNodeAtValue(robotStatus, "/task/multiTaskLoopIndex"));
          stone.setTaskQueueListIndex(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueListIndex"));
          stone.setTaskQueueLoopIndex(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueLoopIndex"));
          stone.setTaskQueueProgress(JsonUtils.jsonNodeAtValue(robotStatus, "/task/taskQueueProgress"));
          stone.setSubTaskProgress(JsonUtils.jsonNodeAtValue(robotStatus, "/task/subTaskProgress"));
          stone.setSubTaskType(JsonUtils.jsonNodeAtValue(robotStatus, "/task/subTaskType"));
          stone.setTaskExpectCleaningType(JsonUtils.jsonNodeAtValue(robotStatus, "/task/expectCleaningType"));
          stone.setTaskCurrentCleaningType(JsonUtils.jsonNodeAtValue(robotStatus, "/task/currentCleaningType"));

          //elevator
          stone.setTakeElevatorStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/elevator/takeElevatorStatus"));
          stone.setTakeElevatorFrom(JsonUtils.jsonNodeAtValue(robotStatus, "/elevator/takeElevatorFrom"));
          stone.setTakeElevatorTo(JsonUtils.jsonNodeAtValue(robotStatus, "/elevator/takeElevatorTo"));
          stone.setTakeElevatorState(JsonUtils.jsonNodeAtValue(robotStatus, "/elevator/takeElevatorState"));

          //station
          stone.setStationStatus(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationStatus"));
          stone.setStationState(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationState"));
          stone.setStationNumInQueue(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationNumInQueue"));
          stone.setStationAvailableItems(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationAvailableItems"));
          stone.setStationSupplyingItems(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationSupplyingItems"));
          stone.setStationFinishedItems(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationFinishedItems"));
          stone.setStationPosName(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationPosName"));
          stone.setStationPosType(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationPosType"));
          stone.setStationPosFunction(JsonUtils.jsonNodeAtValue(robotStatus, "/station/stationPosFunction"));
        }

        //electronicSystem
        JsonNode electronicSystem = payload.path("electronicSystem");
        if (!JsonUtils.jsonNodeIsBlank(electronicSystem)) {
          stone.setBatteryVoltage(JsonUtils.jsonNodeGetValue(electronicSystem, "batteryVoltage"));
          stone.setChargerVoltage(JsonUtils.jsonNodeGetValue(electronicSystem, "chargerVoltage"));
          stone.setChargerCurrent(JsonUtils.jsonNodeGetValue(electronicSystem, "chargerCurrent"));
          stone.setBatteryCurrent(JsonUtils.jsonNodeGetValue(electronicSystem, "batteryCurrent"));
          stone.setBattery(JsonUtils.jsonNodeGetValue(electronicSystem, "battery"));
          stone.setWheelDriverData8(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverData8"));
          stone.setWheelDriverData9(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverData9"));
          stone.setWheelDriverDataE(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverDatae"));
          stone.setWheelDriverDataF(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverDataf"));
          stone.setWheelDriverData10(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverData10"));
          stone.setWheelDriverData11(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverData11"));
          stone.setWheelDriverData12(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverData12"));
          stone.setWheelDriverData13(JsonUtils.jsonNodeGetValue(electronicSystem, "wheelDriverData13"));
          stone.setHybridDriverData32(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData32"));
          stone.setHybridDriverData33(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData33"));
          stone.setHybridDriverData34(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData34"));
          stone.setHybridDriverData35(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData35"));
          stone.setHybridDriverData36(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData36"));
          stone.setHybridDriverData37(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData37"));
          stone.setHybridDriverData38(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData38"));
          stone.setHybridDriverData39(JsonUtils.jsonNodeGetValue(electronicSystem, "hybridDriverData39"));
        }

        //cleaningSystem
        JsonNode cleaningSystem = payload.path("cleaningSystem");
        if (!JsonUtils.jsonNodeIsBlank(cleaningSystem)) {
          stone.setRollingBrushMotorWorking(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorWorking"));
          stone.setBrushMotorWorking(JsonUtils.jsonNodeGetValue(cleaningSystem, "brushMotorWorking"));
          stone.setLeftBrushMotorWorking(JsonUtils.jsonNodeGetValue(cleaningSystem, "leftBrushMotorWorking"));
          stone.setSprayMotor(JsonUtils.jsonNodeGetValue(cleaningSystem, "sprayMotor"));
          stone.setFanLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "fanLevel"));
          stone.setSqueegeeDown(JsonUtils.jsonNodeGetValue(cleaningSystem, "squeegeeDown"));
          stone.setFrontRollingBrushMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "frontRollingBrushMotorCurrent"));
          stone.setRearRollingBrushMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "rearRollingBrushMotorCurrent"));
          stone.setRollingBrushMotorFront(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorFront"));
          stone.setRollingBrushMotorAfter(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorAfter"));
          stone.setBrushSpinLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "brushSpinLevel"));
          stone.setSideBrushSpinLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "sideBrushSpinLevel"));
          stone.setBrushDownPosition(JsonUtils.jsonNodeGetValue(cleaningSystem, "brushDownPosition"));
          stone.setWaterLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "waterLevel"));
          stone.setLeftBrushSpinLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "leftBrushSpinLevel"));
          stone.setFilterLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "filterLevel"));
          stone.setSprayDetergent(JsonUtils.jsonNodeGetValue(cleaningSystem, "sprayDetergent"));
          stone.setValve(JsonUtils.jsonNodeGetValue(cleaningSystem, "valve"));
          stone.setCleanWaterLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "cleanWaterLevel"));
          stone.setSewageLevel(JsonUtils.jsonNodeGetValue(cleaningSystem, "sewageLevel"));
          stone.setRollingBrushMotorFrontFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorFrontFeedBack"));
          stone.setRollingBrushMotorAfterFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorAfterFeedBack"));
          stone.setLeftSideBrushCurrentFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "leftSideBrushCurrentFeedBack"));
          stone.setRightSideBrushCurrentFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "rightSideBrushCurrentFeedBack"));
          stone.setXdsDriverInfo(JsonUtils.jsonNodeGetValue(cleaningSystem, "xdsDriverInfo"));
          stone.setBrushDownPositionFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "brushDownPositionFeedBack"));
          stone.setSuctionPressureVoltage(JsonUtils.jsonNodeGetValue(cleaningSystem, "suctionPressureVoltage"));
          stone.setLeftSideBrushMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "leftSideBrushMotorCurrent"));
          stone.setRightSideBrushMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "rightSideBrushMotorCurrent"));
          stone.setSprayMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "sprayMotorCurrent"));
          stone.setVacuumMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "vacuumMotorCurrent"));
          stone.setSqueegeeLiftMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "squeegeeLiftMotorCurrent"));
          stone.setFilterMotorCurrent(JsonUtils.jsonNodeGetValue(cleaningSystem, "filterMotorCurrent"));
          stone.setRollingBrushMotorFrontPwmFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorFrontPwmFeedBack"));
          stone.setRollingBrushMotorAfterPwmFeedBack(JsonUtils.jsonNodeGetValue(cleaningSystem, "rollingBrushMotorAfterPwmFeedBack"));
        }
        return stone;
      }
    });
    dataStream.sinkTo(KafkaSinkUtils.getKafkaSink(properties, new RobotCornerstone()));
    sinkHudiOds(dataStream1, properties);
    sinkHudiDwd(dataStream, properties);
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
        .column("pt STRING")
        .pk("id")
        .partition("pt")
        .options(options);
    builder.sink(dataStream, false);
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
