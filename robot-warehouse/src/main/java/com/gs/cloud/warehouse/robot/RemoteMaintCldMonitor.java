package com.gs.cloud.warehouse.robot;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.EventTicket;
import com.gs.cloud.warehouse.robot.entity.FiredIncident;
import com.gs.cloud.warehouse.robot.entity.IncidentEvent;
import com.gs.cloud.warehouse.robot.entity.MonitorResult;
import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.RobotDisplacement;
import com.gs.cloud.warehouse.robot.entity.RobotState;
import com.gs.cloud.warehouse.robot.entity.RobotTimestamp;
import com.gs.cloud.warehouse.robot.entity.WorkTask;
import com.gs.cloud.warehouse.robot.lookup.EventTaskTypeLookupFunction;
import com.gs.cloud.warehouse.robot.lookup.RegionInfoLookupFunction;
import com.gs.cloud.warehouse.robot.lookup.RobotInfoLookupFunction;
import com.gs.cloud.warehouse.robot.process.RemoteMaintCldMonitorProcessor;
import com.gs.cloud.warehouse.robot.source.PlanTaskSource;
import com.gs.cloud.warehouse.robot.entity.*;
import com.gs.cloud.warehouse.robot.process.RobotOffsetAvgProcessor;
import com.gs.cloud.warehouse.robot.source.KafkaSourceFactory;
import com.gs.cloud.warehouse.robot.util.Convertor;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.gs.cloud.warehouse.util.KafkaSinkUtils;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RemoteMaintCldMonitor {

  private final static String JOB_NAME = "RemoteMaintCldMonitor";

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    validate(params);
    Properties properties = PropertiesHelper.loadProperties(params);
//    Configuration configuration = new Configuration();
//    configuration.setInteger("rest.port", 8081);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
    // 排班任务数据
    DataStream<RobotPlanTask> robotPlanTask = getRobotPlanTask(env, properties, params);
    // todo 过滤掉老的数据，因为一些核心字段有可能为空
    DataStream<RobotState> robotStateSource = getRobotStateSource(env, properties)
        .filter(RobotState::isNewData);
    DataStream<IncidentEvent> incidentEventSource = getIncidentEventSource(env, properties);
    DataStream<EventTicket> eventTicketSource = getEventTicketSource(env, properties);
    DataStream<WorkTask> workTaskSource = getWorkTaskSource(env, properties);
    DataStream<RobotTimestamp> timestampSource = getRobotTimestampSource(env, properties);
    DataStream<RobotDisplacement> displacementSource = getDisplacementSource(env, properties)
        .filter(value -> value.getKey() != null
            && value.getCldEndTimeUtc() != null
            && value.getEndTimeUtc() != null); //过滤异常数据

    DataStream<IncidentEvent> incidentEventStream = incidentEventSource
        .filter((FilterFunction<IncidentEvent>) value -> value.getKey() != null && value.isFault())
        .process(new EventTaskTypeLookupFunction(properties));

    //merge state and incident
    DataStream<BaseEntity> stateIncident = mergeStateIncident(robotStateSource, incidentEventStream);
    //merge displacement
    DataStream<BaseEntity> stateIncidentDis = mergeDisplacement(stateIncident, displacementSource);
    //merge ticket data
    DataStream<BaseEntity> stateIncidentTicket = mergeEventTicket(stateIncidentDis, eventTicketSource);
    //merge work task data
    DataStream<BaseEntity> stateIncidentTicketTask = mergeWorkTask(stateIncidentTicket, workTaskSource);
    //merge plan task data
    DataStream<BaseEntity> stateIncidentTicketTaskPlanTask = mergeRobotPlanTask(stateIncidentTicketTask,
            robotPlanTask);
    //merge corner stone data
//    DataStream<BaseEntity> stateIncidentTicketTaskStone = mergeCornerStone(stateIncidentTicketTask,
//            robotCornerStoneSource);

    //计算机器人时间offset的平均值
    DataStream<RobotTimestamp> avgOffset = timestampSource.keyBy(RobotTimestamp::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .process(new RobotOffsetAvgProcessor());

    final OutputTag<MonitorWindowStat> outputTag = new OutputTag<MonitorWindowStat>("side-output"){};
    SingleOutputStreamOperator<MonitorResult> result = stateIncidentTicketTaskPlanTask.coGroup(avgOffset)
        .where(BaseEntity::getKey).equalTo(RobotTimestamp::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .apply(new CoGroupFunction<BaseEntity, RobotTimestamp, BaseEntity>() {
          @Override
          public void coGroup(Iterable<BaseEntity> first, Iterable<RobotTimestamp> second, Collector<BaseEntity> out) throws Exception {
            List<BaseEntity> firstList = (List<BaseEntity>)first;
            List<RobotTimestamp> secondList = (List<RobotTimestamp>)second;
            firstList.addAll(secondList);
            Collections.sort(firstList);
            for (BaseEntity ele : firstList) {
              out.collect(ele);
            }
          }
        })
        .keyBy(BaseEntity::getKey)
        .process(new RemoteMaintCldMonitorProcessor(outputTag));
    result.process(new RobotInfoLookupFunction(properties))
        .process(new RegionInfoLookupFunction(properties))
        .map(Convertor::monitorResult2firedIncident)
        .sinkTo(KafkaSinkUtils.getKafkaSink(properties, new FiredIncident()));
    DataStream<MonitorWindowStat> sideOutputStream = result.getSideOutput(outputTag);
    sinkHudi(sideOutputStream, properties);
    env.execute();
  }

  private static DataStream<RobotPlanTask> getRobotPlanTask(StreamExecutionEnvironment env,
                                                            Properties properties,ParameterTool params) {
    return env.addSource(new PlanTaskSource(properties, params), "robot plan task")
            .assignTimestampsAndWatermarks(WatermarkStrategy.<RobotPlanTask>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTime().getTime())
                    .withIdleness(Duration.ofSeconds(60)));
  }
  // 基石数据，暂时不接入
//  private static DataStream<RobotCornerStone> getRobotCornerStone(StreamExecutionEnvironment env,
//                                                                  Properties properties) {
//    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotCornerStone(), JOB_NAME),
//            WatermarkStrategy.<RobotCornerStone>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                    .withTimestampAssigner((event, timestamp) -> event.getCldTimestampUtc().getTime()),
//            "robot corner stone data");
//  }
  private static DataStream<RobotTimestamp> getRobotTimestampSource(StreamExecutionEnvironment env, Properties properties) {
    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotTimestamp(), JOB_NAME),
        WatermarkStrategy.<RobotTimestamp>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner((event, timestamp) -> event.getCldTimestampUtc().getTime()),
        "robot timestamp data");
  }

  private static DataStream<RobotState> getRobotStateSource(StreamExecutionEnvironment env, Properties properties) {
    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotState(), JOB_NAME),
        WatermarkStrategy.<RobotState>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner((event, timestamp) -> event.getCldDate().getTime()),
        "robot state data");
  }

  private static DataStream<IncidentEvent> getIncidentEventSource(StreamExecutionEnvironment env, Properties properties) {
    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new IncidentEvent(), JOB_NAME),
        WatermarkStrategy.<IncidentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner((event, timestamp) -> event.getUpdateTime().getTime()),
        "incident event data");
  }

  private static DataStream<EventTicket> getEventTicketSource(StreamExecutionEnvironment env, Properties properties) {
    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new EventTicket(), JOB_NAME),
        WatermarkStrategy.<EventTicket>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner((event, timestamp) -> event.getUpdateTime().getTime()),
        "event ticket data");
  }

  private static DataStream<WorkTask> getWorkTaskSource(StreamExecutionEnvironment env, Properties properties) {
    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new WorkTask(), JOB_NAME),
        WatermarkStrategy.<WorkTask>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner((event, timestamp) -> event.getUpdateTime().getTime()),
        "work task data");
  }

  private static DataStream<RobotDisplacement> getDisplacementSource(StreamExecutionEnvironment env, Properties properties) {
    return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotDisplacement(), JOB_NAME),
        WatermarkStrategy.<RobotDisplacement>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner((event, timestamp) -> event.getCldEndTimeUtc().getTime()),
        "robot displacement data");
  }

  private static DataStream<BaseEntity> mergeStateIncident(DataStream<RobotState> robotStateSource,
                                                           DataStream<IncidentEvent> incidentEventSource) {
    return robotStateSource.coGroup(incidentEventSource)
        .where(RobotState::getKey).equalTo(IncidentEvent::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .apply(new RichCoGroupFunction<RobotState, IncidentEvent, BaseEntity>() {
          private transient ValueState<String> lastStateState;
          private transient ValueState<String> lastOnlineState;

          @Override
          public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
            ValueStateDescriptor<String> lastStateDescriptor =
                new ValueStateDescriptor<>(
                    "lastStateState",
                    BasicTypeInfo.STRING_TYPE_INFO);
            ValueStateDescriptor<String> lastOnlineDescriptor =
                new ValueStateDescriptor<>(
                    "lastOnlineState",
                    BasicTypeInfo.STRING_TYPE_INFO);
            lastStateDescriptor.enableTimeToLive(ttlConfig);
            lastOnlineDescriptor.enableTimeToLive(ttlConfig);
            lastStateState = getRuntimeContext().getState(lastStateDescriptor);
            lastOnlineState = getRuntimeContext().getState(lastOnlineDescriptor);
            super.open(parameters);
          }

          @Override
          public void coGroup(Iterable<RobotState> first,
                              Iterable<IncidentEvent> second,
                              Collector<BaseEntity> out) throws Exception {
            //下发故障中的告警数据
            List<IncidentEvent> secondList = (List<IncidentEvent>)second;
            for (IncidentEvent ele : secondList) {
              if (ele.isFault()) {
                out.collect(ele);
              }
            }
            //状态数据去重复
            List<RobotState> firstList = (List<RobotState>) first;
            if (firstList.isEmpty()) {
              return;
            }
            Collections.sort(firstList);
            String lastState = lastStateState.value();
            String lastOnline = lastOnlineState.value();
            for (RobotState state : firstList) {
              if (lastState == null) {
                out.collect(Convertor.robotState2workState(state));
                lastState = state.getWorkStatus();
              }
              if (lastOnline == null) {
                out.collect(Convertor.robotState2isOnline(state));
                lastOnline = state.getIsOnlineCode();
              }
              if (lastState != null && !lastState.equals(state.getWorkStatus())) {
                out.collect(Convertor.robotState2workState(state));
                lastState = state.getWorkStatus();
              }
              if (lastOnline != null && !lastOnline.equals(state.getIsOnlineCode())) {
                out.collect(Convertor.robotState2isOnline(state));
                lastOnline = state.getIsOnlineCode();
              }
            }
            lastStateState.update(lastState);
            lastOnlineState.update(lastOnline);
          }
        });
  }

  private static DataStream<BaseEntity> mergeDisplacement(DataStream<BaseEntity> stateIncident,
                                                          DataStream<RobotDisplacement> displacementSource) {
    return stateIncident.coGroup(displacementSource)
        .where(BaseEntity::getKey).equalTo(RobotDisplacement::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .apply(new RichCoGroupFunction<BaseEntity, RobotDisplacement, BaseEntity>() {
          @Override
          public void coGroup(Iterable<BaseEntity> first,
                              Iterable<RobotDisplacement> second,
                              Collector<BaseEntity> out) throws Exception {
            first.forEach(out::collect);
            second.forEach(out::collect);
          }
        });
  }

  private static DataStream<BaseEntity> mergeEventTicket(DataStream<BaseEntity> stateIncident,
                                                         DataStream<EventTicket> eventTicketSource) {
    return stateIncident.coGroup(eventTicketSource)
        .where(BaseEntity::getKey).equalTo(EventTicket::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .apply(new RichCoGroupFunction<BaseEntity, EventTicket, BaseEntity>() {
          private transient ValueState<EventTicket> lastEventTicketState;

          @Override
          public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
            ValueStateDescriptor<EventTicket> lastEventTicketDescriptor =
                new ValueStateDescriptor<>(
                    "lastEventTicketState",
                    TypeInformation.of(EventTicket.class));
            lastEventTicketDescriptor.enableTimeToLive(ttlConfig);
            lastEventTicketState = getRuntimeContext().getState(lastEventTicketDescriptor);
          }

          @Override
          public void coGroup(Iterable<BaseEntity> first,
                              Iterable<EventTicket> second,
                              Collector<BaseEntity> out) throws Exception {
            first.forEach(out::collect);
            List<EventTicket> secondList = (List<EventTicket>)second;
            EventTicket lastTicket = lastEventTicketState.value();
            for (EventTicket ticket:secondList) {
              //只有系统关单的数据才需要处理
              if (!ticket.isSystemClosed()) {
                return;
              }
              if (lastTicket == null) {
                out.collect(ticket);
              } else if (ticket.getTicketNum().equals(lastTicket.getTicketNum())
                  && !ticket.getTicketState().equals(lastTicket.getTicketState())) {
                out.collect(ticket);
              } else if (!ticket.getTicketNum().equals(lastTicket.getTicketNum())) {
                out.collect(ticket);
              }
              lastTicket = ticket;
            }
            lastEventTicketState.update(lastTicket);
          }
        });
  }

  private static DataStream<BaseEntity> mergeWorkTask(DataStream<BaseEntity> stateIncidentTicket,
                                                      DataStream<WorkTask> workTaskSource) {
    return stateIncidentTicket.coGroup(workTaskSource)
        .where(BaseEntity::getKey).equalTo(WorkTask::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .apply(new CoGroupFunction<BaseEntity, WorkTask, BaseEntity>() {
          @Override
          public void coGroup(Iterable<BaseEntity> first,
                              Iterable<WorkTask> second,
                              Collector<BaseEntity> out) throws Exception {
            first.forEach(out::collect);
            List<WorkTask> secondList = (List<WorkTask>)second;
            for (WorkTask workTask:secondList) {
              if (workTask.isIgnored()) {
                out.collect(workTask);
              }
            }
          }
        });
  }

//  private static DataStream<BaseEntity> mergeCornerStone(DataStream<BaseEntity> stateIncident,
//                                                         DataStream<RobotCornerStone> cornerStoneSource) {
//    return stateIncident.coGroup(cornerStoneSource)
//            .where(value -> value.getKey()).equalTo(value -> value.getKey())
//            .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
//            .apply(new RichCoGroupFunction<BaseEntity, RobotCornerStone, BaseEntity>() {
//              @Override
//              public void coGroup(Iterable<BaseEntity> first, Iterable<RobotCornerStone> second,
//                                  Collector<BaseEntity> out) throws Exception {
//                first.forEach(out::collect);
//                second.forEach(out::collect);
//              }
//            });
//  }

  private static DataStream<BaseEntity> mergeRobotPlanTask(DataStream<BaseEntity> MainStream,
                                                           DataStream<RobotPlanTask> robotPlanTask) {
    return MainStream.coGroup(robotPlanTask)
            .where(value -> value.getKey()).equalTo(value -> value.getKey())
            .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
            .apply(new RichCoGroupFunction<BaseEntity, RobotPlanTask, BaseEntity>() {
              @Override
              public void coGroup(Iterable<BaseEntity> first, Iterable<RobotPlanTask> second,
                                  Collector<BaseEntity> out) throws Exception {
                first.forEach(out::collect);
                second.forEach(out::collect);
              }
            });
  }

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }


  public static void sinkHudi(DataStream<MonitorWindowStat> resultStream, Properties properties) {
    String targetTable = "ads_real_monitor_window_stat";
    String basePath = "oss://cloud-emr-prod.cn-shanghai.oss-dls.aliyuncs.com/user/hive/warehouse/gs_real_ads.db/ads_real_monitor_window_stat";
//    String basePath = "oss://cloud-emr-prod.cn-shanghai.oss-dls.aliyuncs.com/user/hive/warehouse/gs_dev_real_ads" +
//            ".db/ads_real_monitor_window_stat";

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), basePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.WRITE_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    options.put(FlinkOptions.COMPACTION_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    DataStream<RowData> dataStream = resultStream.map((MapFunction<MonitorWindowStat, RowData>) value -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      String pt = sdf.format(DateUtils.addHours(value.getStartTimestamp(), 8));
      return GenericRowData.of(
          StringData.fromString(value.getProductId()),
          TimestampData.fromEpochMillis(value.getStartTimestamp().getTime()),
          TimestampData.fromEpochMillis(value.getEndTimestamp().getTime()),
          value.getAvgOffset(),
          value.getIncidentCnt(),
          value.getTotalDisplacement(),
          StringData.fromString(value.getCodeIncidentCnt().toString()),
          StringData.fromString(value.getDurationMap().toString()),
          StringData.fromString(value.getMaxDurationMap().toString()),
          StringData.fromString(value.getWorkStates().toString()),
          StringData.fromString(value.getIncidentMap().toString()),
          StringData.fromString(value.getDisplacementMap().toString()),
          StringData.fromString(pt));
    });

    HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
        .column("product_id STRING")
        .column("start_timestamp timestamp")
        .column("end_timestamp timestamp")
        .column("avg_offset bigint")
        .column("incident_cnt int")
        .column("total_displacement Double")
        .column("code_incident_cnt String")
        .column("duration_map String")
        .column("max_duration_map String")
        .column("state_map String")
        .column("incident_map String")
        .column("displacement_map String")
        .column("pt String")
        .pk("product_id")
        .partition("pt")
        .options(options);
    builder.sink(dataStream, false);
  }

}
