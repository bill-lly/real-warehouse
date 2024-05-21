package com.gs.cloud.warehouse.robot;

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
import com.gs.cloud.warehouse.robot.process.RobotOffsetAvgProcessor;
import com.gs.cloud.warehouse.robot.sink.HudiSinkGS;
import com.gs.cloud.warehouse.robot.sink.JdbcSinkGS;
import com.gs.cloud.warehouse.robot.source.KafkaSourceFactory;
import com.gs.cloud.warehouse.robot.util.Convertor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.KafkaSinkUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
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
        .process(new EventTaskTypeLookupFunction(properties))
        .filter(x-> x.isCreateTicket() || x.isTaskStatus());

    //merge state and incident
    DataStream<BaseEntity> stateIncident = mergeStateIncident(robotStateSource, incidentEventStream);
    //merge displacement
    DataStream<BaseEntity> stateIncidentDis = mergeDisplacement(stateIncident, displacementSource);
    //merge ticket data
    DataStream<BaseEntity> stateIncidentTicket = mergeEventTicket(stateIncidentDis, eventTicketSource);
    //merge work task data
    DataStream<BaseEntity> stateIncidentTicketTask = mergeWorkTask(stateIncidentTicket, workTaskSource);

    //计算机器人时间offset的平均值
    DataStream<RobotTimestamp> avgOffset = timestampSource.keyBy(RobotTimestamp::getKey)
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
        .process(new RobotOffsetAvgProcessor());

    final OutputTag<MonitorWindowStat> outputTag = new OutputTag<MonitorWindowStat>("side-output"){};
    SingleOutputStreamOperator<MonitorResult> result = stateIncidentTicketTask.coGroup(avgOffset)
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
    HudiSinkGS.sinkHudi(sideOutputStream, properties);
    env.execute();
  }

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
        "event ticket data");
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

  private static void validate(ParameterTool params) {
    Preconditions.checkNotNull(params.get("env"), "env can not be null");
  }

}
