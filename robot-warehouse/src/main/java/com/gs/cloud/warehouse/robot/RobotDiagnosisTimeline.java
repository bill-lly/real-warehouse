package com.gs.cloud.warehouse.robot;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.*;
import com.gs.cloud.warehouse.robot.lookup.IncidentEventInfoLookupFunction;
import com.gs.cloud.warehouse.robot.process.DiagnosisTimelineCoGroupProcessor;
import com.gs.cloud.warehouse.robot.process.DiagnosisTimelineKeyedProcess;
import com.gs.cloud.warehouse.source.KafkaSourceFactory;
import com.gs.cloud.warehouse.robot.source.PlanTaskSource;
import com.gs.cloud.warehouse.util.KafkaSinkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class RobotDiagnosisTimeline {

    private final static String JOB_NAME = "RobotDiagnosisTimeline";
    private static final int TumblingWindowTime = 300;
    private static final int BoundedOutOfOrdernessTime = 180;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        validate(params);
        Properties properties = PropertiesHelper.loadProperties(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 生产注释掉
//        env.setParallelism(1);

        // 排班任务数据
        DataStream<BaseEntity> robotPlanTask = getRobotPlanTask(env, properties, params)
                .filter(value -> !value.getProductId().isEmpty())
                .map(value -> value);

        // 事件单
        DataStream<BaseEntity> eventTicketSource = getEventTicketSource(env, properties)
                .filter(value -> !value.getUpdateTime().after(value.getCreateTime()))
                .map(value -> value);

        // 状态2.0
        DataStream<BaseEntity> robotWorkState2Source = getRobotWorkState2Source(env, properties)
                .filter(value -> !("定位状态".equals(value.getCloudStatusKeyCn()) || "定位正常".equals(value.getCloudStatusValueCn())))
                .map(value -> {
                    value.setCloudStatusKeyCn(value.getCloudStatusKeyCn().replaceAll("一级|二级", ""));
                    return value;
                });
        // 告警
        DataStream<BaseEntity> incidentEventSource = getIncidentEventSource(env, properties)
                .filter(value -> value.getKey() != null)
                .process(new IncidentEventInfoLookupFunction(properties))
                .map(value -> value);

        // todo 状态1.0 过滤掉老的数据，因为一些核心字段有可能为空, 只使用在离线状态
        DataStream<BaseEntity> robotStateSource = getRobotStateSource(env, properties)
                .filter(RobotState::isNewData)
                .map(value -> value);

        // 工作状态，使用状态数据
        DataStream<BaseEntity> robotWorkStateSource = getRobotWorkStateSource(env, properties)
                .map(value -> value);

        DataStream<BaseEntity> robotPlanTaskIncidentEventWorkStateSource = mergeStream(mergeStream(mergeStream(mergeStream(robotPlanTask, incidentEventSource), robotWorkStateSource), eventTicketSource), robotWorkState2Source);

        DataStream<RobotDiagnosisData> resultStream = mergeStateWorkState(robotPlanTaskIncidentEventWorkStateSource, robotStateSource)
                .keyBy(BaseEntity::getKey)
                .process(new DiagnosisTimelineKeyedProcess(properties))
                .map(value -> (RobotDiagnosisData)value);

        resultStream.addSink(sinkToMysql(properties));
        resultStream.sinkTo(KafkaSinkUtils.getKafkaSink(properties, new RobotDiagnosisData()));

        env.execute();
    }

    private static @NotNull SinkFunction<RobotDiagnosisData> sinkToMysql(Properties properties) {
        return JdbcSink.sink(
                "insert into robot_diagnosis_timeline_state (product_id, event_time, cld_timestamp, start_time, attached_task, attached_online_status, attached_work_status, attached_ess_status, state_key_cn, state_value_cn) " +
                        "values (?,?,?,?,?,?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE event_time = VALUES(event_time), cld_timestamp = VALUES(cld_timestamp), start_time = VALUES(start_time), attached_task = VALUES(attached_task), attached_online_status = VALUES(attached_online_status)" +
                        ", attached_work_status = VALUES(attached_work_status), attached_ess_status = VALUES(attached_ess_status), state_key_cn = VALUES(state_key_cn), state_value_cn = VALUES(state_value_cn);"
                ,
                (ps, t) -> {
                    ps.setString(1, t.getProductId());
                    ps.setTimestamp(2, new Timestamp(t.getTimestampUtc().getTime()));
                    ps.setTimestamp(3, new Timestamp(t.getCldTimestampUtc().getTime()));
                    ps.setTimestamp(4, new Timestamp(t.getStartTime() == null ? 0 : t.getStartTime().getTime()));
                    ps.setString(5, t.getAttachedTask());
                    ps.setString(6, t.getAttachedOnlineStatus());
                    ps.setString(7, t.getAttachedWorkStatus());
                    ps.setString(8, t.getAttachedEssStatus());
                    ps.setString(9, "null".equals(t.getStateKeyCn())?null:t.getStateKeyCn());
                    ps.setString(10, "null".equals(t.getStateValueCn())?null:t.getStateValueCn());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(properties.getProperty("robotbi.jdbc.url"))
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(properties.getProperty("robotbi.jdbc.user.name"))
                        .withPassword(properties.getProperty("robotbi.jdbc.password"))
                        .build());
    }

    private static DataStream<RobotPlanTask> getRobotPlanTask(StreamExecutionEnvironment env,
                                                              Properties properties, ParameterTool params) {
        return env.addSource(new PlanTaskSource(properties, params), "robot plan task")
                .assignTimestampsAndWatermarks(WatermarkStrategy.<RobotPlanTask>forBoundedOutOfOrderness(Duration.ofSeconds(BoundedOutOfOrdernessTime))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime().getTime())
                        .withIdleness(Duration.ofSeconds(60)));
    }

    private static DataStream<RobotWorkState> getRobotWorkStateSource(StreamExecutionEnvironment env, Properties properties) {
        return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotWorkState(), JOB_NAME),
                WatermarkStrategy.<RobotWorkState>forBoundedOutOfOrderness(Duration.ofSeconds(BoundedOutOfOrdernessTime))
                        .withTimestampAssigner((event, timestamp) -> event.getRecvTimestampT().getTime()),
                "robot is Online");
    }
    private static DataStream<RobotState2> getRobotWorkState2Source(StreamExecutionEnvironment env, Properties properties) {
        return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotState2(), JOB_NAME),
                WatermarkStrategy.<RobotState2>forBoundedOutOfOrderness(Duration.ofSeconds(BoundedOutOfOrdernessTime))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime().getTime()),
                "robot state 2");
    }
    private static DataStream<EventTicket> getEventTicketSource(StreamExecutionEnvironment env, Properties properties) {
        return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new EventTicket(), JOB_NAME),
                WatermarkStrategy.<EventTicket>forBoundedOutOfOrderness(Duration.ofSeconds(BoundedOutOfOrdernessTime))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime().getTime()),
                "event ticket");
    }

    private static DataStream<RobotState> getRobotStateSource(StreamExecutionEnvironment env, Properties properties) {
        return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new RobotState(), JOB_NAME),
                WatermarkStrategy.<RobotState>forBoundedOutOfOrderness(Duration.ofSeconds(BoundedOutOfOrdernessTime))
                        .withTimestampAssigner((event, timestamp) -> event.getCldDate().getTime()),
                "robot state data");
    }

    private static DataStream<IncidentEvent> getIncidentEventSource(StreamExecutionEnvironment env, Properties properties) {
        return env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new IncidentEvent(), JOB_NAME),
                WatermarkStrategy.<IncidentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(BoundedOutOfOrdernessTime))
                        .withTimestampAssigner((event, timestamp) -> event.getUpdateTime().getTime()),
                "incident event data");
    }

    private static DataStream<BaseEntity> mergeStream(DataStream<BaseEntity> source1,
                                                      DataStream<BaseEntity> source2) {
        return source1.coGroup(source2)
                .where(BaseEntity::getKey).equalTo(BaseEntity::getKey)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(TumblingWindowTime)))
                .apply(new RichCoGroupFunction<BaseEntity, BaseEntity, BaseEntity>() {
                    @Override
                    public void coGroup(Iterable<BaseEntity> first, Iterable<BaseEntity> second, Collector<BaseEntity> out) throws Exception {
                        first.forEach(out::collect);
                        second.forEach(out::collect);
                    }
                });
    }

    private static DataStream<BaseEntity> mergeStateWorkState(DataStream<BaseEntity> baseEntitySource,
                                                              DataStream<BaseEntity> robotStateSource) {
        return robotStateSource.coGroup(baseEntitySource)
                .where(BaseEntity::getKey).equalTo(BaseEntity::getKey)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(TumblingWindowTime)))
                .apply(new DiagnosisTimelineCoGroupProcessor());
    }

    private static void validate(ParameterTool params) {
        Preconditions.checkNotNull(params.get("env"), "env can not be null");
    }

}
