package com.gs.cloud.warehouse.ics;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.ics.entity.PersonLocationBeacon;
import com.gs.cloud.warehouse.ics.entity.PersonTenMinuteTrace;
import com.gs.cloud.warehouse.ics.function.filter.PersonLocationBeaconFilter;
import com.gs.cloud.warehouse.ics.function.process.PersonLocationStatisticsFunction;
import com.gs.cloud.warehouse.ics.function.process.PersonTenMinuteTraceFunction;
import com.gs.cloud.warehouse.ics.function.window.LastPersonLocationWindow;
import com.gs.cloud.warehouse.ics.sink.PersonTenMinuteTraceJdbcSink;
import com.gs.cloud.warehouse.ics.source.KafkaSourceFactory;
import com.gs.cloud.warehouse.trigger.EventTimePurgeTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

/*
ics人员定位统计任务
* */
public class ICSPersonTraceJob {
    private final static String JOB_NAME = "PersonTenMinuteTrace";
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        validate(params);
        Properties properties = PropertiesHelper.loadProperties(params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        DataStream<PersonLocationBeacon> personLocationBeaconSource =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new PersonLocationBeacon(), JOB_NAME),
                        watermarkStrategyWorkState(),
                        "ics person location beacon data");
        //人员定位10分钟轨迹统计
        DataStream<PersonTenMinuteTrace> personTenMinuteTrace = personLocationBeaconSource.filter(new PersonLocationBeaconFilter())
            .keyBy(PersonLocationBeacon::getCrewId)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .trigger(EventTimePurgeTrigger.create())
            .apply(new LastPersonLocationWindow())
            .process(new PersonLocationStatisticsFunction())
            .process(new PersonTenMinuteTraceFunction());
        //personTenMinuteTrace.print();
        PersonTenMinuteTraceJdbcSink.getJdbcSink(properties, personTenMinuteTrace);
        env.execute();
    }
    private static void validate(ParameterTool params) {
        Preconditions.checkNotNull(params.get("env"), "env can not be null");
    }

    private static WatermarkStrategy<PersonLocationBeacon> watermarkStrategyWorkState() {
        return WatermarkStrategy.<PersonLocationBeacon>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTime().getTime());
    }
}


