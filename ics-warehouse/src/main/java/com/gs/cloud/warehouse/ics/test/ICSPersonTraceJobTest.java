package com.gs.cloud.warehouse.ics.test;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.ics.entity.*;
import com.gs.cloud.warehouse.ics.function.filter.PersonLocationBeaconFilter;
import com.gs.cloud.warehouse.ics.function.process.PersonLocationStatisticsFunction;
import com.gs.cloud.warehouse.ics.function.process.PersonTenMinuteTraceFunction;
import com.gs.cloud.warehouse.ics.function.process.TxCardTriggerIncidentFunction;
import com.gs.cloud.warehouse.ics.function.window.LastPersonLocationWindow;
import com.gs.cloud.warehouse.ics.sink.CardAbnormalJdbcSink;
import com.gs.cloud.warehouse.ics.sink.PersonTenMinuteTraceJdbcSink;
import com.gs.cloud.warehouse.ics.source.KafkaSourceFactory;
import com.gs.cloud.warehouse.ics.util.Convertor;
import com.gs.cloud.warehouse.trigger.EventTimePurgeTrigger;
import com.gs.cloud.warehouse.util.KafkaSinkUtils;
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
public class ICSPersonTraceJobTest {
    private final static String JOB_NAME = "PersonTenMinuteTrace";
    private final static String JOB_NAME_NEW = "PersonTrace";
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        validate(params);
        Properties properties = PropertiesHelper.loadProperties(params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ItemCrewCardReportTriggerIncident> icsTestSource =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new ItemCrewCardReportTriggerIncident(), JOB_NAME_NEW),
                        watermarkItemCrewCardReportTriggerIncident(),
                        "ics test data");
        //电量异常数据写入mysql结果表
        icsTestSource.addSink(new CardAbnormalJdbcSink(properties));
        env.execute();
    }

    private static void validate(ParameterTool params) {
        Preconditions.checkNotNull(params.get("env"), "env can not be null");
    }

    private static WatermarkStrategy<ItemCrewCardReportTriggerIncident> watermarkItemCrewCardReportTriggerIncident() {
        return WatermarkStrategy.<ItemCrewCardReportTriggerIncident>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTime().getTime());
    }

}


