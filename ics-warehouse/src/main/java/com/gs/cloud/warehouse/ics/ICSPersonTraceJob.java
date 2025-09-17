package com.gs.cloud.warehouse.ics;

import com.gs.cloud.warehouse.config.PropertiesHelper;
import com.gs.cloud.warehouse.ics.entity.*;
import com.gs.cloud.warehouse.ics.function.filter.ItemCrewCardReportTriggerIncidentFilter;
import com.gs.cloud.warehouse.ics.function.filter.PersonLocationBeaconFilter;
import com.gs.cloud.warehouse.ics.function.filter.TxCardFilter;
import com.gs.cloud.warehouse.ics.function.lookup.PositionCrewRelationFunction;
import com.gs.cloud.warehouse.ics.function.lookup.PositionFunction;
import com.gs.cloud.warehouse.ics.function.process.*;
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
public class ICSPersonTraceJob {
    private final static String JOB_NAME = "PersonTenMinuteTrace";
    private final static String JOB_NAME_NEW = "PersonTrace";
    private final static String JOB_NAME_DWS_ITEM_CREW_CARD_DETAIL = "DwsItemCrewCardDetail";
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        validate(params);
        Properties properties = PropertiesHelper.loadProperties(params);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        DataStream<PersonLocationBeacon> personLocationBeaconSource =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new PersonLocationBeacon(), JOB_NAME),
                        watermarkStrategyPersonLocationBeacon(),
                        "ics person location beacon data");
        DataStream<TxCard> txCardReportSource =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new TxCard(), JOB_NAME_NEW),
                        watermarkStrategyTxCard(),
                        "ics tx card data");
        DataStream<Incident> incidentSource =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new Incident(), JOB_NAME_NEW),
                        watermarkStrategyIncident(),
                        "ics incident data");
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

        //图新工牌上报监控
        DataStream<ItemCrewCardReportTriggerIncident> dwsItemCrewCardRepTrigIncid = txCardReportSource.coGroup(incidentSource)
                .where(TxCard::getKey).equalTo(Incident::getKey)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(EventTimePurgeTrigger.create())
                .apply(new TxCardTriggerIncidentFunction())
                .map(Convertor::trigIncidResult2ItemCrewCardRepTrigIncid);
        //dwsItemCrewCardRepTrigIncid.print();
        dwsItemCrewCardRepTrigIncid.sinkTo(KafkaSinkUtils.getKafkaSink(properties, new ItemCrewCardReportTriggerIncident()));
        //电量异常数据写入mysql结果表
        dwsItemCrewCardRepTrigIncid.addSink(new CardAbnormalJdbcSink(properties));

        //抽检功能开发(图新工牌上报、定位数据、监控数据)
        DataStream<TxCard> txCardRepSrcItemCrewCardDet =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new TxCard(), JOB_NAME_DWS_ITEM_CREW_CARD_DETAIL),
                        watermarkStrategyTxCard2(),
                        "ics tx card data to item crew card detail");
        DataStream<PersonLocationBeacon> perLocBeaconSrcItemCrewCardDet =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new PersonLocationBeacon(), JOB_NAME_DWS_ITEM_CREW_CARD_DETAIL),
                        watermarkStrategyPersonLocationBeacon(),
                        "ics person location beacon data to item crew card detail");
        DataStream<ItemCrewCardReportTriggerIncident> dwsItemCrewCardRepTrigIncidItemCrewCardDet =
                env.fromSource(KafkaSourceFactory.getKafkaSource(properties, new ItemCrewCardReportTriggerIncident(), JOB_NAME_DWS_ITEM_CREW_CARD_DETAIL),
                        watermarkStrategyDwsItemCrewCardReportTriggerIncident(),
                        "item crew report trigger incident to item crew card detail");

        DataStream<ItemCrewCardDetail> txCardLocationCoGroup = (txCardRepSrcItemCrewCardDet.filter(new TxCardFilter())).coGroup(perLocBeaconSrcItemCrewCardDet.filter(new PersonLocationBeaconFilter()))
                .where(TxCard::getCrewIdStr).equalTo(PersonLocationBeacon::getCrewId)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .trigger(EventTimePurgeTrigger.create())
                .apply(new TxCardLocationCoGroupFunction())
                .process(new PositionCrewRelationFunction(properties))
                .process(new PositionFunction(properties));
        //txCardLocationCoGroup.print();
        DataStream<ItemCrewCardDetail> dwsItemCrewCardDetail = txCardLocationCoGroup.coGroup(dwsItemCrewCardRepTrigIncidItemCrewCardDet.filter(new ItemCrewCardReportTriggerIncidentFilter()))
                .where(ItemCrewCardDetail::getKey).equalTo(ItemCrewCardReportTriggerIncident::getKey)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .trigger(EventTimePurgeTrigger.create())
                .apply(new TxCardTrigIncidCoGroupFunction());
        //dwsItemCrewCardDetail.print();
        dwsItemCrewCardDetail.sinkTo(KafkaSinkUtils.getKafkaSink(properties, new ItemCrewCardDetail()));

        env.execute();
    }

    private static void validate(ParameterTool params) {
        Preconditions.checkNotNull(params.get("env"), "env can not be null");
    }

    private static WatermarkStrategy<PersonLocationBeacon> watermarkStrategyPersonLocationBeacon() {
        return WatermarkStrategy.<PersonLocationBeacon>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTime().getTime())
                .withIdleness(Duration.ofSeconds(20));
    }

    private static WatermarkStrategy<TxCard> watermarkStrategyTxCard() {
        return WatermarkStrategy.<TxCard>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getCloudRecvTime().getTime())
                .withIdleness(Duration.ofSeconds(20));
    }

    private static WatermarkStrategy<TxCard> watermarkStrategyTxCard2() {
        return WatermarkStrategy.<TxCard>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTime().getTime())
                .withIdleness(Duration.ofSeconds(20));
    }

    private static WatermarkStrategy<Incident> watermarkStrategyIncident() {
        return WatermarkStrategy.<Incident>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTime().getTime())
                .withIdleness(Duration.ofSeconds(20));
    }

    private static WatermarkStrategy<ItemCrewCardReportTriggerIncident> watermarkStrategyDwsItemCrewCardReportTriggerIncident() {
        return WatermarkStrategy.<ItemCrewCardReportTriggerIncident>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(
                        (event, timestamp) -> event.getEventTime() == null ? event.getCloudTime().getTime() : event.getEventTime().getTime())
                .withIdleness(Duration.ofSeconds(20));
    }
}


