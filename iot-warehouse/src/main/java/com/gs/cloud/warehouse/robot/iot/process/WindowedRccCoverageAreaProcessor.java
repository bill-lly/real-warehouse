package com.gs.cloud.warehouse.robot.iot.process;

import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.iot.entity.WindowedRccCoverageArea;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static java.lang.String.valueOf;

public class WindowedRccCoverageAreaProcessor extends KeyedProcessFunction<String, WindowedRccCoverageArea, WindowedRccCoverageArea> {
    private final Properties properties;
    public WindowedRccCoverageAreaProcessor(Properties properties) {
        this.properties=properties;
    }

    private transient ValueState<Integer> runningDurationState;
    private transient ValueState<Double> coverageAreaState;
    private transient ValueState<WindowedRccCoverageArea> firstRccCoverageAreaState;
    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        ValueStateDescriptor<Integer> runningDurationStateDescriptor =
                new ValueStateDescriptor<>(
                        "runningDurationState",
                        BasicTypeInfo.INT_TYPE_INFO);
        ValueStateDescriptor<Double> coverageAreaStateDescriptor =
                new ValueStateDescriptor<>(
                        "coverageAreaState",
                        BasicTypeInfo.DOUBLE_TYPE_INFO);
        ValueStateDescriptor<WindowedRccCoverageArea> firstRccCoverageAreaStateDescriptor =
                new ValueStateDescriptor<>(
                        "firstRccCoverageAreaState",
                        WindowedRccCoverageArea.class);

        runningDurationStateDescriptor.enableTimeToLive(ttlConfig);
        coverageAreaStateDescriptor.enableTimeToLive(ttlConfig);
        firstRccCoverageAreaStateDescriptor.enableTimeToLive(ttlConfig);

        runningDurationState = getRuntimeContext().getState(runningDurationStateDescriptor);
        coverageAreaState = getRuntimeContext().getState(coverageAreaStateDescriptor);
        firstRccCoverageAreaState = getRuntimeContext().getState(firstRccCoverageAreaStateDescriptor);

        super.open(parameters);
    }

    @Override
    public void processElement(WindowedRccCoverageArea value, KeyedProcessFunction<String, WindowedRccCoverageArea, WindowedRccCoverageArea>.Context ctx, Collector<WindowedRccCoverageArea> out) throws Exception {
        Integer runningDuration = runningDurationState.value();
        Double coverageArea = coverageAreaState.value();
        WindowedRccCoverageArea firstRccCoverageArea = firstRccCoverageAreaState.value();
        if (runningDuration == null || coverageArea == null
                || firstRccCoverageArea == null
        ) {
            runningDuration = 0;
            coverageArea = 0D;
            firstRccCoverageArea=value;
        }

        runningDuration += value.getRunningDuration();
        coverageArea += value.getIncreasedCoverageArea();

        if (runningDuration >= 60 * 1000 * 10) {
            // 10 分钟内 清洁面积是否异常
            Boolean isAbnormal;
            switch (valueOf(value.getRobotFamilyCode())){
                case "S":
                case "50":
                case "40":
                   isAbnormal = coverageArea<50;
                    break;
                case "75":
                   isAbnormal = coverageArea<100;
                    break;
                default:
                    isAbnormal = false;
                    break;
            }
            if (isAbnormal){
                WindowedRccCoverageArea rccCoverageArea = new WindowedRccCoverageArea();
                rccCoverageArea.setProductId(value.getProductId());
                rccCoverageArea.setWindowStartTimeUtc(firstRccCoverageArea.getWindowStartTimeUtc());
                rccCoverageArea.setWindowEndTimeUtc(value.getWindowEndTimeUtc());
                rccCoverageArea.setCldStartTimeUtc(firstRccCoverageArea.getCldStartTimeUtc());
                rccCoverageArea.setCldEndTimeUtc(value.getCldEndTimeUtc());
                rccCoverageArea.setStartTimeUtc(firstRccCoverageArea.getStartTimeUtc());
                rccCoverageArea.setEndTimeUtc(value.getEndTimeUtc());
                rccCoverageArea.setFirstCoverageArea(firstRccCoverageArea.getFirstCoverageArea());
                rccCoverageArea.setLastCoverageArea(value.getLastCoverageArea());
                rccCoverageArea.setIncreasedCoverageArea(coverageArea);
                rccCoverageArea.setRunningDuration(runningDuration);
                rccCoverageArea.setRobotFamilyCode(value.getRobotFamilyCode());
                // 异常处理
                out.collect(rccCoverageArea);
            }
            runningDurationState.clear();
            coverageAreaState.clear();
            firstRccCoverageAreaState.clear();
        }else {
            runningDurationState.update(runningDuration);
            coverageAreaState.update(coverageArea);
            firstRccCoverageAreaState.update(firstRccCoverageArea);
        }
    }

    public KafkaSink<WindowedRccCoverageArea> getKafkaSink() {
        return KafkaSink.<WindowedRccCoverageArea>builder()
                .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers.bigdata"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(properties.getProperty("kafka.topic.rcc.robot.coverage.area"))
                        .setKeySerializationSchema(new CommonKeySerialization<WindowedRccCoverageArea>())
                        .setValueSerializationSchema(new CommonFormat<>(WindowedRccCoverageArea.class))
                        .build())
                .build();
    }
}
