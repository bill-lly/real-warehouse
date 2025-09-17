package com.gs.cloud.warehouse.robot.iot.process;

import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.robot.iot.entity.TaskStatus;
import com.gs.cloud.warehouse.robot.iot.entity.WindowedRccCoverageArea;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskStatusProcessor extends ProcessWindowFunction<RccPropertyReport, WindowedRccCoverageArea, String, TimeWindow> {

    private final Properties properties;

    public TaskStatusProcessor(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void process(String s,
                        Context context,
                        Iterable<RccPropertyReport> elements,
                        Collector<WindowedRccCoverageArea> out) {

        List<RccPropertyReport> list = (ArrayList<RccPropertyReport>)elements;
        if (list == null || list.isEmpty()) {
            return;
        }
        list.sort(Comparator.comparing(RccPropertyReport::getUnixTimestamp));

        ArrayList<Tuple2<Long, Long>> timeSlicing = new ArrayList<>();
        for (int i = 0, j = i + 1;  i < list.size() - 1 && j < list.size(); j++) {
            if ("RUNNING".equals(list.get(i).getTaskStatus().getStatus())) {
                if (!"RUNNING".equals(list.get(j).getTaskStatus().getStatus()) || j == list.size() - 1) {
                    timeSlicing.add(new Tuple2<>(list.get(i).getUnixTimestamp(), list.get(j).getUnixTimestamp()));
                    i = j + 1;
                }
            }
        }
        int runningDuration = timeSlicing.stream().mapToInt(tuple2 -> (int) (tuple2.f1 - tuple2.f0)).sum();

        list = list.stream().filter(rccPropertyReport -> {
             return "RUNNING".equals(rccPropertyReport.getTaskStatus().getStatus())
                     && rccPropertyReport.getTaskStatus().getCoverageArea() != null;
        }).collect(Collectors.toList());

        if (list.isEmpty()) {
            return;
        }

        RccPropertyReport first = list.get(0);
        RccPropertyReport last = list.get(list.size()-1);
        TaskStatus firstTask = first.getTaskStatus();
        TaskStatus lastTask = last.getTaskStatus();
        Double increasedCoverageArea = lastTask.getCoverageArea() >= firstTask.getCoverageArea()
                ? lastTask.getCoverageArea() - firstTask.getCoverageArea() : lastTask.getCoverageArea();

        WindowedRccCoverageArea rccCoverageArea = new WindowedRccCoverageArea();
        rccCoverageArea.setProductId(first.getProductId());
        rccCoverageArea.setWindowStartTimeUtc(new Date(context.window().getStart()));
        rccCoverageArea.setWindowEndTimeUtc(new Date((context.window().getEnd())));
        rccCoverageArea.setCldStartTimeUtc(first.getCldTimestampUtc());
        rccCoverageArea.setCldEndTimeUtc(last.getCldTimestampUtc());
        rccCoverageArea.setStartTimeUtc(first.getTimestampUtc());
        rccCoverageArea.setEndTimeUtc(last.getTimestampUtc());
        rccCoverageArea.setFirstCoverageArea(firstTask.getCoverageArea());
        rccCoverageArea.setLastCoverageArea(lastTask.getCoverageArea());
        rccCoverageArea.setIncreasedCoverageArea(increasedCoverageArea);
        rccCoverageArea.setRunningDuration(runningDuration);
        rccCoverageArea.setRobotFamilyCode(last.getRobotFamilyCode());
        out.collect(rccCoverageArea);
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
