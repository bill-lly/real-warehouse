package com.gs.cloud.warehouse.robot.iot.process;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.robot.iot.entity.WindowedRccPosition;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class PositionCalculateProcessor extends ProcessWindowFunction<RccPropertyReport, WindowedRccPosition, String, TimeWindow> {

  private final Properties properties;

  private final SimpleDateFormat df_yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
  private final SimpleDateFormat df_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public PositionCalculateProcessor(Properties properties) {
    this.properties = properties;
  }

  @Override
  public void process(String s,
                      Context context,
                      Iterable<RccPropertyReport> elements,
                      Collector<WindowedRccPosition> out) throws IOException {

    List<RccPropertyReport> list = (ArrayList<RccPropertyReport>)elements;
    if (list == null || list.isEmpty()) {
      return;
    }
    TreeMap<Long, RccPropertyReport> rccMap = new TreeMap<>();
    list.forEach(x-> rccMap.put(x.getUnixTimestamp(), x));
    Date windowStart = new Date(context.window().getStart());
    Date windowEnd = new Date(context.window().getEnd());

    while (true) {
      Iterator<Long> iterator = rccMap.keySet().iterator();
      Long first = iterator.next();
      while (iterator.hasNext()) {
        Long curr = iterator.next();
        if (curr - first >= 60000) {
          flush(rccMap, curr, windowStart, windowEnd, out);
          break;
        }
      }
      if (!iterator.hasNext()) {
        flush(rccMap, rccMap.lastKey() + 1, windowStart, windowEnd, out);
        break;
      }
    }
  }

  private void flush(TreeMap<Long, RccPropertyReport> rccMap,
                     Long end, Date windowStart, Date windowEnd,
                     Collector<WindowedRccPosition> out) {
    if (rccMap.isEmpty()) {
      return;
    }
    Iterator<Long> iterator = rccMap.keySet().iterator();
    RccPropertyReport first = rccMap.get(iterator.next());
    RccPropertyReport pre = first;
    iterator.remove();
    Date firstCldStartTimeUtc = first.getCldTimestampUtc();
    Date endCldStartTimeUtc = first.getCldTimestampUtc();
    Date firstStartTimeUtc = first.getTimestampUtc();
    double angleSum = first.getAngle();
    double distance = 0.0;
    while (iterator.hasNext()) {
      Long currKey = iterator.next();
      RccPropertyReport curr = rccMap.get(currKey);
      if (currKey < end) {
        distance = distance + Math.sqrt(Math.pow(curr.getX() - pre.getX(), 2) + Math.pow(curr.getY() - pre.getY(), 2));
        distance = (double)Math.round(distance * 100) / 100;
        angleSum = angleSum + curr.getAngle();
        firstCldStartTimeUtc = firstCldStartTimeUtc.compareTo(curr.getCldTimestampUtc()) < 0
            ? firstCldStartTimeUtc : curr.getCldTimestampUtc();
        endCldStartTimeUtc = endCldStartTimeUtc.compareTo(curr.getCldTimestampUtc()) < 0
            ? curr.getCldTimestampUtc() : endCldStartTimeUtc;
        pre = curr;
        iterator.remove();
      } else {
        break;
      }
    }
    WindowedRccPosition windowedRccPosition = new WindowedRccPosition();
    windowedRccPosition.setDeviceId(first.getDeviceId());
    windowedRccPosition.setProductId(first.getProductId());
    windowedRccPosition.setWindowStartTimeUtc(windowStart);
    windowedRccPosition.setWindowEndTimeUtc(windowEnd);
    windowedRccPosition.setCldStartTimeUtc(firstCldStartTimeUtc);
    windowedRccPosition.setCldEndTimeUtc(endCldStartTimeUtc);
    windowedRccPosition.setStartTimeUtc(firstStartTimeUtc);
    windowedRccPosition.setEndTimeUtc(pre.getTimestampUtc());
    windowedRccPosition.setDistance(distance * 5);
    double effDis = Math.sqrt(Math.pow(pre.getX() - first.getX(), 2) + Math.pow(pre.getY() - first.getY(), 2));
    windowedRccPosition.setEffectiveDistance(((double)Math.round(effDis * 100) / 100) * 5);
    windowedRccPosition.setAngle((double)Math.round(angleSum * 100) / 100);
    out.collect(windowedRccPosition);
  }

  public KafkaSink<WindowedRccPosition> getKafkaSink() {
    return KafkaSink.<WindowedRccPosition>builder()
        .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers.bigdata"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(properties.getProperty("kafka.topic.rcc.robot.displacement"))
            .setKeySerializationSchema(new CommonKeySerialization<WindowedRccPosition>())
            .setValueSerializationSchema(new CommonFormat<>(WindowedRccPosition.class))
            .build())
        .build();
  }

  public void sinkHudi(DataStream<WindowedRccPosition> resultStream) {
    String targetTable = "ads_i_rcc_robot_displacement";
    String basePath = "oss://cloud-emr-prod.cn-shanghai.oss-dls.aliyuncs.com/user/hive/warehouse/gs_real_ads.db/ads_i_rcc_robot_displacement";

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), basePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.WRITE_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    options.put(FlinkOptions.COMPACTION_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    DataStream<RowData> dataStream = resultStream.map((MapFunction<WindowedRccPosition, RowData>) value ->
        GenericRowData.of(
            StringData.fromString(value.getDeviceId()),
            StringData.fromString(value.getProductId()),
            StringData.fromString(df_yyyyMMddHHmmss.format(value.getWindowStartTimeUtc())),
            StringData.fromString(df_yyyyMMddHHmmss.format(value.getWindowEndTimeUtc())),
            StringData.fromString(df_yyyyMMddHHmmss.format(value.getCldStartTimeUtc())),
            StringData.fromString(df_yyyyMMddHHmmss.format(value.getCldEndTimeUtc())),
            StringData.fromString(value.getStartTimeUtc() == null ? null : df_yyyyMMddHHmmss.format(value.getStartTimeUtc())),
            StringData.fromString(value.getEndTimeUtc() == null ? null : df_yyyyMMddHHmmss.format(value.getEndTimeUtc())),
            value.getDistance(),
            value.getEffectiveDistance(),
            value.getAngle(),
            //分区字段转换成东八区的时间
            StringData.fromString(df_yyyyMMdd.format(DateUtils.addHours(value.getWindowEndTimeUtc(), 8)))));

    HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
        .column("device_id STRING")
        .column("product_id STRING")
        .column("window_start_time STRING")
        .column("window_end_time STRING")
        .column("cld_start_time_utc String")
        .column("cld_end_time_utc String")
        .column("start_time_utc String")
        .column("end_time_utc String")
        .column("distance Double")
        .column("effective_distance Double")
        .column("angle Double")
        .column("pt String")
        .pk("device_id")
        .partition("pt")
        .options(options);
    builder.sink(dataStream, false);
  }
}
