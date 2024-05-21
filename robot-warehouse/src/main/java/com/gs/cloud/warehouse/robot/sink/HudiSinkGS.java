package com.gs.cloud.warehouse.robot.sink;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HudiSinkGS {

  public static void sinkHudi(DataStream<MonitorWindowStat> resultStream, Properties properties) {
    String targetTable = "ads_real_monitor_window_stat";
    String basePath = "oss://cloud-emr-prod.cn-shanghai.oss-dls.aliyuncs.com/user/hive/warehouse/gs_real_ads.db/ads_real_monitor_window_stat";

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
