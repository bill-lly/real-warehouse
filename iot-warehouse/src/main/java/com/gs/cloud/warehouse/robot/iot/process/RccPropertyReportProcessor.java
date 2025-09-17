package com.gs.cloud.warehouse.robot.iot.process;

import com.gs.cloud.warehouse.robot.iot.entity.Position;
import com.gs.cloud.warehouse.robot.iot.entity.TaskStatus;
import com.gs.cloud.warehouse.robot.iot.lookup.ProductKeyFamilyCodeILookupFunction;
import com.gs.cloud.warehouse.util.JsonUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.robot.iot.entity.IotEntity;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.iot.lookup.DeviceIdProductIdILookupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RccPropertyReportProcessor implements Serializable {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final SimpleDateFormat df_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private final Properties properties;

  public RccPropertyReportProcessor(Properties properties) {
    this.properties = properties;
  }

  public SingleOutputStreamOperator<RccPropertyReport> process(DataStream<IotEntity> dataStream) {
    return dataStream.flatMap((FlatMapFunction<IotEntity, RccPropertyReport>) (value, out) -> {
      if (value.getData() == null || value.getData().isEmpty()) {
        return;
      }
      JsonNode jsonNode = objectMapper.readTree(value.getData());


      JsonNode extensions = jsonNode.get("extensions");
      Long originalTimestamp = extensions ==
              null ? null : JsonUtils.jsonNodeGetLongValue(extensions, "originalTimestamp");
      if (originalTimestamp == null || value.getCldUnixTimestamp() == null) {
        return;
      }
      //主信息
      RccPropertyReport rcc = new RccPropertyReport();
      rcc.setDeviceId(value.getDeviceId());
      Long cldUnixTimestamp = value.getCldUnixTimestamp();
      rcc.setCldUnixTimestamp(cldUnixTimestamp);
      rcc.setUnixTimestamp(originalTimestamp);
      rcc.setCldTimestampUtc(Date.from(Instant.ofEpochMilli(cldUnixTimestamp)));
      rcc.setCldTimestampUtcStr(df_yyyyMMddHHmmss.format(rcc.getCldTimestampUtc()));
      rcc.setTimestampUtc(Date.from(Instant.ofEpochMilli(originalTimestamp)));
      rcc.setTimestampUtcStr(df_yyyyMMddHHmmss.format(rcc.getTimestampUtc()));
      rcc.setProductKey(value.getProductKey());

      //位置信息
      JsonNode localizationInfo = jsonNode.get("localizationInfo");
      if (!JsonUtils.jsonNodeIsBlank(localizationInfo)
          && !JsonUtils.jsonNodeIsBlank(localizationInfo.get("position"))) {
        JsonNode position = localizationInfo.get("position");
        Position pos = new Position();
        pos.setX(JsonUtils.jsonNodeGetIntValue(position, "x"));
        pos.setY(JsonUtils.jsonNodeGetIntValue(position, "y"));
        pos.setAngle(JsonUtils.jsonNodeGetDoubleValue(position, "angle"));
        rcc.setPosition(pos);
      }

      //任务信息
      JsonNode taskStatus = jsonNode.get("taskStatus");
      if (!JsonUtils.jsonNodeIsBlank(taskStatus) && !JsonUtils.jsonNodeIsBlank(taskStatus.get("taskInfo"))) {
        TaskStatus ts = new TaskStatus();
        ts.setStatus(JsonUtils.jsonNodeGetStrValue(taskStatus, "status"));
        ts.setTaskPauseReason(JsonUtils.jsonNodeGetStrValue(taskStatus, "taskPauseReason"));
        JsonNode taskInfo =  taskStatus.get("taskInfo");
        ts.setId(JsonUtils.jsonNodeGetStrValue(taskInfo, "id"));
        ts.setName(JsonUtils.jsonNodeGetStrValue(taskInfo, "name"));
        ts.setProgress(JsonUtils.jsonNodeGetIntValue(taskInfo, "progress"));
        ts.setCleaningMileage(JsonUtils.jsonNodeGetDoubleValue(taskInfo, "cleaningMileage"));
        ts.setTimeRemaining(JsonUtils.jsonNodeGetDoubleValue(taskInfo, "timeRemaining"));
        ts.setCoverageArea(JsonUtils.jsonNodeGetDoubleValue(taskInfo, "coverageArea"));
        ts.setEstimateTime(JsonUtils.jsonNodeGetDoubleValue(taskInfo, "estimateTime"));
        ts.setLength(JsonUtils.jsonNodeGetDoubleValue(taskInfo, "length"));
        ts.setLoop(JsonUtils.jsonNodeGetBolValue(taskInfo, "loop"));
        ts.setLoopCount(JsonUtils.jsonNodeGetIntValue(taskInfo, "loopCount"));
        ts.setTaskMapId(JsonUtils.jsonNodeGetStrValue(taskInfo, "taskMapId"));
        ts.setTaskMapName(JsonUtils.jsonNodeGetStrValue(taskInfo, "taskMapName"));
        ts.setModifyTime(JsonUtils.jsonNodeGetLongValue(taskInfo, "modifyTime"));
        ts.setPictureUrl(JsonUtils.jsonNodeGetStrValue(taskInfo, "pictureUrl"));
        ts.setTaskQueueId(JsonUtils.jsonNodeGetStrValue(taskInfo, "taskQueueId"));
        ts.setTaskQueueType(JsonUtils.jsonNodeGetIntValue(taskInfo, "taskQueueType"));
        ts.setTasks(JsonUtils.jsonNodeGetStrValue(taskInfo, "tasks"));
        ts.setTotalArea(JsonUtils.jsonNodeGetDoubleValue(taskInfo, "totalArea"));
        ts.setUsedCount(JsonUtils.jsonNodeGetIntValue(taskInfo, "usedCount"));
        ts.setWorkModeId(JsonUtils.jsonNodeAtStrValue(taskInfo, "/workMode/workModeId"));
        ts.setWorkModeName(JsonUtils.jsonNodeAtStrValue(taskInfo, "/workMode/workModeName"));
        ts.setWorkModeType(JsonUtils.jsonNodeAtIntValue(taskInfo, "/workMode/workModeType"));
        ts.setLastTime(JsonUtils.jsonNodeGetLongValue(taskInfo, "lastTime"));
        rcc.setTaskStatus(ts);
      }
      rcc.setPt(value.getPt());
      out.collect(rcc);
    }).returns(TypeInformation.of(RccPropertyReport.class))
        .process(new DeviceIdProductIdILookupFunction(properties))
        .process(new ProductKeyFamilyCodeILookupFunction(properties))
            .filter((FilterFunction<RccPropertyReport>) rccPropertyReport ->
                    rccPropertyReport.getProductId() != null && rccPropertyReport.getRobotFamilyCode() != null);
  }

  public KafkaSink<RccPropertyReport> getKafkaSink() {
    return KafkaSink.<RccPropertyReport>builder()
        .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers.bigdata"))
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(properties.getProperty("kafka.topic.rcc.property.report"))
            .setKeySerializationSchema(new CommonKeySerialization<RccPropertyReport>())
            .setValueSerializationSchema(new CommonFormat<>(RccPropertyReport.class))
            .build())
        .build();
  }

  public void sinkHudi(DataStream<IotEntity> resultStream) {
    String targetTable = "ods_i_rcc_property_report_new";
    String basePath = "oss://cloud-emr-prod.cn-shanghai.oss-dls.aliyuncs.com/user/hive/warehouse/gs_real_ods.db/ods_i_rcc_property_report_new";

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), basePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.INSERT.value());
    options.put(FlinkOptions.WRITE_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    options.put(FlinkOptions.COMPACTION_TASKS.key(), properties.getProperty("hudiParallelism", "1"));
    DataStream<RowData> dataStream = resultStream.map((MapFunction<IotEntity, RowData>) value ->
            GenericRowData.of(
                    StringData.fromString(value.getProductKey()),
                    StringData.fromString(value.getDeviceId()),
                    StringData.fromString(value.getVersion()),
                    StringData.fromString(value.getNamespace()),
                    StringData.fromString(value.getObjectName()),
                    StringData.fromString(String.valueOf(value.getCldUnixTimestamp())),
                    StringData.fromString(value.getMethod()),
                    StringData.fromString(value.getData()),
                    StringData.fromString(value.getPt())));

    HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
        .column("product_key STRING")
        .column("device_id STRING")
        .column("version STRING")
        .column("namespace STRING")
        .column("object_name STRING")
        .column("cld_unix_timestamp STRING")
        .column("`method` STRING")
        .column("data STRING")
        .column("pt STRING")
        .pk("product_key")
        .partition("pt")
        .options(options);
    builder.sink(dataStream, false);
  }
}
