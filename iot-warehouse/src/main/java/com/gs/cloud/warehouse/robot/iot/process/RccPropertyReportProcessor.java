package com.gs.cloud.warehouse.robot.iot.process;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.gs.cloud.warehouse.robot.iot.entity.IotEntity;
import com.gs.cloud.warehouse.robot.iot.entity.RccPropertyReport;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import com.gs.cloud.warehouse.robot.iot.lookup.DeviceIdProductIdILookupFunction;
import org.apache.commons.lang3.time.DateUtils;
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
      JsonNode localizationInfo = jsonNode.get("localizationInfo");
      if (localizationInfo.isNull() || localizationInfo.isEmpty()) {
        return;
      }
      JsonNode position = localizationInfo.get("position");
      if (position.isNull() || position.isEmpty()) {
        return;
      }
      JsonNode extensions = jsonNode.get("extensions");
      Long originalTimestamp = extensions == null ? null : extensions.get("originalTimestamp").asLong();
      RccPropertyReport info = new RccPropertyReport();
      info.setDeviceId(value.getDeviceId());
      Long cldUnixTimestamp = Long.parseLong(value.getCldUnixTimestamp());
      info.setCldUnixTimestamp(cldUnixTimestamp);
      info.setUnixTimestamp(originalTimestamp);
      info.setCldTimestampUtc(Date.from(Instant.ofEpochMilli(cldUnixTimestamp)));
      info.setCldTimestampUtcStr(df_yyyyMMddHHmmss.format(info.getCldTimestampUtc()));
      if (originalTimestamp != null) {
        info.setTimestampUtc(Date.from(Instant.ofEpochMilli(originalTimestamp)));
        info.setTimestampUtcStr(df_yyyyMMddHHmmss.format(info.getTimestampUtc()));
      }
      info.setX(position.get("x").asInt());
      info.setY(position.get("y").asInt());
      info.setAngle(position.get("angle").asDouble());
      info.setPt(value.getPt());
      out.collect(info);
    }).returns(TypeInformation.of(RccPropertyReport.class))
        .process(new DeviceIdProductIdILookupFunction(properties));
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
            StringData.fromString(value.getCldUnixTimestamp()),
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
