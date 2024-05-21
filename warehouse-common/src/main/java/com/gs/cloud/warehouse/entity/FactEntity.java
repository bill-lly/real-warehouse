package com.gs.cloud.warehouse.entity;

import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class FactEntity extends BaseEntity{

  @JsonIgnore
  public abstract CommonFormat getSerDeserializer();

  @JsonIgnore
  public abstract CommonKeySerialization getKeySerializer();

  @JsonIgnore
  public abstract String getKafkaServer();

  @JsonIgnore
  public abstract String getKafkaTopic();

  @JsonIgnore
  public abstract String getKafkaGroupId();
}
