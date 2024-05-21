package com.gs.cloud.warehouse.entity;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;
import java.util.Optional;

public abstract class BaseEntity implements Comparable<BaseEntity> {

  @JsonIgnore
  public abstract Date getEventTime();

  @JsonIgnore
  public abstract String getKey();
}
