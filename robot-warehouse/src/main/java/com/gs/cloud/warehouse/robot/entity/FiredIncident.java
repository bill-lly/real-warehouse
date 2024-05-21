package com.gs.cloud.warehouse.robot.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.entity.FactEntity;
import com.gs.cloud.warehouse.format.CommonFormat;
import com.gs.cloud.warehouse.format.CommonKeySerialization;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

import java.util.Date;
import java.util.Map;

public class FiredIncident extends FactEntity {

  @JsonProperty("eventId")
  private String eventId;

  @JsonProperty("subjectId")
  private String subjectId;

  @JsonProperty("subjectType")
  private String subjectType;

  @JsonProperty("incidentCode")
  private String incidentCode;

  @JsonProperty("incidentStartTime")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date incidentStartTime;

  @JsonProperty("eventTime")
  @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
  private Date entityEventTime;

  @JsonProperty("finalized")
  private Boolean finalized;

  @JsonProperty("cleanType")
  private String cleanType;

  @JsonProperty("subjectModel")
  private String subjectModel;

  @JsonProperty("metas")
  private Map<String, String> metas;

  @JsonProperty("ext")
  private Map<String, Map<String, String>> ext;

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public String getSubjectId() {
    return subjectId;
  }

  public void setSubjectId(String subjectId) {
    this.subjectId = subjectId;
  }

  public String getSubjectType() {
    return subjectType;
  }

  public void setSubjectType(String subjectType) {
    this.subjectType = subjectType;
  }

  public String getIncidentCode() {
    return incidentCode;
  }

  public void setIncidentCode(String incidentCode) {
    this.incidentCode = incidentCode;
  }

  public Date getIncidentStartTime() {
    return incidentStartTime;
  }

  public void setIncidentStartTime(Date incidentStartTime) {
    this.incidentStartTime = incidentStartTime;
  }

  public Date getEntityEventTime() {
    return entityEventTime;
  }

  public void setEntityEventTime(Date entityEventTime) {
    this.entityEventTime = entityEventTime;
  }

  public Boolean getFinalized() {
    return finalized;
  }

  public void setFinalized(Boolean finalized) {
    this.finalized = finalized;
  }

  public String getCleanType() {
    return cleanType;
  }

  public void setCleanType(String cleanType) {
    this.cleanType = cleanType;
  }

  public Map<String, String> getMetas() {
    return metas;
  }

  public void setMetas(Map<String, String> metas) {
    this.metas = metas;
  }

  public Map<String, Map<String, String>> getExt() {
    return ext;
  }

  public void setExt(Map<String, Map<String, String>> ext) {
    this.ext = ext;
  }

  public String getSubjectModel() {
    return subjectModel;
  }

  public void setSubjectModel(String subjectModel) {
    this.subjectModel = subjectModel;
  }

  @Override
  public Date getEventTime() {
    return getEntityEventTime();
  }

  @Override
  public String getKey() {
    return getSubjectId();
  }

  @Override
  public CommonFormat<FiredIncident> getSerDeserializer() {
    return new CommonFormat<>(FiredIncident.class);
  }

  @Override
  public CommonKeySerialization<FiredIncident> getKeySerializer() {
    return new CommonKeySerialization<>();
  }

  @Override
  public String getKafkaServer() {
    return "kafka.bootstrap.servers";
  }

  @Override
  public String getKafkaTopic() {
    return "kafka.topic.incidentEvent.notice";
  }

  @Override
  public String getKafkaGroupId() {
    return null;
  }

  @Override
  public int compareTo(@NotNull BaseEntity o) {
    return 0;
  }
}
