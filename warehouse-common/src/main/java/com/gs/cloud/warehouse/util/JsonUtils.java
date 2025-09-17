package com.gs.cloud.warehouse.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class JsonUtils {

  public static String jsonNodeGetStrValue(JsonNode node, String key) {
    JsonNode value = node.path(key);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else if (value.isObject() || value.isArray()) {
      return value.toString();
    } else {
      return value.asText();
    }
  }

  public static Long jsonNodeGetLongValue(JsonNode node, String key) {
    JsonNode value = node.path(key);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else {
      return value.asLong();
    }
  }

  public static Integer jsonNodeGetIntValue(JsonNode node, String key) {
    JsonNode value = node.path(key);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else {
      return value.asInt();
    }
  }

  public static Double jsonNodeGetDoubleValue(JsonNode node, String key) {
    JsonNode value = node.path(key);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else {
      return value.asDouble();
    }
  }

  public static Boolean jsonNodeGetBolValue(JsonNode node, String key) {
    JsonNode value = node.path(key);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else {
      return value.asBoolean();
    }
  }

  public static String jsonNodeAtStrValue(JsonNode node, String path) {
    JsonNode value = node.at(path);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else if (value.isObject() || value.isArray()) {
      return value.toString();
    } else {
      return value.asText();
    }
  }

  public static Integer jsonNodeAtIntValue(JsonNode node, String path) {
    JsonNode value = node.at(path);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    }  else {
      return value.asInt();
    }
  }

  public static boolean jsonNodeIsBlank(JsonNode node) {
    return node == null || node.isMissingNode() || node.isNull() || node.isEmpty();
  }
}
