package com.gs.cloud.warehouse.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class JsonUtils {

  public static String jsonNodeGetValue(JsonNode node, String key) {
    JsonNode value = node.path(key);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else if (value.isObject() || value.isArray()) {
      return value.toString();
    } else {
      return value.asText();
    }
  }

  public static String jsonNodeAtValue(JsonNode node, String path) {
    JsonNode value = node.at(path);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    } else if (value.isObject() || value.isArray()) {
      return value.toString();
    } else {
      return value.asText();
    }
  }

  public static boolean jsonNodeIsBlank(JsonNode node) {
    return node == null || node.isMissingNode() || node.isNull() || node.isEmpty();
  }
}
