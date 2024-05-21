package com.gs.cloud.warehouse.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.regex.Pattern;

public class PropertiesHelper {

  private static final Logger logger = LoggerFactory.getLogger(PropertiesHelper.class);
  private static final Pattern QUOTE_PATTERN = Pattern.compile("\\'");

  public static Properties loadProperties(ParameterTool params) throws IOException {
    Properties properties = PropertiesHelper.loadProperties("conf.properties", params.get("env"));
    mergeProperties(properties, params);
    return properties;
  }

  private static void mergeProperties(Properties properties, ParameterTool params) {
    Properties p = params.getProperties();
    for (String pn: params.getProperties().stringPropertyNames()) {
      properties.setProperty(pn, p.getProperty(pn));
    }
  }

  public static Properties loadProperties(String fileName, String env) throws IOException {
    Properties properties = new Properties();
    InputStream inputStream = PropertiesHelper.class.getResourceAsStream(String.format("/%s_%s", env, fileName));
    if (null == inputStream) {
      logger.error(fileName + " does not exist");
      throw new FileNotFoundException(fileName + " does not exist");
    }
    properties.load(inputStream);
    return properties;
  }



  public static int getInt(Properties properties, String key, int defaultValue) {
    String value = properties.getProperty(key);
    return null == value ? defaultValue : Integer.parseInt(value);
  }

  public static String value(String raw) {
    return null == raw ? "" : QUOTE_PATTERN.matcher(raw).replaceAll("\\\\'");
  }

  public static String dateFormatter(LocalDateTime dateTime, DateTimeFormatter dateTimeFormatter) {
    if (dateTime == null) {
      return null;
    }
    return dateTime.format(dateTimeFormatter);
  }

  public static Integer intValue(Integer value) {
    return value == null ? 0 : value;
  }
}
