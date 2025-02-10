package com.gs.cloud.warehouse.util;

import org.apache.logging.log4j.util.Strings;

public class UnixTimestampUtils {

  public static boolean invalid(String unixTimestamp) {
    return Strings.isBlank(unixTimestamp) || "0".equals(unixTimestamp.trim());
  }
}
