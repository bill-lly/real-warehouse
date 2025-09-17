package com.gs.cloud.warehouse;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test01 {

  private final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

  @Test
  public void test01() {
    long timestamp = System.currentTimeMillis();
    timestamp = timestamp - (timestamp % (60*1000));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    System.out.println(sdf.format(new Date(timestamp)));
    System.out.println(sdf.format(new Date(timestamp + 60*1000)));
  }

  @Test
  public void test02() throws ParseException {
    String path = "/user/hive/warehouse/test.db/ods_f_robot_test";
    String regex = "/user/hive/warehouse/[\\w]+\\.db/[\\w]+/*.?";
    Pattern pattern = Pattern.compile(regex);
    Matcher m =pattern.matcher(path);
    System.out.println(m.find());

  }

  @Test
  public void test03() throws ParseException {
    System.out.println(compare("1.0.0", "1.3.0"));
    System.out.println(compare("1.1.0", "1.3.0"));
    System.out.println(compare("1.1.1", "1.3.0"));
    System.out.println(compare("1.1.12", "1.3.0"));
    System.out.println(compare("1.2.0", "1.3.0"));
    System.out.println(compare("1.2.9", "1.3.0"));
    System.out.println(compare("1.3.0", "1.3.0"));
    System.out.println(compare("1.3.1", "1.3.0"));
    System.out.println(compare("1.4.0", "1.3.0"));
    System.out.println(compare("1.5.0", "1.3.0"));
    System.out.println(compare("1.6.0", "1.3.0"));
    System.out.println(compare("1.7.0", "1.3.0"));
    System.out.println(compare("1.8.0", "1.3.0"));
    System.out.println(compare("1.9.0", "1.3.0"));
    System.out.println(compare("1.10.0", "1.3.0"));
    System.out.println(compare("1.11.0", "1.3.0"));
  }

  public static int compare(String version1, String version2) {
    // 分割版本号
    String[] v1Parts = version1.split("\\.");
    String[] v2Parts = version2.split("\\.");

    // 获取最大长度
    int maxLength = Math.max(v1Parts.length, v2Parts.length);

    // 逐位比较
    for (int i = 0; i < maxLength; i++) {
      int v1 = i < v1Parts.length ? Integer.parseInt(v1Parts[i]) : 0;
      int v2 = i < v2Parts.length ? Integer.parseInt(v2Parts[i]) : 0;

      if (v1 > v2) {
        return 1;  // version1 > version2
      } else if (v1 < v2) {
        return -1; // version1 < version2
      }
      // 相等则继续比较下一位
    }

    return 0; // 版本号完全相同
  }



}
