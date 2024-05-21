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



}
