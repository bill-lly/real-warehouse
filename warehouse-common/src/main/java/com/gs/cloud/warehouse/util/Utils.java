package com.gs.cloud.warehouse.util;


import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

public class Utils {

    /**
     * 基石数据版本比较大小。版本格式类似： “1.0.0”、“1.3.0”、 “1.4.9”
     * @param version1
     * @param version2
     * @return
     */
    public static int compare(String version1, String version2) {

        if (Strings.isBlank(version1) || Strings.isBlank(version2)) {
            return -1;
        }

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

    public static Long parseLong(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        return Long.parseLong(str);
    }

    public static Double parseDouble(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        return Double.parseDouble(str);
    }

    public static Integer parseInt(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        return Integer.parseInt(str);
    }

    public static Long multiply(Long a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double multiply(Double a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }
}
