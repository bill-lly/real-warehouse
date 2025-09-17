package com.gs.cloud.warehouse.util;

import java.sql.*;

public class JdbcUtils {

    public static void setStatementString(PreparedStatement statement,
                                          int index,
                                          String str) throws SQLException {
        if (str == null) {
            statement.setNull(index, Types.VARCHAR);
        }
        statement.setString(index, str);
    }

    public static void setStatementDate(PreparedStatement statement,
                                          int index,
                                          Date date) throws SQLException {
        if (date == null) {
            statement.setNull(index, Types.DATE);
            return;
        }
        statement.setDate(index, date);
    }

    public static void setStatementTimeStamp(PreparedStatement statement,
                                        int index,
                                        Timestamp timestamp) throws SQLException {
        if (timestamp == null) {
            statement.setNull(index, Types.TIMESTAMP);
            return;
        }
        statement.setTimestamp(index, timestamp);
    }

    public static void setStatementLong(PreparedStatement statement,
                                        int index,
                                        Long num) throws SQLException {
        if (num == null) {
            statement.setNull(index, Types.BIGINT);
            return;
        }
        statement.setLong(index, num);
    }

    public static void setStatementDouble(PreparedStatement statement,
                                        int index,
                                        Double num) throws SQLException {
        if (num == null) {
            statement.setNull(index, Types.DOUBLE);
            return;
        }
        statement.setDouble(index, num);
    }

    public static void setStatementInt(PreparedStatement statement,
                                          int index,
                                          Integer num) throws SQLException {
        if (num == null) {
            statement.setNull(index, Types.INTEGER);
            return;
        }
        statement.setInt(index, num);
    }

}
