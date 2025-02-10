package com.gs.cloud.warehouse.robot.source;


import com.gs.cloud.warehouse.robot.entity.RobotPlanTask;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;


public class PlanTaskSource extends RichSourceFunction<RobotPlanTask> implements CheckpointedFunction {
    private static final long serialVersionUID = 3334654984018091675L;
    private ListState<Tuple2<String, Long>> checkpointedState;
    private volatile boolean isRunning = true;
    private final String jdbcUrl;
    private final String userName;
    private final String password;
    private final Long startTimeStamp;
    private Connection connect = null;
    private PreparedStatement ps = null;
    private transient String lastReadDate;
    private transient Long queryStartTime;
    private static final Long OneMinuteSecond = 60l;

    public PlanTaskSource(Properties properties, ParameterTool params) {
        this.jdbcUrl = properties.getProperty("robotbi.jdbc.url");
        this.userName = properties.getProperty("robotbi.jdbc.user.name");
        this.password = properties.getProperty("robotbi.jdbc.password");
        this.startTimeStamp=params.has("planTaskStartTime")?params.getLong("planTaskStartTime"):null;

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化链接
        Class.forName("com.mysql.cj.jdbc.Driver");
        connect = DriverManager.getConnection(jdbcUrl, userName, password);
        super.open(parameters);
    }


    @Override
    public void run(SourceContext<RobotPlanTask> collect) throws Exception {
        while (isRunning) {
            long currentTimestamp = System.currentTimeMillis();
            // 每天重新初始化一次查询参数，从GMT+8的0点开始查询 && 当日数据查询完毕
            if (!lastReadDate.equals(getPartitionDate(currentTimestamp))) {
                inint(currentTimestamp);
            }
            synchronized (collect.getCheckpointLock()) {
                final long currentTs = currentTimestamp / 1000;
                // 只发送当前时间一分钟内的任务
                if (queryStartTime - currentTs <= 0) {
                    String sql =
                            "select product_id,\n" +
                            "       time_zone_id,\n" +
                            "       plan_task_name,\n" +
                            "       plan_task_start_time_ts\n" +
                            "from robot_plan_task \n" +
                            "where biz_date = ? \n" +
                            "  and plan_task_start_time_ts >= ? \n" +
                            "  and plan_task_start_time_ts < ? \n" +
                            "order by plan_task_start_time_utc,id;";

                    ps = connect.prepareStatement(sql);
                    ps.setString(1, lastReadDate);
                    ps.setLong(2, queryStartTime);
                    ps.setLong(3, (queryStartTime + OneMinuteSecond));

                    ResultSet resultSet = ps.executeQuery();
                    int queryDataRows = 0;
                    while (resultSet.next()) {
                        RobotPlanTask robotPlanTask = new RobotPlanTask();
                        robotPlanTask.setEventTime(new Date(resultSet.getLong(4) * 1000));
                        robotPlanTask.setProductId(resultSet.getString(1));
                        robotPlanTask.setTimeZone(resultSet.getString(2));
                        robotPlanTask.setPlanTaskName(resultSet.getString(3));
                        robotPlanTask.setPlanTaskStartTimeUTC(new Date(resultSet.getLong(4) * 1000));
                        robotPlanTask.setPlanTaskStartTimeMS(resultSet.getLong(4) * 1000);
                        robotPlanTask.setPlanTaskStartTimeLocal(getLocalDate(resultSet.getLong(4),
                                resultSet.getString(2)));
                        queryDataRows++;
                        collect.collect(robotPlanTask);
                    }
                    if (queryDataRows == 0) {
                        RobotPlanTask robotPlanTask = new RobotPlanTask();
                        robotPlanTask.setEventTime(new Date((queryStartTime) * 1000));
                        robotPlanTask.setProductId("");
                        robotPlanTask.setTimeZone("Asia/Shanghai");
                        robotPlanTask.setPlanTaskName("");
                        robotPlanTask.setPlanTaskStartTimeUTC(new Date((queryStartTime) * 1000));
                        robotPlanTask.setPlanTaskStartTimeMS((queryStartTime) * 1000);
                        robotPlanTask.setPlanTaskStartTimeLocal(getLocalDate((queryStartTime), "Asia/Shanghai"));
                        collect.collect(robotPlanTask);
                    }
                    queryStartTime = queryStartTime + OneMinuteSecond;
                }
            }
        }
    }

    private void inint(long currentTimestamp) {
        lastReadDate = getPartitionDate(currentTimestamp);
        // 每天从GMT+8:00的0点开始
        queryStartTime = getCurrentDayTimestamp(currentTimestamp);
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connect != null) {
                connect.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            isRunning = false;
        }
    }

    /**
     * @return 获取分区日期，当日计算数据T-1
     */
    private String getPartitionDate(long currentTimestamp) {

        LocalDateTime currentDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentTimestamp), ZoneId.of(
                "Asia/Shanghai"));
        LocalDateTime previousDateTime = currentDateTime.minusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String readPartitionDate = previousDateTime.format(formatter);
        return readPartitionDate;
    }

    private Long getCurrentDayTimestamp(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("Asia/Shanghai"));
        // 获取当天的日期
        LocalDate localDate = zonedDateTime.toLocalDate();
        // 构造当天的0点时间
        LocalTime midnight = LocalTime.MIDNIGHT;
        ZonedDateTime targetDateTime = localDate.atTime(midnight).atZone(ZoneId.of("Asia/Shanghai"));
        // 转换为毫秒级时间戳
        long targetTimestamp = targetDateTime.toInstant().toEpochMilli();
        // 获取当日时间戳
        return targetTimestamp / 1000;
    }

    /**
     * 获取本地时间
     *
     * @param timestamp 时间戳精确到秒
     * @param timeZone  机器时区
     * @return
     */
    private Date getLocalDate(long timestamp, String timeZone) {
        TimeZone localTimeZone = TimeZone.getTimeZone(timeZone);
        Date utcDate = new Date(timestamp * 1000);
        long utcTimeInMillis = utcDate.getTime();
        int offset = localTimeZone.getOffset(utcTimeInMillis);
        long localTimeInMillis = utcTimeInMillis + offset;
        return new Date(localTimeInMillis);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        checkpointedState.add(new Tuple2<>(lastReadDate, queryStartTime));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>("checkpointedState",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            final Iterable<Tuple2<String, Long>> states = checkpointedState.get();
            for (Tuple2<String, Long> state : states) {
                lastReadDate = state.f0;
                queryStartTime = state.f1;
            }
        }else {
            long currentTimestamp = System.currentTimeMillis();
            // 初始化查询日期
            if (null==startTimeStamp){
                lastReadDate = getPartitionDate(currentTimestamp);
                // 从系统时间开始查询
                queryStartTime = currentTimestamp/1000 - (currentTimestamp/1000 % 60);
            }else {
                lastReadDate = getPartitionDate(startTimeStamp * 1000);
                queryStartTime = startTimeStamp - (startTimeStamp % 60);
            }
        }
    }
}