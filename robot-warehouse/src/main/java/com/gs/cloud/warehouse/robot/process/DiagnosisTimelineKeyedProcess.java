package com.gs.cloud.warehouse.robot.process;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.RobotDiagnosisData;
import com.gs.cloud.warehouse.robot.lookup.RobotStateLookupFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.binary.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.time.Time;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class DiagnosisTimelineKeyedProcess extends KeyedProcessFunction<String, BaseEntity, BaseEntity> {
    private transient ValueState<Date> lastTaskStartTimeState;
    private transient ValueState<String> lastStateState;
    private transient ValueState<String> lastOnlineState;
    private transient ValueState<String> lastEssState;
    private transient ValueState<String> lastTaskStateState;
    private transient ValueState<String> lastRobotState2State;
    private RobotStateLookupFunction robotStateLookupFunction;

    public DiagnosisTimelineKeyedProcess(Properties properties) throws Exception {
        robotStateLookupFunction = new RobotStateLookupFunction(properties);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        robotStateLookupFunction.open(parameters);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        ValueStateDescriptor<Date> lastTaskStartTimeStateDescriptor =
                new ValueStateDescriptor<>(
                        "lastTaskStartTimeState",
                        BasicTypeInfo.DATE_TYPE_INFO);
        ValueStateDescriptor<String> lastStateDescriptor =
                new ValueStateDescriptor<>(
                        "lastStateState",
                        BasicTypeInfo.STRING_TYPE_INFO);
        ValueStateDescriptor<String> lastOnlineDescriptor =
                new ValueStateDescriptor<>(
                        "lastOnlineState",
                        BasicTypeInfo.STRING_TYPE_INFO);
        ValueStateDescriptor<String> lastEssDescriptor =
                new ValueStateDescriptor<>(
                        "lastEssState",
                        BasicTypeInfo.STRING_TYPE_INFO);
        ValueStateDescriptor<String> lastTaskStateStateDescriptor =
                new ValueStateDescriptor<>(
                        "lastTaskStateState",
                        BasicTypeInfo.STRING_TYPE_INFO);
        ValueStateDescriptor<String> lastRobotState2Descriptor =
                new ValueStateDescriptor<>(
                        "lastRobotState2State",
                        BasicTypeInfo.STRING_TYPE_INFO);

        lastTaskStartTimeStateDescriptor.enableTimeToLive(ttlConfig);
        lastStateDescriptor.enableTimeToLive(ttlConfig);
        lastOnlineDescriptor.enableTimeToLive(ttlConfig);
        lastEssDescriptor.enableTimeToLive(ttlConfig);
        lastTaskStateStateDescriptor.enableTimeToLive(ttlConfig);
        lastRobotState2Descriptor.enableTimeToLive(ttlConfig);

        lastTaskStartTimeState = getRuntimeContext().getState(lastTaskStartTimeStateDescriptor);
        lastStateState = getRuntimeContext().getState(lastStateDescriptor);
        lastOnlineState = getRuntimeContext().getState(lastOnlineDescriptor);
        lastEssState = getRuntimeContext().getState(lastEssDescriptor);
        lastTaskStateState = getRuntimeContext().getState(lastTaskStateStateDescriptor);
        lastRobotState2State = getRuntimeContext().getState(lastRobotState2Descriptor);

        super.open(parameters);
    }

    @Override
    public void processElement(BaseEntity value, KeyedProcessFunction<String, BaseEntity, BaseEntity>.Context ctx, Collector<BaseEntity> out) throws Exception {

        Date lastTaskStartTime = lastTaskStartTimeState.value();
        String lastTaskState = lastTaskStateState.value();
        String lastState = lastStateState.value();
        String lastOnline = lastOnlineState.value();
        String lastEss = lastEssState.value();
        String lastRobotState2 = lastRobotState2State.value();

        RobotDiagnosisData robotDiagnosisData = (RobotDiagnosisData)value;
        if (null == lastOnline && null== lastState && null == lastRobotState2){
            // 要去查mysql状态数据
            List<BaseEntity> baseEntities = robotStateLookupFunction.processElement(robotDiagnosisData);
            if (!baseEntities.isEmpty()){
                robotDiagnosisData = (RobotDiagnosisData)baseEntities.get(0);
            }
        }

        updateState(robotDiagnosisData, lastTaskState, lastEss, lastOnline, lastState, lastTaskStartTime,lastRobotState2);
        setRobotState(robotDiagnosisData, lastTaskState, lastEss, lastOnline, lastState, lastTaskStartTime,lastRobotState2);

        // 输出两条 存下当前状态持续时间
        if (1 == robotDiagnosisData.getSource() || 2 == robotDiagnosisData.getSource() || 7 == robotDiagnosisData.getSource()) {
            if (!StringUtils.equals(robotDiagnosisData.getAttachedWorkStatus(), lastState)
                    || !(StringUtils.equals(robotDiagnosisData.getAttachedOnlineStatus(), lastOnline))
                    || !(StringUtils.equals(String.join("-",robotDiagnosisData.getStateKeyCn(),robotDiagnosisData.getStateValueCn()), lastRobotState2))
            ) {
                if (!StringUtils.equals(robotDiagnosisData.getAttachedWorkStatus(), lastState)){
                    robotDiagnosisData.setData("{\"state_switch\":\"" + String.join("->", lastState, robotDiagnosisData.getAttachedWorkStatus()) + "\"}");
                }
                if (!(StringUtils.equals(robotDiagnosisData.getAttachedOnlineStatus(), lastOnline))){
                    robotDiagnosisData.setData("{\"state_switch\":\"" + String.join("->", lastOnline, robotDiagnosisData.getAttachedOnlineStatus()) + "\"}");
                }
                if (!(StringUtils.equals(String.join("-",robotDiagnosisData.getStateKeyCn(),robotDiagnosisData.getStateValueCn()), lastRobotState2))){
                    robotDiagnosisData.setData("{\"state_switch\":\"" + String.join("->", lastRobotState2, String.join("-",robotDiagnosisData.getStateKeyCn(),robotDiagnosisData.getStateValueCn())) + "\"}");
                }

                lastOnline = robotDiagnosisData.getAttachedOnlineStatus();
                lastState = robotDiagnosisData.getAttachedWorkStatus();
                lastRobotState2=String.join("-",robotDiagnosisData.getStateKeyCn(),robotDiagnosisData.getStateValueCn());
                updateState(robotDiagnosisData, lastTaskState, lastEss, lastOnline, lastState, lastTaskStartTime,lastRobotState2);

                out.collect(robotDiagnosisData);
            }
        }
        else {

            if ("任务结束".equals(robotDiagnosisData.getAttachedTask())){
                lastTaskState= null;
                lastTaskStateState.clear();
            }
            if ("急停关闭".equals(robotDiagnosisData.getAttachedEssStatus())){
                lastEss=null;
                lastEssState.clear();
            }
            updateState(robotDiagnosisData, lastTaskState, lastEss, lastOnline, lastState, lastTaskStartTime,lastRobotState2);
            out.collect(robotDiagnosisData);
        }
    }

    private void updateState(RobotDiagnosisData robotDiagnosisData, String lastTaskState, String lastEss, String lastOnline, String lastState, Date lastTaskStartTime, String lastRobotState2) throws IOException {
        // 更新状态
        if (null == lastOnline) {
            lastOnline = robotDiagnosisData.getAttachedOnlineStatus();
        }
        if (null == lastState) {
            lastState = robotDiagnosisData.getAttachedWorkStatus();
        }
        if (null == lastTaskState && "任务开始".equals(robotDiagnosisData.getAttachedTask())) {
            lastTaskState = robotDiagnosisData.getAttachedTask();
            lastTaskStartTime = robotDiagnosisData.getEventTime();
        } else if ("任务结束".equals(robotDiagnosisData.getAttachedTask())) {
            lastTaskState = null;
            lastTaskStartTime = null;
        }
        if (null == lastEss && "急停开启".equals(robotDiagnosisData.getAttachedEssStatus())) {
            lastEss = robotDiagnosisData.getAttachedEssStatus();
        } else if ("急停关闭".equals(robotDiagnosisData.getAttachedEssStatus())) {
            lastEss = null;
        }
        if (null == lastRobotState2){
            if (!(null == robotDiagnosisData.getStateKeyCn() || null == robotDiagnosisData.getStateValueCn())){
                lastRobotState2 = String.join("-", robotDiagnosisData.getStateKeyCn(), robotDiagnosisData.getStateValueCn());
            }
        }
        lastEssState.update(lastEss);
        lastStateState.update(lastState);
        lastOnlineState.update(lastOnline);
        lastTaskStateState.update(lastTaskState);
        lastTaskStartTimeState.update(lastTaskStartTime);
        lastRobotState2State.update(lastRobotState2);
    }

    private static void setRobotState(RobotDiagnosisData robotDiagnosisData, String lastTaskState, String lastEss, String lastOnline, String lastState, Date lastTaskStartTime, String lastRobotState2) {
        if (null == robotDiagnosisData.getAttachedOnlineStatus()) {
            robotDiagnosisData.setAttachedOnlineStatus(lastOnline);
        }
        if (null == robotDiagnosisData.getAttachedWorkStatus()) {
            robotDiagnosisData.setAttachedWorkStatus(lastState);
        }
        if (null == robotDiagnosisData.getAttachedEssStatus()) {
            robotDiagnosisData.setAttachedEssStatus(lastEss);
        }
        if (null == robotDiagnosisData.getAttachedTask() && "任务开始".equals(lastTaskState) && lastTaskStartTime.compareTo(robotDiagnosisData.getEventTime()) < 0) {
            robotDiagnosisData.setAttachedTask("任务执行中");
        }
        if ((null == robotDiagnosisData.getStateKeyCn() || null == robotDiagnosisData.getStateValueCn()) && null != lastRobotState2){
            robotDiagnosisData.setStateKeyCn(lastRobotState2.split("-")[0]);
            robotDiagnosisData.setStateValueCn(lastRobotState2.split("-")[1]);
        }
        robotDiagnosisData.setStartTime(robotDiagnosisData.getTimestampUtc());
    }

}
