package com.gs.cloud.warehouse.robot.process;

import com.google.gson.JsonObject;
import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.*;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DiagnosisTimelineCoGroupProcessor extends RichCoGroupFunction<BaseEntity, BaseEntity, BaseEntity> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void coGroup(Iterable<BaseEntity> first, Iterable<BaseEntity> second, Collector<BaseEntity> out) throws Exception {

        ArrayList<RobotDiagnosisData> robotDiagnosisDatas = new ArrayList<>();
        List<BaseEntity> firstList = (List<BaseEntity>) first;
        List<BaseEntity> secondList = (List<BaseEntity>) second;
        if (firstList.isEmpty() && secondList.isEmpty()) {
            return;
        }
        conver(firstList, secondList, robotDiagnosisDatas);

        Collections.sort(robotDiagnosisDatas, (o1, o2) -> {
            if (!o1.getTimestampUtc().equals(o2.getTimestampUtc())) {
                return o1.getTimestampUtc().compareTo(o2.getTimestampUtc());
            } else {
                return o1.getCldTimestampUtc().compareTo(o2.getCldTimestampUtc());
            }
        });

        for (RobotDiagnosisData robotDiagnosisData : robotDiagnosisDatas) {
            out.collect(robotDiagnosisData);
        }
    }
    private void conver(List<BaseEntity> firstList,  List<BaseEntity> secondList, ArrayList<RobotDiagnosisData> robotDiagnosisDatas) {
        // 在离线数据
        for (BaseEntity firstBaseEntity : firstList) {
            procrssRobotState(robotDiagnosisDatas, (RobotState) firstBaseEntity);
        }
        //  状态/排班任务/告警 数据
        for (BaseEntity secondBaseEntity : secondList) {
            String eventType;
            String eventName;
            RobotDiagnosisData robotDiagnosisData = new RobotDiagnosisData();
            // 状态
            if (isRobotWorkState(secondBaseEntity)) {
                RobotWorkState robotWorkState = (RobotWorkState) secondBaseEntity;
                switch (null == robotWorkState.getWorkStateCode() ? "" : robotWorkState.getWorkStateCode()) {
                    case "330": eventName = "乘梯中";eventType = "WT_TTE";break;
                    case "240": eventName = "导航暂停";eventType = "WT_NP";break;
                    case "170": eventName = "导航中";eventType = "WT_NAV";break;
                    case "360": eventName = "倒计时准备进入低功耗";eventType = "WT_C2ELP";break;
                    case "300": eventName = "等候工作站";eventType = "WT_WFW";break;
                    case "380": eventName = "低功耗中";eventType = "WT_ILP";break;
                    case "200": eventName = "工作站加排水";eventType = "WT_WAD";break;
                    case "320": eventName = "候梯中";eventType = "WT_WFE";break;
                    case "100": eventName = "空闲中";eventType = "WT_IDL";break;
                    case "140": eventName = "录制路径";eventType = "WT_RP";break;
                    case "150": eventName = "录制区域";eventType = "WT_RA";break;
                    case "340": eventName = "去往乘梯点";eventType = "WT_GTEP";break;
                    case "250": eventName = "扫描地图";eventType = "WT_STM";break;
                    case "180": eventName = "手动充电中";eventType = "WT_MC";break;
                    case "210": eventName = "手动作业";eventType = "WT_MO";break;
                    case "110": eventName = "未初始化";eventType = "WT_NI";break;
                    case "0":eventName = "未定义";eventType = "WT_UD";break;
                    case "310": eventName = "预约工作站";eventType = "WT_RWS";break;
                    case "270": eventName = "远程唤醒";eventType = "WT_RWU";break;
                    case "260": eventName = "暂停乘梯";eventType = "WT_PTE";break;
                    case "350": eventName = "召唤清洁到达召唤点";eventType = "WT_SCTCP";break;
                    case "370": eventName = "正在进入低功耗";eventType = "WT_ELP";break;
                    case "390": eventName = "正在退出低功耗";eventType = "WT_XLP";break;
                    case "190": eventName = "自动充电中";eventType = "WT_AC";break;
                    case "220": eventName = "自动任务暂停";eventType = "WT_ATP";break;
                    case "230": eventName = "自动任务中";eventType = "WT_ATIP";break;
                    case "400": eventName = "手动转场";eventType = "WT_MT";break;
                    case "410": eventName = "休息中";eventType = "WT_REST";break;
                    case "420": eventName = "一类停止";eventType = "WT_TTS";break;
                    case "430": eventName = "前往闸机";eventType = "WT_GTG";break;
                    case "431": eventName = "等候闸机";eventType = "WT_WTG";break;
                    case "432": eventName = "正在通过闸机";eventType = "WT_PTG";break;
                    default: eventType = ""; eventName = "";
                }
                robotDiagnosisData.setId(String.join(":",eventType,robotWorkState.getProductId(),converDateFormat (robotWorkState.getCreatedAtT().getTime())));
                robotDiagnosisData.setEventType(eventType);
                robotDiagnosisData.setEventName(eventName);
                robotDiagnosisData.setProductId(robotWorkState.getProductId());
                robotDiagnosisData.setTimestampUtc(robotWorkState.getCreatedAtT());
                robotDiagnosisData.setCldTimestampUtc(robotWorkState.getRecvTimestampT());
                //  空值处理掉
                robotDiagnosisData.setAttachedWorkStatus(eventName);
                robotDiagnosisData.setName(robotWorkState.getWorkState());
                robotDiagnosisData.setPriority(0);
                robotDiagnosisData.setSource(2);

            }
            // 告警
            else if (isIncidentEvent(secondBaseEntity)) {
                IncidentEvent incidentEvent = (IncidentEvent) secondBaseEntity;
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("IncidentCode",incidentEvent.getIncidentCode());
                jsonObject.addProperty("IncidentStartTime",converDateFormat (incidentEvent.getIncidentStartTime().getTime()));
                robotDiagnosisData.setData(jsonObject.toString());
                // 任务开始 && 任务结束
                if (incidentEvent.isTaskStatus()){
                    incidentEvent.isTaskEnd();
                    if(incidentEvent.isTaskStart()){
                        eventType="TASK_START";
                        eventName = "任务开始";
                        robotDiagnosisData.setPriority(1);
                    }else {
                        eventType="TASK_END";
                        eventName = "任务结束";
                        robotDiagnosisData.setPriority(2);
                    }
                    robotDiagnosisData.setSource(3);
                    robotDiagnosisData.setData("["+jsonObject+"]");
                    robotDiagnosisData.setAttachedTask(eventName);
                    robotDiagnosisData.setEventType(eventType);
                    robotDiagnosisData.setEventName(eventName);
                }
                // 关机 & 重启
                else if (isShutdownAndRestartCode(incidentEvent.getIncidentCode())) {
                    if (incidentEvent.getFinalized().equals("1")) {
                        switch (incidentEvent.getIncidentCode()){
                            case "20799": eventType = "WT_ST";eventName = "主动关机";break;
                            case "20798": eventType = "WT_KST";eventName = "主动关机";break;
                            case "20724": eventType = "WT_TAR";eventName= "主动重启";break;
                            case "20728": eventType = "WT_TARB";eventName= "主动重启";break;
                            case "20801": eventType = "WT_RRM";eventName= "主动重启";break;
                            case "20802": eventType = "WT_RRU";eventName= "主动重启";break;
                            case "20803": eventType = "WT_LRM";eventName= "主动重启";break;
                            case "20804": eventType = "WT_RUL";eventName= "主动重启";break;
                            case "20805": eventType = "WT_OTAR";eventName= "主动重启";break;
                            case "20806": eventType = "WT_TZR";eventName= "主动重启";break;
                            case "20808": eventType = "WT_LRU";eventName= "主动重启";break;
                            case "10199": eventType = "WT_UAR";eventName="异常关机";break;
                            case "10172": eventType = "WT_UAST";eventName="异常关机";break;
                            case "11034": eventType = "WT_MOTL";eventName="异常关机";break;
                            case "10200": eventType = "WT_ASRLP";eventName="低电关机";break;
                            case "20800": eventType = "WT_LPO";eventName="低电关机";break;
                            case "10143": eventType = "WT_ASR";eventName="异常重启";break;
                            case "20807": eventType = "WT_ASO";eventName="异常重启";break;
                            case "10175": eventType = "WT_UMB";eventName="异常重启";break;
                            case "30067": eventType = "WT_ASRBV";eventName="异常重启";break;
                            case "20604": eventType = "WT_SIS";eventName="开机";break;
                            case "20605": eventType = "WT_SIF";eventName="开机";break;
                            default: eventType = "";eventName="";
                        }
                        robotDiagnosisData.setAttachedWorkStatus(eventName);
                        robotDiagnosisData.setEventType(eventType);
                        robotDiagnosisData.setEventName(eventName);
                        robotDiagnosisData.setPriority(0);
                        robotDiagnosisData.setSource(2);
                    }else {
                        continue;
                    }
                }
                // 急停告警补状态
                else if (isEssCode(incidentEvent.getIncidentCode())) {
                    eventType = incidentEvent.getFinalized().equals("0") ? "ESS_ON" : "ESS_OFF";
                    eventName = incidentEvent.getFinalized().equals("0") ? "急停开启" : "急停关闭";
                    robotDiagnosisData.setAttachedEssStatus(eventName);
                    robotDiagnosisData.setEventType(eventType);
                    robotDiagnosisData.setEventName(eventName);
                    robotDiagnosisData.setPriority(incidentEvent.getFinalized().equals("0") ? 3 : 4);
                    robotDiagnosisData.setSource(4);
                }
                // 普通告警
                else {
                    eventType = incidentEvent.getFinalized().equals("0") ? "INCI_START" : "INCI_END";
                    eventName = incidentEvent.getFinalized().equals("0") ? "告警开始" : "告警恢复";
                    robotDiagnosisData.setEventType(eventType);
                    robotDiagnosisData.setEventName(eventName);
                    robotDiagnosisData.setPriority(incidentEvent.getFinalized().equals("0") ? 3 : 4);
                    robotDiagnosisData.setSource(4);
                }
                Date eventTime = incidentEvent.getFinalized().equals("0") ? incidentEvent.getIncidentStartTime() : incidentEvent.getEntityEventTime();
                robotDiagnosisData.setId(String.join(":", eventType, incidentEvent.getProductId(), incidentEvent.getIncidentCode(), converDateFormat (eventTime.getTime())));
                robotDiagnosisData.setProductId(incidentEvent.getProductId());
                robotDiagnosisData.setCode(incidentEvent.getIncidentCode());
                robotDiagnosisData.setLevel(incidentEvent.getIncidentLevel());
                robotDiagnosisData.setName(incidentEvent.getIncidentTitle());
                robotDiagnosisData.setTimestampUtc(eventTime);
                robotDiagnosisData.setCldTimestampUtc(incidentEvent.getUpdateTime());
            }
            // 排班任务
            else if (isRobotPlanTask(secondBaseEntity)){
                procrssRobotPlanTask((RobotPlanTask) secondBaseEntity, robotDiagnosisData);
            }else if (isEventTicket(secondBaseEntity)){
                procrssEventTicket((EventTicket) secondBaseEntity, robotDiagnosisData);
            }else if (isRobotWorkState2(secondBaseEntity)){
                procrssRobotState2((RobotState2)secondBaseEntity,robotDiagnosisData);
            }

            robotDiagnosisDatas.add(robotDiagnosisData);
        }
    }


    private void procrssRobotState2(RobotState2 secondBaseEntity, RobotDiagnosisData robotDiagnosisData) {
        RobotState2 robotState2 = secondBaseEntity;
        String eventName=String.join("-",robotState2.getCloudStatusKeyCn(), robotState2.getCloudStatusValueCn());
        String eventType;
        switch (robotState2.getCloudStatusKey()){
            case "LOCATION_STATUS": eventType= "WT2_LS";break;
            case "REBOOTING": eventType= "WT2_R";break;
            case "NAVI_STATUS": eventType= "WT2_NS";break;
            case "CURRENT_ELEVATOR_STATUS_FIRST_GRADE": eventType= "WT2_E_F";break;
            case "CURRENT_ELEVATOR_STATUS_SECOND_GRADE": eventType= "WT2_E_S";break;
            case "WORK_STATION_STATE_FIRST_GRADE": eventType= "WT2_WS_F";break;
            case "WORK_STATION_STATE_SECOND_GRADE": eventType= "WT2_WS_S";break;
            case "DEPLOY_STATUS_FIRST_GRADE": eventType= "WT2_D_F";break;
            case "DEPLOY_STATUS_SECOND_GRADE": eventType= "WT2_D_S";break;
            case "TASK_STATUS_FIRST_GRADE": eventType= "WT2_T_F";break;
            case "TASK_STATUS_SECOND_GRADE": eventType= "WT2_T_S";break;
            case "TASK_PAUSE_REASON_SUM": eventType= "WT2_T_P";break;
            case "RECORD_PATH_STATUS_FIRST_GRADE": eventType= "WT2_RP_F";break;
            case "RECORD_PATH_STATUS_SECOND_GRADE": eventType= "WT2_RP_S";break;
            case "SCANNING_MAP_STATUS_FIRST_GRADE": eventType= "WT2_SM_F";break;
            case "SCANNING_MAP_STATUS_SECOND_GRADE": eventType= "WT2_SM_S";break;
            default:eventType="";
        }
        robotDiagnosisData.setId(String.join(":",eventType,robotState2.getProductId(),converDateFormat(robotState2.getEventTime().getTime())));
        robotDiagnosisData.setProductId(robotState2.getProductId());
        robotDiagnosisData.setTimestampUtc(robotState2.getLocalTimestampT());
        robotDiagnosisData.setCldTimestampUtc(robotState2.getUpdateTime());
        robotDiagnosisData.setEventType(eventType);
        robotDiagnosisData.setEventName(eventName);
        robotDiagnosisData.setName(eventName);
        robotDiagnosisData.setStateKeyCn(robotState2.getCloudStatusKeyCn());
        robotDiagnosisData.setStateValueCn(robotState2.getCloudStatusValueCn());
        robotDiagnosisData.setSource(7);
        robotDiagnosisData.setPriority(7);
    }
    private void procrssEventTicket(EventTicket secondBaseEntity, RobotDiagnosisData robotDiagnosisData) {
        EventTicket eventTicket = secondBaseEntity;
        String eventName = "创建事件单";
        String eventType = "CREATE_EVENT_TICKET";

        robotDiagnosisData.setId(String.join(":", eventType, eventTicket.getProductId(),eventTicket.getTicketNum(),converDateFormat(eventTicket.getCreateTime().getTime())));
        robotDiagnosisData.setProductId(eventTicket.getProductId());
        robotDiagnosisData.setTimestampUtc(eventTicket.getEventTime());
        robotDiagnosisData.setCldTimestampUtc(eventTicket.getEventTime());
        robotDiagnosisData.setEventType(eventType);
        robotDiagnosisData.setEventName(eventName);
        robotDiagnosisData.setName(eventName);
        robotDiagnosisData.setPriority(6);
        robotDiagnosisData.setSource(6);

    }
    private void procrssRobotPlanTask(RobotPlanTask secondBaseEntity, RobotDiagnosisData robotDiagnosisData) {
        RobotPlanTask robotPlanTask = secondBaseEntity;
        robotDiagnosisData.setId(String.join(":", "SST", robotPlanTask.getProductId(), String.valueOf(robotPlanTask.getId()), converDateFormat((robotPlanTask.getPlanTaskStartTimeMS()))));
        robotDiagnosisData.setProductId(robotPlanTask.getProductId());
        robotDiagnosisData.setTimestampUtc(robotPlanTask.getEventTime());
        robotDiagnosisData.setCldTimestampUtc(robotPlanTask.getCldTimestampUTC());
        robotDiagnosisData.setEventType("SST");
        robotDiagnosisData.setEventName("启动排班任务");
        robotDiagnosisData.setName(robotPlanTask.getPlanTaskName());
        robotDiagnosisData.setData(robotPlanTask.getTaskInfo());
        robotDiagnosisData.setSource(5);
    }
    private void procrssRobotState(ArrayList<RobotDiagnosisData> robotDiagnosisDatas, RobotState firstBaseEntity) {
        RobotDiagnosisData robotDiagnosisData = new RobotDiagnosisData();
        RobotState robotState = firstBaseEntity;
        String eventName;
        String eventType;
        if (robotState.getIsOnline() == 1) {
            eventName = "在线";
            eventType = "O2O_ONLINE";
        } else {
            eventName = "离线";
            eventType = "O2O_OFFLINE";
        }

        robotDiagnosisData.setId(String.join(":",eventType,robotState.getProductId(), converDateFormat (robotState.getLocalTimestamp()*1000)));
        robotDiagnosisData.setProductId(robotState.getProductId());
        robotDiagnosisData.setTimestampUtc(new Date(robotState.getLocalTimestamp() * 1000L));
        robotDiagnosisData.setCldTimestampUtc(robotState.getCldDate());

        robotDiagnosisData.setAttachedOnlineStatus(eventName);
        robotDiagnosisData.setEventType(eventType);
        robotDiagnosisData.setEventName(eventName);
        robotDiagnosisData.setName(eventName);
        robotDiagnosisData.setPriority(0);
        robotDiagnosisData.setSource(1);

        robotDiagnosisDatas.add(robotDiagnosisData);
    }

    private boolean isIncidentEvent(BaseEntity value) {
        return value instanceof IncidentEvent;
    }

    private boolean isRobotWorkState(BaseEntity value) {
        return value instanceof RobotWorkState;
    }

    private boolean isRobotPlanTask(BaseEntity value) {
        return value instanceof RobotPlanTask;
    }
    private boolean isEventTicket(BaseEntity value) {
        return value instanceof EventTicket;
    }

    private boolean isRobotWorkState2(BaseEntity value) {
        return value instanceof RobotState2;
    }

    // 是否是急停code
    private boolean isEssCode(String incidentCode) {
        return incidentCode.equals("20022") || incidentCode.equals("14030");
    }

    // 关机&重启code
    private boolean isShutdownAndRestartCode(String incidentCode) {
        return incidentCode.equals("20799") ||
                incidentCode.equals("20798") ||
                incidentCode.equals("20724") ||
                incidentCode.equals("20728") ||
                incidentCode.equals("20801") ||
                incidentCode.equals("20802") ||
                incidentCode.equals("20803") ||
                incidentCode.equals("20804") ||
                incidentCode.equals("20805") ||
                incidentCode.equals("20806") ||
                incidentCode.equals("20808") ||
                incidentCode.equals("10199") ||
                incidentCode.equals("11034") ||
                incidentCode.equals("20800") ||
                incidentCode.equals("10200") ||
                incidentCode.equals("10143") ||
                incidentCode.equals("30067") ||
                incidentCode.equals("20807") ||
                incidentCode.equals("20605") ||
                incidentCode.equals("20604") ||
                incidentCode.equals("10172") ||
                incidentCode.equals("10175");
    }

    private String converDateFormat (Long timestamp){
        LocalDateTime dateTimeInGmt8 = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("Asia/Shanghai"));
        return dateTimeInGmt8.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
