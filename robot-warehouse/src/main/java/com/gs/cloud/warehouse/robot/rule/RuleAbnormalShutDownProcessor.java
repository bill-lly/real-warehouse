package com.gs.cloud.warehouse.robot.rule;

import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.rule.Rule;
import com.gs.cloud.warehouse.robot.entity.rule.RuleAbnormalShutDown;
import com.gs.cloud.warehouse.robot.enums.OnlineEnum;
import com.gs.cloud.warehouse.robot.enums.TaskStatusEnum;
import com.gs.cloud.warehouse.robot.entity.RuleResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RuleAbnormalShutDownProcessor implements IRuleProcessor{

  private static final Logger LOG = LoggerFactory.getLogger(RuleAbnormalShutDownProcessor.class);
  private final static Long OFFLINE_DURATION = 20*60*1000L;

  @Override
  public RuleResult eval(Rule ele, MonitorWindowStat stat) {
    RuleAbnormalShutDown rule = (RuleAbnormalShutDown) ele;
    Tuple2<Long, String> taskStatus =  stat.getTaskStatus();
    Tuple2<Long, String> currState = stat.getCurrentWorkState();
    Map<String, Integer> map = stat.getCodeIncidentCnt();
    Long robotTimestamp = stat.getLastRobotUnixTimestamp();
    Long cldTimestamp = stat.getCldTimestamp().getTime();
    Long avgOffset = stat.getAvgOffset();
    if (taskStatus == null || currState == null || robotTimestamp == null) {
      return RuleResult.create(rule.getCode());
    }
    if (hasCommunicationError(map)) {
      return RuleResult.create(rule.getCode());
    }
    if (taskStatus.f1.equals(TaskStatusEnum.TASK_START_20102.getStatus()) //任务中
        && currState.f1.equals(OnlineEnum.OFFLINE.getCode()) //当前状态为离线
        && (cldTimestamp - currState.f0 > OFFLINE_DURATION) && (cldTimestamp - robotTimestamp - avgOffset > OFFLINE_DURATION) //离线时长超过20分钟(既要看离线时间，还要看车端时间)
        && (robotTimestamp - taskStatus.f0 - avgOffset < 4*60*60*1000) //只看任务开始4小时内的数据，因为车端有可能会丢失任务开始结束的标签
        ) {
      LOG.info(String.format("机器人异常关机, productId=%s, currState.f0=%s, currState.f1=%s, robotTimestamp=%s, cldTimestamp=%s, avgOffset=%s",
          stat.getProductId(), currState.f0, currState.f1, robotTimestamp, cldTimestamp, avgOffset));
      return RuleResult.createError(rule.getCode(), "机器人异常关机");
    }
    return RuleResult.create(rule.getCode());
  }

  private boolean hasCommunicationError(Map<String, Integer> map) {
    Integer cnt11048 = map.getOrDefault("11048", 0);
    return cnt11048>0;
  }
}
