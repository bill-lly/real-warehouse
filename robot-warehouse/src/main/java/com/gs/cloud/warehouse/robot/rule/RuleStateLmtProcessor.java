package com.gs.cloud.warehouse.robot.rule;

import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.rule.Rule;
import com.gs.cloud.warehouse.robot.entity.rule.WorkStateLimitationRule;
import com.gs.cloud.warehouse.robot.entity.RuleResult;

import java.util.Map;

public class RuleStateLmtProcessor implements IRuleProcessor{

  public RuleResult eval(Rule ele, MonitorWindowStat stat) {
    WorkStateLimitationRule rule = (WorkStateLimitationRule)ele;
    Map<String, Long> maxDuration = stat.getMaxDurationMap();
    Long duration = maxDuration.getOrDefault(rule.getWorkStateCode(), 0L);
    if (duration > rule.getThreshold()) {
      return RuleResult.createError(rule.getCode(),
          String.format("%s持续时间超过%s秒", rule.getWorkState(), rule.getThreshold()));
    }
    return RuleResult.create(rule.getCode());
  }

}
