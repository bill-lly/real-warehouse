package com.gs.cloud.warehouse.robot.rule;

import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.rule.Rule;
import com.gs.cloud.warehouse.robot.entity.RuleResult;

public interface IRuleProcessor {

  public RuleResult eval(Rule rule, MonitorWindowStat stat);

}
