package com.gs.cloud.warehouse.robot.rule;

import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.rule.IncidentCntRule;
import com.gs.cloud.warehouse.robot.entity.rule.Rule;
import com.gs.cloud.warehouse.robot.entity.RuleResult;

public class RuleInciCntProcessor implements IRuleProcessor{

  @Override
  public RuleResult eval(Rule ele, MonitorWindowStat stat) {
    IncidentCntRule rule = (IncidentCntRule) ele;
    if (stat.getCodeIncidentCnt().isEmpty()) {
      return RuleResult.create(rule.getCode());
    }
    int cnt = 0;
    for (String incidentCode : rule.getIncidentCodeList()) {
      cnt = cnt + stat.getCodeIncidentCnt().getOrDefault(incidentCode, 0);
    }
    if (cnt > rule.getCnt()) {
      return RuleResult.createError(rule.getCode(),
          String.format("%s故障数超过%s次", rule.getTitle(), rule.getCnt()));
    }
    return RuleResult.create(rule.getCode());
  }
}
