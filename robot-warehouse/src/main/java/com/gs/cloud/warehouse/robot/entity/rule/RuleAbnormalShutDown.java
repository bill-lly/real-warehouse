package com.gs.cloud.warehouse.robot.entity.rule;

public class RuleAbnormalShutDown extends Rule {


  public RuleAbnormalShutDown() {
    super("199001");
  }

  @Override
  public String getProcessor() {
    return "RuleAbnormalShutDown";
  }
}
