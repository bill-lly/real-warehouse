package com.gs.cloud.warehouse.robot.entity.rule;

public class RulePlanTaskNotExecution extends Rule {

    public RulePlanTaskNotExecution() {
        super("199002");
    }

    @Override
    public String getProcessor() {
        return "RulePlanTaskNotExecution";
    }
}
