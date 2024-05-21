package com.gs.cloud.warehouse.robot.process;

import com.gs.cloud.warehouse.robot.entity.RobotStateMapping;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CloseStateProcessor extends KeyedProcessFunction<String, RobotStateMapping, RobotStateMapping> {

  private transient ValueState<RobotStateMapping> unClosedStateMappingState;

  @Override
  public void open(Configuration parameters) throws Exception {
    ValueStateDescriptor<RobotStateMapping> descriptor =
        new ValueStateDescriptor<>(
            "robotStateMapping",
            TypeInformation.of(RobotStateMapping.class));
    unClosedStateMappingState = getRuntimeContext().getState(descriptor);
    super.open(parameters);
  }

  @Override
  public void processElement(RobotStateMapping value,
                             Context ctx,
                             Collector<RobotStateMapping> out) throws Exception {
    RobotStateMapping mapping = unClosedStateMappingState.value();
    if (mapping != null) {
      mapping.setEndTime(value.getStartTime());
      mapping.setRecvEndTime(value.getRecvStartTime());
      out.collect(mapping);
    }
    out.collect(value);
    if (!value.isClosed()) {
      unClosedStateMappingState.update(value);
    }
  }
}
