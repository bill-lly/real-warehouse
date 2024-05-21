package com.gs.cloud.warehouse.robot.trigger;

import com.gs.cloud.warehouse.robot.entity.RobotWorkState;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

public class SingleElementWindows extends WindowAssigner<RobotWorkState, TimeWindow>  {

  protected long defaultWaitingTime;

  public SingleElementWindows(long defaultWaitingTime) {
    if (defaultWaitingTime <= 0) {
      throw new IllegalArgumentException(
          "SingleElementWindows parameters must satisfy 0 < size");
    }
    this.defaultWaitingTime = defaultWaitingTime;
  }

  @Override
  public Collection<TimeWindow> assignWindows(RobotWorkState element, long timestamp, WindowAssignerContext context) {
    return Collections.singletonList(new TimeWindow(timestamp, timestamp + defaultWaitingTime));
  }

  @Override
  public Trigger<RobotWorkState, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
    return IncidentCheckTrigger.create();
  }

  @Override
  public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
    return new TimeWindow.Serializer();
  }

  @Override
  public boolean isEventTime() {
    return true;
  }

  public static SingleElementWindows create(Time size) {
    return new SingleElementWindows(size.toMilliseconds());
  }
}
