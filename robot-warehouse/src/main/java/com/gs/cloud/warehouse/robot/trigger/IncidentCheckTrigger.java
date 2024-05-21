package com.gs.cloud.warehouse.robot.trigger;

import com.gs.cloud.warehouse.robot.entity.RobotWorkState;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class IncidentCheckTrigger extends Trigger<RobotWorkState, TimeWindow> {

  @Override
  public TriggerResult onElement(
      RobotWorkState element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
    if ("100".equals(element.getWorkStateCode())) {
      return TriggerResult.FIRE;
    }
    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
      // if the watermark is already past the window fire immediately
      return TriggerResult.FIRE;
    } else {
      ctx.registerEventTimeTimer(window.maxTimestamp());
      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return time == window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
      throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

  @Override
  public boolean canMerge() {
    return false;
  }

  @Override
  public void onMerge(TimeWindow window, OnMergeContext ctx) {}

  @Override
  public String toString() {
    return "IncidentCheckTrigger()";
  }

  public static IncidentCheckTrigger create() {
    return new IncidentCheckTrigger();
  }
}
