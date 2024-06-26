package com.gs.cloud.warehouse.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EventTimePurgeTrigger extends Trigger<Object, TimeWindow> {

  private static final long serialVersionUID = 1L;

  private EventTimePurgeTrigger() {}

  @Override
  public TriggerResult onElement(
      Object element, long timestamp, TimeWindow window, TriggerContext ctx)
      throws Exception {
    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
      // if the watermark is already past the window fire immediately
      return TriggerResult.FIRE_AND_PURGE;
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
    return true;
  }

  @Override
  public void onMerge(TimeWindow window, OnMergeContext ctx) {
    // only register a timer if the watermark is not yet past the end of the merged window
    // this is in line with the logic in onElement(). If the watermark is past the end of
    // the window onElement() will fire and setting a timer here would fire the window twice.
    long windowMaxTimestamp = window.maxTimestamp();
    if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
      ctx.registerEventTimeTimer(windowMaxTimestamp);
    }
  }

  @Override
  public String toString() {
    return "EventTimeTrigger()";
  }

  /**
   * Creates an event-time trigger that fires once the watermark passes the end of the window.
   *
   * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
   * trigger window evaluation with just this one element.
   */
  public static EventTimePurgeTrigger create() {
    return new EventTimePurgeTrigger();
  }
}
