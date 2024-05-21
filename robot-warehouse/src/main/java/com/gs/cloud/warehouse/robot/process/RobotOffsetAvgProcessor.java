package com.gs.cloud.warehouse.robot.process;

import com.gs.cloud.warehouse.robot.entity.RobotTimestamp;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.List;

public class RobotOffsetAvgProcessor
    extends ProcessWindowFunction<RobotTimestamp, RobotTimestamp, String, TimeWindow> {

  private transient ValueState<Long> offsetState;
  private transient ValueState<Integer> cntState;
  private transient ValueState<Date> lastTimestampState;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.hours(36))
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
        .build();
    ValueStateDescriptor<Long> offsetDescriptor =
        new ValueStateDescriptor<>(
            "offsetState",
            BasicTypeInfo.LONG_TYPE_INFO);
    ValueStateDescriptor<Integer> cntDescriptor =
        new ValueStateDescriptor<>(
            "cntState",
            BasicTypeInfo.INT_TYPE_INFO);
    ValueStateDescriptor<Date> lastTimestampDescriptor =
        new ValueStateDescriptor<>(
            "lastTimestampState",
            TypeInformation.of(Date.class));
    offsetDescriptor.enableTimeToLive(ttlConfig);
    cntDescriptor.enableTimeToLive(ttlConfig);
    lastTimestampDescriptor.enableTimeToLive(ttlConfig);
    offsetState = getRuntimeContext().getState(offsetDescriptor);
    cntState = getRuntimeContext().getState(cntDescriptor);
    lastTimestampState = getRuntimeContext().getState(lastTimestampDescriptor);
  }

  @Override
  public void process(String s,
                      Context context,
                      Iterable<RobotTimestamp> elements,
                      Collector<RobotTimestamp> out) throws Exception {
    List<RobotTimestamp> robotTimestampList = (List<RobotTimestamp>)elements;
    if (robotTimestampList.isEmpty()) {
      return;
    }
    Long offset = offsetState.value();
    Integer cnt = cntState.value();
    offset = offset == null ? 0L : offset;
    cnt = cnt == null ? 0 : cnt;
    Date lastTimestamp = lastTimestampState.value();
    //30秒同步一次，当超过2小时的情况下，重置成1小时
    if (cnt > 240) {
      offset = Math.round((double)offset/cnt)*120;
      cnt = 120;
    }
    for (RobotTimestamp ele : robotTimestampList) {
      offset = offset + ele.getOffset();
      cnt++;
      if (lastTimestamp == null) {
        lastTimestamp = ele.getTimestampUtc();
      } else {
        lastTimestamp = lastTimestamp.getTime() > ele.getTimestampUtc().getTime()
            ? lastTimestamp : ele.getTimestampUtc();
      }
    }
    RobotTimestamp robotTimestamp = new RobotTimestamp();
    robotTimestamp.setProductId(robotTimestampList.get(0).getProductId());
    robotTimestamp.setAvgOffset(Math.round((double)offset/cnt));
    robotTimestamp.setCldTimestampUtc(new Date(context.currentWatermark()));
    robotTimestamp.setTimestampUtc(lastTimestamp);
    out.collect(robotTimestamp);
    offsetState.update(offset);
    cntState.update(cnt);
    lastTimestampState.update(lastTimestamp);
  }
}
