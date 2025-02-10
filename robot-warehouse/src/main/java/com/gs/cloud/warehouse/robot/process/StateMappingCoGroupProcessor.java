package com.gs.cloud.warehouse.robot.process;

import com.gs.cloud.warehouse.robot.entity.RobotIsOnline;
import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.RobotStateMapping;
import com.gs.cloud.warehouse.robot.entity.RobotWorkState;
import com.gs.cloud.warehouse.robot.process.impl.StateSearchImpl;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class StateMappingCoGroupProcessor extends RichCoGroupFunction<RobotWorkState, RobotIsOnline, RobotStateMapping> {

  private transient ValueState<RobotStateMapping> robotStateMappingState;
  private final StateSearchImpl stateSearch;
  private final Date t2999;

  public StateMappingCoGroupProcessor(Properties properties) {
    stateSearch = new StateSearchImpl(properties);
    Calendar t = Calendar.getInstance();
    //服务时间是utc时间，写入到mysql的时候会自动+8小时
    t.set(2999, Calendar.DECEMBER, 31, 15, 59, 59);
    t.set(Calendar.MILLISECOND, 0);
    t2999 = t.getTime();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.hours(36))
        .setUpdateType(StateTtlConfig.UpdateType.Disabled)
        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
        .build();
    ValueStateDescriptor<RobotStateMapping> descriptorRobotStateMapping =
        new ValueStateDescriptor<>(
            "RobotStateMapping",
            TypeInformation.of(RobotStateMapping.class));
    descriptorRobotStateMapping.enableTimeToLive(ttlConfig);
    robotStateMappingState = getRuntimeContext().getState(descriptorRobotStateMapping);
    super.open(parameters);
  }

  @Override
  public void coGroup(Iterable<RobotWorkState> first,
                      Iterable<RobotIsOnline> second,
                      Collector<RobotStateMapping> out) throws Exception {
    List<RobotWorkState> workStateList = (List<RobotWorkState>)first;
    List<RobotIsOnline> onlineList = (List<RobotIsOnline>)second;
    if (workStateList.isEmpty() && onlineList.isEmpty()) {
      return;
    }

    RobotStateMapping lastStateMapping = robotStateMappingState.value();
    if (lastStateMapping == null) {
      BaseEntity one = getMinBaseEntity(workStateList, onlineList);
      lastStateMapping = stateSearch.search(one.getKey(), one.getEventTime());
    }

    if (!workStateList.isEmpty() && lastStateMapping != null
        && workStateList.get(0).getWorkStateCode().equals(lastStateMapping.getWorkStateCode())) {
      workStateList.remove(0);
    }

    if (!onlineList.isEmpty() && lastStateMapping != null && lastStateMapping.getIsOnline() != null
        && onlineList.get(0).getIsOnline().intValue() == lastStateMapping.getIsOnline().intValue()) {
      onlineList.remove(0);
    }

    List<BaseEntity> sortedList = new ArrayList<>();
    sortedList.addAll(workStateList);
    sortedList.addAll(onlineList);
    Collections.sort(sortedList);

    if (sortedList.size() == 1) {
      RobotStateMapping firstOne = convert2Mapping(lastStateMapping, sortedList.get(0));
      if (lastStateMapping != null) {
        out.collect(updateEnd(lastStateMapping, firstOne.getStartTime(), firstOne.getRecvStartTime()));
      }
      out.collect(firstOne);
      lastStateMapping = firstOne;
    } else {
      for (int i=0; i<sortedList.size(); i++) {
        RobotStateMapping cur = convert2Mapping(lastStateMapping, sortedList.get(i));
        if (lastStateMapping != null) {
          out.collect(updateEnd(lastStateMapping, cur.getStartTime(), cur.getRecvStartTime()));
        }
        if (i == sortedList.size()-1) {
          out.collect(cur);
        }
        lastStateMapping = cur;
      }
    }
    robotStateMappingState.update(lastStateMapping);
  }

  private BaseEntity getMinBaseEntity(List<RobotWorkState> workStateList, List<RobotIsOnline> onlineList) {
    Date date1 = workStateList.isEmpty() ? null : workStateList.get(0).getEventTime();
    Date date2 = onlineList.isEmpty() ? null : onlineList.get(0).getEventTime();
    if (date1 == null) {
      return onlineList.get(0);
    } else if (date2 == null) {
      return workStateList.get(0);
    } else if (date1.before(date2)) {
      return workStateList.get(0);
    } else {
      return onlineList.get(0);
    }
  }

  private RobotStateMapping updateEnd(RobotStateMapping lastStateMapping, Date startTime, Date recvStartTime) {

    lastStateMapping.setEndTime(startTime);
    lastStateMapping.setRecvEndTime(recvStartTime);
    lastStateMapping.setDuration((lastStateMapping.getEndTime().getTime()-lastStateMapping.getStartTime().getTime())/1000);
    return lastStateMapping;
  }

  private RobotStateMapping convert2Mapping(RobotStateMapping lastMapping, BaseEntity baseEntity) {
    if (baseEntity instanceof RobotWorkState) {
      return convert2Mapping(lastMapping, (RobotWorkState)baseEntity);
    } else {
      return convert2Mapping(lastMapping, (RobotIsOnline)baseEntity);
    }
  }

  private RobotStateMapping convert2Mapping(RobotStateMapping lastMapping, RobotWorkState workState) {
    RobotStateMapping mapping = new RobotStateMapping();
    mapping.setProductId(workState.getProductId());
    mapping.setStartTime(workState.getAdjustCreatedAtT());
    mapping.setEndTime(t2999);
    mapping.setWorkStateCode(workState.getWorkStateCode());
    mapping.setWorkState(workState.getWorkState());
    mapping.setIsOnline(lastMapping != null ? lastMapping.getIsOnline() : null);
    mapping.setDuration(getDuration(workState.getAdjustCreatedAtT()));
    mapping.setRecvStartTime(workState.getRecvTimestampT());
    mapping.setRecvEndTime(workState.getRecvTimestampT());
    return mapping;
  }

  private RobotStateMapping convert2Mapping(RobotStateMapping lastMapping, RobotIsOnline isOnline) {
    RobotStateMapping mapping = new RobotStateMapping();
    mapping.setProductId(isOnline.getProductId());
    mapping.setStartTime(isOnline.getCldDate());
    mapping.setEndTime(t2999);
    mapping.setWorkStateCode(lastMapping != null ? lastMapping.getWorkStateCode() : null);
    mapping.setWorkState(lastMapping != null ? lastMapping.getWorkState() : null);
    mapping.setIsOnline(isOnline.getIsOnline());
    mapping.setDuration(getDuration(isOnline.getCldDate()));
    mapping.setRecvStartTime(isOnline.getCldDate());
    mapping.setRecvEndTime(isOnline.getCldDate());
    return mapping;
  }

  private long getDuration(Date start) {
    Calendar c = Calendar.getInstance();
    c.setTime(start);
    c.add(Calendar.HOUR, 8);
    Calendar end = Calendar.getInstance();
    end.set(c.get(Calendar.YEAR),
        c.get(Calendar.MONTH),
        c.get(Calendar.DATE), 15, 59, 59);
    return (end.getTime().getTime() - c.getTime().getTime())/1000;
  }
}
