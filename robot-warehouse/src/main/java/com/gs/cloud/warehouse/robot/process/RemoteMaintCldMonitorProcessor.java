package com.gs.cloud.warehouse.robot.process;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.EventTicket;
import com.gs.cloud.warehouse.robot.entity.IncidentEvent;
import com.gs.cloud.warehouse.robot.entity.MonitorResult;
import com.gs.cloud.warehouse.robot.entity.MonitorWindowStat;
import com.gs.cloud.warehouse.robot.entity.RobotDisplacement;
import com.gs.cloud.warehouse.robot.entity.RobotTimestamp;
import com.gs.cloud.warehouse.robot.entity.RobotWorkOnlineState;
import com.gs.cloud.warehouse.robot.entity.RuleResult;
import com.gs.cloud.warehouse.robot.entity.rule.IncidentCntRule;
import com.gs.cloud.warehouse.robot.entity.rule.Rule;
import com.gs.cloud.warehouse.robot.entity.rule.RuleAbnormalShutDown;
import com.gs.cloud.warehouse.robot.entity.rule.WorkStateLimitationRule;
import com.gs.cloud.warehouse.robot.entity.WorkTask;
import com.gs.cloud.warehouse.robot.enums.ErrorWorkStateEnum;
import com.gs.cloud.warehouse.robot.enums.IncidentCntEnum;
import com.gs.cloud.warehouse.robot.enums.OnlineEnum;
import com.gs.cloud.warehouse.robot.enums.TaskStatusEnum;
import com.gs.cloud.warehouse.robot.enums.WorkStateLimitationEnum;
import com.gs.cloud.warehouse.robot.rule.IRuleProcessor;
import com.gs.cloud.warehouse.robot.rule.RuleAbnormalShutDownProcessor;
import com.gs.cloud.warehouse.robot.rule.RuleInciCntProcessor;
import com.gs.cloud.warehouse.robot.rule.RuleStateLmtProcessor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class RemoteMaintCldMonitorProcessor
    extends KeyedProcessFunction<String, BaseEntity, MonitorResult> {

  private final OutputTag<MonitorWindowStat> outputTag;
  //监控时长
  private final static Long MONITOR_DURATION = 2*60*60*1000L;
  //timer timeout
  private final static Long ONE_MINUTE = 60*1000L;

  private Map<String, IRuleProcessor> ruleProcessorMap;

  private transient ValueState<Boolean> doMonitorState;
  //车的最新时间(车)
  private transient ValueState<Long> lastRobotTimeState;
  //上次检查时间(车)
  private transient ValueState<Long> lastCheckTimeState;
  //车云的时间偏移量
  private transient ValueState<Long> avgOffsetState;
  //监控起始时间(车)
  private transient ValueState<List<Long>> monitorTimeState;

  //工作状态
  private transient ValueState<List<RobotWorkOnlineState>> workStateState;
  //告警状态
  //以code和incidentStartTime作为key，对告警去重
  private transient ValueState<TreeMap<String, Integer>> incidentState;
  //机器人位移数据
  private transient ValueState<TreeMap<Long, Double>> displacementState;
  //机器人任务状态
  private transient ValueState<Tuple2<Long, String>> taskStatusState;

  private transient ValueState<List<Rule>> ruleState;

  public RemoteMaintCldMonitorProcessor(OutputTag<MonitorWindowStat> outputTag) {
    this.outputTag = outputTag;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    doMonitorState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("doMonitorState", BasicTypeInfo.BOOLEAN_TYPE_INFO, false));
    lastRobotTimeState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("lastRobotTimeState", BasicTypeInfo.LONG_TYPE_INFO, 0L));
    lastCheckTimeState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("lastCheckTimeState", BasicTypeInfo.LONG_TYPE_INFO, 0L));
    avgOffsetState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("avgOffsetState", BasicTypeInfo.LONG_TYPE_INFO, 0L));
    monitorTimeState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("monitorTimeState",
            TypeInformation.of(new TypeHint<List<Long>>() {}), new ArrayList<>()));
    workStateState = getRuntimeContext().getState(
        new ValueStateDescriptor<>(
            "workStateState",
            TypeInformation.of(new TypeHint<List<RobotWorkOnlineState>>() {}), new ArrayList<>()));
    incidentState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("incidentState",
            TypeInformation.of(new TypeHint<TreeMap<String, Integer>>() {}), initIncidentMap()));
    displacementState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("displacementState",
            TypeInformation.of(new TypeHint<TreeMap<Long, Double>>() {}), new TreeMap<>()));
    taskStatusState = getRuntimeContext().getState(
        new ValueStateDescriptor<>(
            "taskStatusState",
            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}), Tuple2.of(0L, TaskStatusEnum.TASK_END_20103.getStatus())));
    ruleState = getRuntimeContext().getState(
        new ValueStateDescriptor<>("ruleState",
            TypeInformation.of(new TypeHint<List<Rule>>() {}), new ArrayList<>())
    );
    initRuleProcessor();
    super.open(parameters);
  }

  @Override
  public void processElement(BaseEntity value,
                             Context ctx,
                             Collector<MonitorResult> out) throws Exception {
    if (isRobotTimestamp(value)) {
      processRobotTimestamp((RobotTimestamp) value, ctx);
    } else if (isWorkOnlineState(value)) {
      processWorkOnlineState((RobotWorkOnlineState) value, ctx);
    } else if (isIncidentEvent(value)) {
      processIncidentEvent((IncidentEvent) value, ctx);
    } else if (isDisplacement(value)) {
      processDisplacement((RobotDisplacement) value, ctx);
    } else if (isEventTicket(value) || isWorkTask(value)) {
      processMonitorTrigger(value, ctx);
    } else if (isTaskStatus(value)) {
      processTaskStatus((IncidentEvent) value);
    }
  }

  @Override
  public void onTimer(long cldTimestamp,
                      OnTimerContext ctx,
                      Collector<MonitorResult> out) throws Exception {
    Boolean doMonitor = doMonitorState.value();
    if (!doMonitor) {
      return;
    }
    Long lastRobotTime = lastRobotTimeState.value();
    List<Long> monitorStartTimes = monitorTimeState.value();
    Long avgOffset = avgOffsetState.value();
    Long lastCheckTime = lastCheckTimeState.value();

    MonitorWindowStat stat = MonitorWindowStat
        .createDoMonitor(ctx.getCurrentKey(),
            monitorStartTimes,
            new Date(ctx.timestamp()),
            lastRobotTime,
            lastCheckTime,
            avgOffset);
    stopMonitorIfTimeArrived(ctx, out, stat);
    registerEventTimeTimer(ctx, ONE_MINUTE);
  }

  private void initRuleProcessor() {
    ruleProcessorMap = new HashMap<>();
    ruleProcessorMap.put("WorkStateLimitationRule", new RuleStateLmtProcessor());
    ruleProcessorMap.put("IncidentCntRule", new RuleInciCntProcessor());
    ruleProcessorMap.put("RuleAbnormalShutDown", new RuleAbnormalShutDownProcessor());
  }

  private void resetMonitorWindow(Context ctx) throws IOException {
    List<Long> monitorList = monitorTimeState.value();
    monitorList.remove(0);
    //无其他等待监控
    if (monitorList.isEmpty()) {
      doMonitorState.update(false);
      monitorTimeState.clear();
      clearWorkState();
      clearIncidentState();
      displacementState.clear();
    } else {
      monitorTimeState.update(monitorList);
      Long monitor = monitorList.get(0);
      List<RobotWorkOnlineState> workStates = workStateState.value();
      TreeMap<String, Integer> incidentMap = incidentState.value();
      TreeMap<Long, Double> displacementMap = displacementState.value();
      resetStateMap(workStates, monitor);
      resetIncidentMap(incidentMap, monitor);
      displacementMap.keySet().removeIf(key-> key < monitor);
      displacementState.update(displacementMap);
      startMonitor(ctx);
    }
  }

  private void clearWorkState() throws IOException {
    List<RobotWorkOnlineState> workStates = workStateState.value();
    if (workStates.size() <= 1) {
      return;
    }
    RobotWorkOnlineState last = workStates.get(workStates.size() - 1);
    workStates.clear();
    workStates.add(last);
    workStateState.update(workStates);
  }

  private void clearIncidentState() throws IOException {
    TreeMap<String, Integer> incidentMap = incidentState.value();
    if (incidentMap.size() <= 1) {
      return;
    }
    String key = incidentMap.lastKey();
    Integer value = incidentMap.get(key);
    incidentMap.clear();
    incidentMap.put(key, value);
    incidentState.update(incidentMap);
  }

  private void resetStateMap(List<RobotWorkOnlineState> workStates, Long start) throws IOException {
    Iterator<RobotWorkOnlineState> iterator = workStates.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    RobotWorkOnlineState lastEle = iterator.next();
    Long lastTimestamp = lastEle.getTimestampUtc().getTime();
    if (lastTimestamp < start) {
      iterator.remove();
    }
    while (iterator.hasNext()) {
      RobotWorkOnlineState ele = iterator.next();
      if (ele.getTimestampUtc().getTime() < start) {
        lastEle = ele;
        iterator.remove();
      } else {
        break;
      }
    }
    workStates.add(lastEle);
    workStateState.update(workStates);
  }

  private void resetIncidentMap(TreeMap<String, Integer> incidentMap, Long start) throws IOException {
    Iterator<String> iterator = incidentMap.keySet().iterator();
    if (!iterator.hasNext()) {
      return;
    }
    String lastKey = iterator.next();
    if (getTimestampFromKey(lastKey) < start) {
      iterator.remove();
    }
    while (iterator.hasNext()) {
      String key = iterator.next();
      if (getTimestampFromKey(key) < start) {
        iterator.remove();
        lastKey = key;
      } else {
        break;
      }
    }
    incidentMap.put(lastKey, 1);
    incidentState.update(incidentMap);
  }

  //更新机器人最新时间，更新偏移量平均值
  private void processRobotTimestamp(RobotTimestamp value,
                                     Context ctx) throws IOException {
    Boolean doMonitor = doMonitorState.value();
    Long lastRobotTime = lastRobotTimeState.value();
    lastRobotTime = getLargerTime(lastRobotTime, value.getTimestampUtc().getTime());
    lastRobotTimeState.update(lastRobotTime);
    avgOffsetState.update(value.getAvgOffset());

    if (doMonitor) {
      registerEventTimeTimer(ctx, ONE_MINUTE);
    }
  }

  private MonitorWindowStat stopMonitorIfTimeArrived(Context ctx,
                                        Collector<MonitorResult> out,
                                        MonitorWindowStat stat) throws IOException {
    if (!stat.isDoMonitor()) {
      return MonitorWindowStat.createUndoMonitor(ctx.getCurrentKey());
    }
    Long lastRobotTime = stat.getLastRobotUnixTimestamp();
    Long lastCheckTime = stat.getLastCheckUnixTimestamp();
    Long monitorStartTime = stat.getStartTimestamp().getTime();
    Long avgOffset = stat.getAvgOffset();
    Long cldTimestamp = stat.getCldTimestamp().getTime();
    List<RobotWorkOnlineState> workStates = sortWorkState();
    stat.setWorkStates(workStates);

    //时间有更新
    if (lastRobotTime > lastCheckTime) {
      MonitorWindowStat rtv = statWindowData(stat, lastRobotTime);
      checkIfAlert(rtv, out);
      if (lastRobotTime - monitorStartTime > MONITOR_DURATION) {
        rtv.setEndTimestamp(new Date(lastRobotTime));
        collect(ctx, rtv);
        resetMonitorWindow(ctx);
      }
      lastCheckTimeState.update(lastRobotTime);
      return rtv;
    } else {
      if (!workStates.isEmpty()) {
        RobotWorkOnlineState lastState = workStates.get(workStates.size() - 1);
        if (OnlineEnum.OFFLINE.getCode().equals(lastState.getState())) {
          //离线
          //离线超过5分钟以上，且 云端时间-最后同步的车端时间超过平均偏移量+5分钟时
          if (cldTimestamp - lastState.getCldTimestampUtc().getTime() > 30*60*1000
              && cldTimestamp - lastRobotTime - avgOffset >  30*60*1000) {
            MonitorWindowStat rtv = statWindowData(stat, lastRobotTime);
            rtv.setEndTimestamp(new Date(lastRobotTime));
            collect(ctx, rtv);
            resetMonitorWindow(ctx);
            return rtv;
            // todo 落库或者判断是否需要触发告警
          }
        }
      }
      //在线
      // do nothing
      return stat;
    }
  }

  private void checkIfAlert(MonitorWindowStat rtv, Collector<MonitorResult> out) throws IOException {
    List<Rule> rules = ruleState.value();
    Iterator<Rule> iterator = rules.iterator();
    while (iterator.hasNext()) {
      Rule rule = iterator.next();
      RuleResult ruleResult = ruleProcessorMap.get(rule.getProcessor()).eval(rule, rtv);
      if (ruleResult.isHasError()) {
        iterator.remove();
        MonitorResult result = new MonitorResult(
            rtv.getProductId(),
            rtv.getStartTimestamp(),
            new Date(rtv.getLastRobotUnixTimestamp()),
            ruleResult.getCode(),
            ruleResult.getMsg());
            out.collect(result);
      }
    }
    ruleState.update(rules);
  }

  private void collect(Context ctx, MonitorWindowStat stat) {
    stat.setAvgOffset(stat.getAvgOffset()/1000);
    ctx.output(outputTag, stat);
  }

  private void registerEventTimeTimer(Context ctx, Long offset) {
    long timestamp = ctx.timestamp() - (ctx.timestamp() % (60*1000));
    timestamp = timestamp + offset;
    ctx.timerService().registerEventTimeTimer(timestamp);
  }

  private MonitorWindowStat statWindowData(MonitorWindowStat stat, Long end) throws IOException {
    List<RobotWorkOnlineState> workStates = getWorkState(stat);
    Map<String, Integer> incidentMap = getIncidentState(stat);
    TreeMap<Long, Double> displacements = getDisplacementState(stat);
    Tuple2<Long, String> taskStatus = getTaskStatusState(stat);

    Long start = stat.getStartTimestamp().getTime();
    Tuple2<Map<String, Long>, Map<String, Long>> tuple2Duration
        = statDuration(workStates, start, end, stat.getCldTimestamp().getTime());
    Map<String, Integer> codeIncidentCntMap = statIncident(incidentMap, start, end);
    Optional<Integer> incidentCnt = codeIncidentCntMap.values().stream().reduce(Integer::sum);
    TreeMap<Long, Double> displacementInclude = statDisplacement(displacements, start, end);
    Optional<Double> totalDisplacement = displacementInclude.values().stream().reduce(Double::sum);
    stat.setIncidentCnt(incidentCnt.orElse(0));
    stat.setCodeIncidentCnt(codeIncidentCntMap);
    stat.setDurationMap(tuple2Duration.f0);
    stat.setMaxDurationMap(tuple2Duration.f1);
    stat.setDisplacementMap(displacementInclude);
    stat.setTaskStatus(taskStatus);
    stat.setTotalDisplacement((double)Math.round(totalDisplacement.orElse(0.0) * 100)/100);
    return stat;
  }

  private List<RobotWorkOnlineState> getWorkState(MonitorWindowStat stat) throws IOException {
    if (stat.getWorkStates() != null) {
      return stat.getWorkStates();
    }
    stat.setWorkStates(workStateState.value());
    return stat.getWorkStates();
  }

  private Map<String, Integer> getIncidentState(MonitorWindowStat stat) throws IOException {
    if (stat.getIncidentMap() != null) {
      return stat.getIncidentMap();
    }
    stat.setIncidentMap(incidentState.value());
    return stat.getIncidentMap();
  }

  private TreeMap<Long, Double> getDisplacementState(MonitorWindowStat stat) throws IOException {
    if (stat.getDisplacementMap() != null) {
      return stat.getDisplacementMap();
    }
    stat.setDisplacementMap(displacementState.value());
    return stat.getDisplacementMap();
  }

  private Tuple2<Long, String> getTaskStatusState(MonitorWindowStat stat) throws IOException {
    if (stat.getTaskStatus()!= null) {
      return stat.getTaskStatus();
    }
    stat.setTaskStatus(taskStatusState.value());
    return stat.getTaskStatus();
  }

  private void processWorkOnlineState(RobotWorkOnlineState value,
                                      Context ctx) throws IOException {
    Boolean doMonitor = doMonitorState.value();
    List<RobotWorkOnlineState> workStates = workStateState.value();
    Long lastRobotTime = lastRobotTimeState.value();
    if (value.isWorkState()) {
      //更新最近的车端时间
      Long currentRobotTime = value.getTimestampUtc().getTime();
      lastRobotTimeState.update(getLargerTime(lastRobotTime, currentRobotTime));

      if (!doMonitor) {
      //不在监控中的情况下，需要记录最近的一条工作状态
        if (workStates.isEmpty()
            || workStates.get(workStates.size()-1).getTimestampUtc().getTime() < currentRobotTime) {
          workStates.clear();
          workStates.add(value);
          workStateState.update(workStates);
        }
      } else {
      //监控中，需要记录状态变化记录
        //更新新的状态数据
        workStates.add(value);
        workStateState.update(workStates);
        registerEventTimeTimer(ctx, ONE_MINUTE);
      }
    } else {
      if (doMonitor) {
        value.setTimestampUtc(new Date(lastRobotTime));
        workStates.add(value);
        workStateState.update(workStates);
      }
    }
  }

  private void processIncidentEvent(IncidentEvent value,
                                    Context ctx) throws IOException {
    Boolean doMonitor = doMonitorState.value();
    Long lastRobotTime = lastRobotTimeState.value();
    Long currentRobotTime = value.getEntityEventTime().getTime();
    lastRobotTimeState.update(getLargerTime(lastRobotTime, currentRobotTime));

    TreeMap<String, Integer> incidentMap = incidentState.value();
    if (!doMonitor) {
      if (incidentMap.isEmpty()
          || getTimestampFromKey(incidentMap.lastKey()) < currentRobotTime) {
        incidentMap.clear();
        incidentMap.put(
            getCodeIncidentStartTime(value.getIncidentCode(), value.getIncidentStartTime()), 1);
        incidentState.update(incidentMap);
      }
    } else {
      String incidentKey = getCodeIncidentStartTime(value.getIncidentCode(), value.getIncidentStartTime());
      if (!incidentMap.containsKey(incidentKey)) {
        incidentMap.put(incidentKey, 1);
        incidentState.update(incidentMap);
      }
      registerEventTimeTimer(ctx, ONE_MINUTE);
    }
  }

  private void processDisplacement(RobotDisplacement value, Context ctx) throws IOException {
    Boolean doMonitor = doMonitorState.value();
    if (doMonitor) {
      TreeMap<Long, Double> displacements = displacementState.value();
      displacements.put(
          value.getEndTimeUtc().getTime(),
          (double)Math.round(value.getDistance() * 100)/100);
      displacementState.update(displacements);
      registerEventTimeTimer(ctx, ONE_MINUTE);
    }
  }

  private void processMonitorTrigger(BaseEntity value, Context ctx) throws IOException {
    Boolean doMonitor = doMonitorState.value();
    Long lastRobotTime = lastRobotTimeState.value();
    if (!doMonitor) {
      startMonitor(ctx);
      List<Long> monitorList = new ArrayList<>();
      monitorList.add(lastRobotTime);
      monitorTimeState.update(monitorList);
    } else {
      List<Long> monitorList = monitorTimeState.value();
      Long monitor = monitorList.get(monitorList.size()-1);
      if (monitor == 0L && lastRobotTime == 0L) {
        return;
      } else if (monitor == 0L) {
        monitorList.set(monitorList.size()-1, lastRobotTime);
        monitorTimeState.update(monitorList);
      } else if (monitor+MONITOR_DURATION < lastRobotTime) {
      //为了减少告警量，使用滚动窗口的方式，保证监控区间不重叠
        monitorList.add(lastRobotTime);
        monitorTimeState.update(monitorList);
      }
    }
  }

  private void processTaskStatus(IncidentEvent value) throws IOException {
    if (value.isTaskStart()) {
      taskStatusState.update(Tuple2.of(value.getIncidentStartTime().getTime(),
          TaskStatusEnum.TASK_START_20102.getStatus()));
    } else if (value.isTaskEnd()) {
      taskStatusState.update(Tuple2.of(value.getIncidentStartTime().getTime(),
          TaskStatusEnum.TASK_END_20103.getStatus()));
    }
  }

  private void startMonitor(Context ctx) throws IOException {
    initRule();
    doMonitorState.update(true);
    registerEventTimeTimer(ctx, ONE_MINUTE);
  }

  private void initRule() throws IOException {
    List<Rule> rules = new ArrayList<>();
    //状态时长
    for (WorkStateLimitationEnum ele : WorkStateLimitationEnum.values()) {
      rules.add(
          new WorkStateLimitationRule(ele.getCode(), ele.getWorkStateCode(), ele.getWorkState(), ele.getLimitation()));
    }
    //告警次数
    for (IncidentCntEnum ele : IncidentCntEnum.values()) {
      rules.add(
          new IncidentCntRule(ele.getCode(), ele.getIncidentCode(), ele.getTitle(), ele.getCnt()));
    }
    rules.add(new RuleAbnormalShutDown());
    ruleState.update(rules);
  }

  private Tuple2<Map<String, Long>, Map<String, Long>> statDuration(List<RobotWorkOnlineState> workStates,
                                                                    Long start, Long end, Long cldTimestamp) {
    Map<String, Long> durationMap = new HashMap<>();
    Map<String, Long> maxDurationMap = new HashMap<>();
    Iterator<RobotWorkOnlineState> iterator = workStates.iterator();
    if (!iterator.hasNext()) {
      return Tuple2.of(durationMap, maxDurationMap);
    }
    RobotWorkOnlineState lastEle = iterator.next();
    while (iterator.hasNext()) {
      RobotWorkOnlineState ele = iterator.next();
      if (lastEle.isOnline()) {
        lastEle = ele;
        continue;
      }
      if (lastEle.isWorkState()) {
        Long lastKey = lastEle.getTimestampUtc().getTime();
        Long key = ele.getTimestampUtc().getTime();
        //key 必然小于等于 end
        if (key > start && lastKey < end) {
          calculateInterval(Math.max(lastKey, start), key, lastEle.getState(), durationMap);
          calculateMaxInterval(Math.max(lastKey, start), key, lastEle.getState(), maxDurationMap);
        }
      } else {
        Long lastKey = lastEle.getCldTimestampUtc().getTime();
        Long key = ele.getCldTimestampUtc().getTime();
        calculateInterval(lastKey, key, lastEle.getState(), durationMap);
        calculateMaxInterval(lastKey, key, lastEle.getState(), maxDurationMap);
      }
      lastEle = ele;
    }
    //补上最后一个状态到结束时间的这段时长
    if (!lastEle.isOnline()) {
      Long lastKey = lastEle.isWorkState()
          ? Math.max(lastEle.getTimestampUtc().getTime(), start) : lastEle.getCldTimestampUtc().getTime();
      Long key = lastEle.isWorkState() ? end : cldTimestamp;
      calculateMaxInterval(lastKey, key, lastEle.getState(), durationMap);
      calculateMaxInterval(lastKey, key, lastEle.getState(), maxDurationMap);
    }
    return Tuple2.of(durationMap, maxDurationMap);
  }

  private Map<String, Integer> statIncident(Map<String, Integer> incidentMap, Long start, Long end) {
    Map<String, Integer> codeIncidentCntMap = new HashMap<>();
    if (incidentMap.isEmpty()) {
      return codeIncidentCntMap;
    }
    for (String key : incidentMap.keySet()) {
      Long timestamp = getTimestampFromKey(key);
      if (timestamp >= start && timestamp < end) { //上闭下开
        String code = getCodeFromKey(key);
        codeIncidentCntMap.compute(code, (x, old)-> old == null ? 1 : old+1);
      }
    }
    return codeIncidentCntMap;
  }

  private TreeMap<Long, Double> statDisplacement(TreeMap<Long, Double> displacements,
                                             Long start, Long end) {
    TreeMap<Long, Double> rtv = new TreeMap<>();
    if (displacements.isEmpty()) {
      return rtv;
    }
    for (Long key : displacements.keySet()) {
      if (key >= start && key < end) {
        rtv.put(key/1000, displacements.get(key));
      }
    }
    return rtv;
  }

  private void calculateInterval(Long lastTimestamp, Long timestamp,
                                 String lastState, Map<String, Long> map) {
    Long interval = (timestamp - lastTimestamp) / 1000;
    map.compute(lastState, (s, old)-> old == null ? interval:old+interval);
  }

  private void calculateMaxInterval(Long lastTimestamp, Long timestamp,
                                 String lastState, Map<String, Long> map) {
    Long interval = (timestamp - lastTimestamp) / 1000;
    map.compute(lastState, (s, old)-> old == null ? interval:Math.max(old, interval));
  }


  private boolean isErrorWorkState(String state) {
    return ErrorWorkStateEnum.AUTO_TASK_PAUSED.getCode().equals(state)
        || ErrorWorkStateEnum.ELEVATOR_PAUSE.getCode().equals(state)
        || ErrorWorkStateEnum.MULTI_TASK_PAUSE.getCode().equals(state)
        || ErrorWorkStateEnum.NAVIGATING_PAUSED.getCode().equals(state)
        || ErrorWorkStateEnum.UNINT.getCode().equals(state);
  }

  private boolean isRobotTimestamp(BaseEntity value) {
    return value instanceof RobotTimestamp;
  }

  private boolean isWorkOnlineState(BaseEntity value) {
    return value instanceof RobotWorkOnlineState;
  }

  private boolean isIncidentEvent(BaseEntity value) {
    return value instanceof IncidentEvent
        && !((IncidentEvent)value).isTaskStatus();
  }

  private boolean isDisplacement(BaseEntity value) {
    return value instanceof RobotDisplacement;
  }

  private boolean isEventTicket(BaseEntity value) {
    return value instanceof EventTicket;
  }

  private boolean isWorkTask(BaseEntity value) {
    return value instanceof WorkTask;
  }

  private boolean isTaskStatus(BaseEntity value) {
    return value instanceof IncidentEvent
        && ((IncidentEvent)value).isTaskStatus();
  }

  private Long getLargerTime(Long date1, Long date2) {
    if (date1 == null) {
      return date2;
    } else if (date1 > date2) {
      return date1;
    } else {
      return date2;
    }
  }

  private String getCodeIncidentStartTime(String code, Date time) {
    return code + "_" + time.getTime();
  }

  private String getCodeFromKey(String key) {
    return key.substring(0, key.indexOf("_"));
  }

  private Long getTimestampFromKey(String key) {
    return Long.parseLong(key.substring(key.indexOf("_") + 1));
  }

  private TreeMap<String, Integer> initIncidentMap() {
    return new TreeMap<>(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return getTimestampFromKey(o1).compareTo(getTimestampFromKey(o2));
      }
    });
  }

  private List<RobotWorkOnlineState> sortWorkState() throws IOException {
    List<RobotWorkOnlineState> workStates = workStateState.value();
    workStates.sort(new Comparator<RobotWorkOnlineState>() {
      @Override
      public int compare(RobotWorkOnlineState o1, RobotWorkOnlineState o2) {
        if (o1.getTimestampUtc().getTime() != o2.getTimestampUtc().getTime()) {
          return o1.getTimestampUtc().compareTo(o2.getTimestampUtc());
        } else {
          return o1.getCldTimestampUtc().compareTo(o2.getCldTimestampUtc());
        }
      }
    });
    workStateState.update(workStates);
    return workStates;
  }
}
