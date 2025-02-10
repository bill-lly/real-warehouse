package com.gs.cloud.warehouse.robot.process;

import com.gs.cloud.warehouse.entity.BaseEntity;
import com.gs.cloud.warehouse.robot.entity.*;
import com.gs.cloud.warehouse.robot.entity.rule.*;
import com.gs.cloud.warehouse.robot.enums.ErrorWorkStateEnum;
import com.gs.cloud.warehouse.robot.enums.IncidentCntEnum;
import com.gs.cloud.warehouse.robot.enums.OnlineEnum;
import com.gs.cloud.warehouse.robot.enums.TaskStatusEnum;
import com.gs.cloud.warehouse.robot.enums.WorkStateLimitationEnum;
import com.gs.cloud.warehouse.robot.rule.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.List;


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
  // 待执行任务列表
  private transient ListState<Tuple2<String, Long>> planTasksState;
  // 基石数据状态,暂时不用
//  private transient MapState<String, Long> cornerStoneState;
  // 任务超时未执行时间 30 min
  private final static long ThirtyMinutesMS = 30 * 60 * 1000L;
  private final static long EightHoursMS = 8 * 60 * 60 * 1000L;

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
    planTasksState = getRuntimeContext().getListState(new ListStateDescriptor<>("planTasksState",
            TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})));

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
    }else if (isRobotPlanTask(value)) {
      processRobotPlanTask((RobotPlanTask) value, ctx);
    }
  }

  @Override
  public void onTimer(long cldTimestamp,
                      OnTimerContext ctx,
                      Collector<MonitorResult> out) throws Exception {
    Boolean doMonitor = doMonitorState.value();

    Long lastRobotTime = lastRobotTimeState.value();
    List<Long> monitorStartTimes = monitorTimeState.value();
    Long avgOffset = avgOffsetState.value();
    Long lastCheckTime = lastCheckTimeState.value();
    final List<Tuple2<String, Long>> planTasks = (List<Tuple2<String, Long>>) (planTasksState.get());

    // 先进判断是否有监控任务，有的话进行处理，否则跳过
    if (planTasks.size() > 0) {
      processPlanTaskState(out, ctx);
      // 逻辑处理完毕，再判断是否还有任务监控，有则开启下一次监控
      if ( planTasks.size()> 0) {
        registerEventTimeTimer(ctx, ONE_MINUTE);
      }
    }

    if (!doMonitor) {
      return;
    }
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
    // 机器时间
    Long lastRobotTime = lastRobotTimeState.value();
    // 只保留2h内+上一条数据即可
    List<RobotWorkOnlineState> workStates = workStateState.value();
    List<RobotWorkOnlineState> tempWorkStates = new ArrayList<>();

    if (workStates.size() <= 1) {
      return;
    }

    for (int i = workStates.size() - 2; i >= 0; i--) {
      if (workStates.get(i).getTimestampUtc().getTime() < lastRobotTime - 2 * 60 * 60 * 1000l) {
        tempWorkStates.add(workStates.get(i));
        workStates.remove(i);
      }
    }
    if (!tempWorkStates.isEmpty()){
      workStates.add(0,tempWorkStates.get(0));
      tempWorkStates.clear();
    }
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
        if (IsOffline30Min(stat)) {
            MonitorWindowStat rtv = statWindowData(stat, lastRobotTime);
            rtv.setEndTimestamp(new Date(lastRobotTime));
            collect(ctx, rtv);
            resetMonitorWindow(ctx);
            return rtv;
            // todo 落库或者判断是否需要触发告警
        }
      //在线
      // do nothing
      return stat;
    }
  }
  private boolean IsOfflineOfMin(OnTimerContext ctx,Long timeOutMs) throws IOException {
    List<RobotWorkOnlineState> workStates = workStateState.value();
    Long cldTimestamp = ctx.timestamp();
    Long lastRobotTime = lastRobotTimeState.value();
    Long avgOffset = avgOffsetState.value();
    if (workStates.isEmpty()) {
      return false;
    }
    RobotWorkOnlineState lastState = workStates.get(workStates.size() - 1);
    return OnlineEnum.OFFLINE.getCode().equals(lastState.getState())
            && (cldTimestamp - lastState.getCldTimestampUtc().getTime() > timeOutMs
            && cldTimestamp - lastRobotTime - avgOffset >  timeOutMs);
  }
  private boolean IsOffline30Min(MonitorWindowStat stat) {
    List<RobotWorkOnlineState> workStates = stat.getWorkStates();
    Long cldTimestamp = stat.getCldTimestamp().getTime();
    Long lastRobotTime = stat.getLastRobotUnixTimestamp();
    Long avgOffset = stat.getAvgOffset();
    if (workStates.isEmpty()) {
      return false;
    }
    RobotWorkOnlineState lastState = workStates.get(workStates.size() - 1);
    return OnlineEnum.OFFLINE.getCode().equals(lastState.getState())
            && (cldTimestamp - lastState.getCldTimestampUtc().getTime() > 30*60*1000
            && cldTimestamp - lastRobotTime - avgOffset >  30*60*1000);
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
    if (stat.getTaskStatus() != null) {
      return stat.getTaskStatus();
    }
    stat.setTaskStatus(taskStatusState.value());
    return stat.getTaskStatus();
  }

  private void processWorkOnlineState(RobotWorkOnlineState value,
                                      Context ctx) throws Exception {
    Boolean doMonitor = doMonitorState.value();
    List<RobotWorkOnlineState> workStates = workStateState.value();
    Long lastRobotTime = lastRobotTimeState.value();
    List<Tuple2<String, Long>> planTasks = (List<Tuple2<String, Long>>) (planTasksState.get());

    if (value.isWorkState()) {
      //更新最近的车端时间
      Long currentRobotTime = value.getTimestampUtc().getTime();
      lastRobotTimeState.update(getLargerTime(lastRobotTime, currentRobotTime));

      //监控中，需要记录状态变化记录
      //更新新的状态数据
      workStates.add(value);
      workStateState.update(workStates);
      if (doMonitor){
        registerEventTimeTimer(ctx, ONE_MINUTE);
      }
    } else {
      value.setTimestampUtc(new Date(lastRobotTime));
      workStates.add(value);
      workStateState.update(workStates);
    }

    if (planTasks.size()>0){
      registerEventTimeTimer(ctx, ONE_MINUTE);
    }
    clearWorkState();
  }

  private void processIncidentEvent(IncidentEvent value,
                                    Context ctx) throws IOException {
    Boolean doMonitor = doMonitorState.value();
    Long lastRobotTime = lastRobotTimeState.value();
    Long currentRobotTime = value.getEntityEventTime().getTime();
    lastRobotTimeState.update(getLargerTime(lastRobotTime, currentRobotTime));

    TreeMap<String, Integer> incidentMap = incidentState.value();
    if (value.isCreateTicket() || value.isTaskStatus()){
      if (!doMonitor) {
        if (incidentMap.isEmpty() || getTimestampFromKey(incidentMap.lastKey()) < currentRobotTime) {
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

  public void processRobotPlanTask(RobotPlanTask value, Context ctx) throws Exception {
    //手动插入数据无意义,无需处理
    if (!value.getKey().equals("")) {
      // 将任务缓存起来
      planTasksState.add(Tuple2.of(value.getPlanTaskName(), value.getPlanTaskStartTimeMS()));
      final List<Tuple2<String, Long>> planTasks = (List<Tuple2<String, Long>>) (planTasksState.get());
      planTasks.sort((o1, o2) -> (int) (o1.f1 - o2.f1));
      planTasksState.update(planTasks);

      // TODO 开始监控
      registerEventTimeTimer(ctx, ONE_MINUTE);
    }
  }

//  private void processRobotCornerStone(RobotCornerStone value) throws Exception {
//    final List<Tuple2<String, Long>> planTasks = (List<Tuple2<String, Long>>) (planTasksState.get());
//      // 更新机器人车端时间
//      Long lastRobotTime = lastRobotTimeState.value();
//      lastRobotTime = getLargerTime(lastRobotTime, value.getEventTime().getTime());
//      lastRobotTimeState.update(lastRobotTime);
//  }

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
        rtv.put(key / 1000, displacements.get(key));
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

  private boolean isRobotPlanTask(BaseEntity value) {
    return value instanceof RobotPlanTask;
  }

  private boolean isTaskStatus(BaseEntity value) {
    return value instanceof IncidentEvent
        && ((IncidentEvent)value).isTaskStatus();
  }

//  private static boolean isRobotCornerStone(BaseEntity value) {
//    return value instanceof RobotCornerStone;
//  }
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

  private void processPlanTaskState(Collector<MonitorResult> out, OnTimerContext ctx) throws Exception {
    final List<Tuple2<String, Long>> planTasks = (List<Tuple2<String, Long>>) (planTasksState.get());
    final String taskName = planTasks.get(0).f0;
    final Long taskStarkTime = planTasks.get(0).f1;
    final Long taskTimeOutTime = planTasks.get(0).f1 + ThirtyMinutesMS;
    final Long avgOffset = avgOffsetState.value();
    final Long lastRobotTime = lastRobotTimeState.value();
    final Long currentCldTimestamp = ctx.timestamp();
    final List<RobotWorkOnlineState> workStates = workStateState.value();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    final Map<String, Object> msg = new HashMap<>();
    msg.put("taskName",taskName);
    msg.put("taskTime",sdf.format(new Date(taskStarkTime)));

    final RulePlanTaskNotExecution rulePlanTaskNotExecution = new RulePlanTaskNotExecution();
    MonitorResult monitorResult = new MonitorResult(
            ctx.getCurrentKey()
            , new Date((lastRobotTime==0?currentCldTimestamp-avgOffset:lastRobotTime))
            , new Date((lastRobotTime==0?currentCldTimestamp-avgOffset:lastRobotTime))
            , rulePlanTaskNotExecution.getCode()
            , ""
    );


    if (Math.abs(avgOffset) >= EightHoursMS * 3){
      // 机云时间相差超过24H的数据不做监控
      // 机云时间相差超过1天的数据不做监控 丢弃告警，不输出
      clearPlanTaskState(planTasks);

    } else if (workStates.isEmpty()){
      // 机器工作&在离线 状态为空
      if (lastRobotTime == 0l){
        // 车端时间为0
       if (currentCldTimestamp - avgOffset >= taskTimeOutTime + EightHoursMS) {
          // 机器一直无车端数据上报且已超过8小时 丢弃告警，不输出
          clearPlanTaskState(planTasks);
        }
      } else if (lastRobotTime > taskTimeOutTime) {
        // TODO 机器时间超过任务告警时间，触发告警
        msg.put("reason","No tasks have been executed");
        monitorResult.setMsg(new ObjectMapper().writeValueAsString(msg));

        clearPlanTaskState(planTasks);
        out.collect(monitorResult);
      }
    } else {
      // 判断机器是否离线
      if (IsOfflineOfMin(ctx,ThirtyMinutesMS)) {
        // 如果机器离线，机器最后的状态是offline，则判断 watermark-avgoffset是否超过半小时小时
        if (currentCldTimestamp - avgOffset > taskTimeOutTime) {
          // TODO 机器离线且未执行任务，输出告警
          msg.put("reason","Robot offline,task ont executed");
          monitorResult.setMsg(new ObjectMapper().writeValueAsString(msg));
          clearPlanTaskState(planTasks);
          out.collect(monitorResult);
        }
      } else if (workStates.get(workStates.size() - 1).getState().equals("230")) {
        // 机器未离线，最后的状态为任务中,且状态开始时间小于任务超时时间 且 车端时间已到达任务开始时间
        if (workStates.get(workStates.size() - 1).getTimestampUtc().getTime() <= taskTimeOutTime
                && lastRobotTime >= taskStarkTime){
          // 任务已执行 删除第一个任务 丢弃告警，不输出
          clearPlanTaskState(planTasks);
        }else if (workStates.get(workStates.size() - 1).getTimestampUtc().getTime() > taskTimeOutTime){
          // TODO 任务超时，输出告警
          msg.put("reason","Task timeout executed");
          monitorResult.setMsg(new ObjectMapper().writeValueAsString(msg));
          clearPlanTaskState(planTasks);
          out.collect(monitorResult);
        }
      } else {
        // 最后一个状态不是工作中,则判断机器 是否已经超过告警时间
        if (lastRobotTime > taskTimeOutTime) {
          // TODO 最后非工作状态持续半小时以上
          msg.put("reason","Non-working state for more than 30 min, task not executed");
          monitorResult.setMsg(new ObjectMapper().writeValueAsString(msg));
          clearPlanTaskState(planTasks);
          out.collect(monitorResult);
        } else {
          // 查找最近2小时内符合运行状态的数据
          for (int i = workStates.size() - 2; i >= 0; i--) {
            // 从倒数第二个状态开始判断
            if (workStates.get(i).getState().equals("230")) {
              // 已车端时间为准
              long stateStartTime = workStates.get(i).getTimestampUtc().getTime();
              long stateEndTime = workStates.get(i + 1).getTimestampUtc().getTime();
              if ((stateStartTime >= taskStarkTime && stateStartTime <= taskTimeOutTime)
                      || (stateEndTime <= taskTimeOutTime && stateEndTime >= taskStarkTime)) {
                // 说明任务被执行，删除第一个任务 丢弃告警，不输出
                clearPlanTaskState(planTasks);
                break;
              }
            }
          }
        }
      }
    }
    // 清理工作状态数据
    clearWorkState();
  }
  // 清理排班任务状态
  private void clearPlanTaskState(List<Tuple2<String, Long>> planTasks) throws Exception {
    planTasks.remove(0);
    planTasksState.clear();
    planTasksState.update(planTasks);
  }
}