package com.gs.cloud.warehouse.ics.function.window;

import com.gs.cloud.warehouse.ics.entity.PersonLocationBeacon;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

public class LastPersonLocationWindow implements WindowFunction<PersonLocationBeacon, List<PersonLocationBeacon>, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<PersonLocationBeacon> input, Collector<List<PersonLocationBeacon>> out) throws Exception {
        //先取最后一笔定位数据
        List<PersonLocationBeacon> inputList = (List<PersonLocationBeacon>)input;
        if (inputList == null || inputList.isEmpty()) {
            return;
        }
        Collections.sort(inputList);
        PersonLocationBeacon lastPersonLocation = null;
        List<PersonLocationBeacon> output = new ArrayList<>();
        int inputListIndexPos = 0;
        for (int i = 0; i < inputList.size(); i++) {
            if (inputList.get(i).getSpaceCode() == null) {
                continue;
            }
            if (lastPersonLocation == null
                    || (lastPersonLocation.getEventTime().getTime() <= inputList.get(i).getEventTime().getTime()
                    && !lastPersonLocation.getSpaceCode().equals(inputList.get(i).getSpaceCode()))) {
                //设置窗口开始时间
                inputList.get(i).setSliceTimestamp(timeWindow.getStart());
                output.add(inputList.get(i));
                inputListIndexPos = i;//记录索引位置，最后一组相同空间记录数量如果大于1，inputList最后一笔需要保留
            }
            lastPersonLocation = inputList.get(i);
        }
        //最后一组相同空间记录数量如果大于1，inputList最后一笔需要保留
        if(inputList.size() - 1 > inputListIndexPos && inputList.get(inputList.size() - 1).getEventTime().getTime() > inputList.get(inputListIndexPos).getEventTime().getTime()) {
            inputList.get(inputList.size() - 1).setSliceTimestamp(timeWindow.getStart());
            output.add(inputList.get(inputList.size() - 1));
        }
        if (!output.isEmpty()) {
            out.collect(output);
        }
    }
}
