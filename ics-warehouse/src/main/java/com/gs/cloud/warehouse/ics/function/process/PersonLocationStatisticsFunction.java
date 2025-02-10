package com.gs.cloud.warehouse.ics.function.process;

import com.gs.cloud.warehouse.ics.entity.PersonLocationBeacon;
import com.gs.cloud.warehouse.ics.entity.PersonLocationStatistics;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PersonLocationStatisticsFunction extends ProcessFunction<List<PersonLocationBeacon>, List<PersonLocationStatistics>> {
    @Override
    public void processElement(List<PersonLocationBeacon> value,
                               Context ctx,
                               Collector<List<PersonLocationStatistics>> out) throws Exception {
        //统计时长
        //取空间内开始时间和结束时间,只有一笔忽略
        if (value.size() > 1) {
            List<PersonLocationStatistics> output = new ArrayList<>();
            PersonLocationStatistics lastPersonLocationSts = null;
            lastPersonLocationSts = convert2LocSts(value.get(0));
            for (int i=1; i<value.size(); i++) {
                PersonLocationStatistics cur = convert2LocSts(value.get(i));
                if (lastPersonLocationSts != null) {
                    output.add(updateEndTime(lastPersonLocationSts, cur.getStartTime()));
                }
                lastPersonLocationSts = cur;
            }
            if (!output.isEmpty()) {
                out.collect(output);
            }
        }
    }

    private PersonLocationStatistics convert2LocSts(PersonLocationBeacon personLocationBeacon) {
        PersonLocationStatistics personLocSts = new PersonLocationStatistics();
        personLocSts.setItemId(Long.parseLong(personLocationBeacon.getItemId()));
        personLocSts.setCrewId(Long.parseLong(personLocationBeacon.getCrewId()));
        personLocSts.setSpaceCode(personLocationBeacon.getSpaceCode());
        personLocSts.setSpaceName(personLocationBeacon.getSpaceName());
        personLocSts.setStartTime(personLocationBeacon.getEventTime());
        personLocSts.setEndTime(null);
        personLocSts.setDuration(0L);
        personLocSts.setSliceTimestamp(personLocationBeacon.getSliceTimestamp());
        return personLocSts;
    }

    private PersonLocationStatistics updateEndTime(PersonLocationStatistics lastPersonLocationSts, Date startTime) {
        lastPersonLocationSts.setEndTime(startTime);
        lastPersonLocationSts.setDuration((lastPersonLocationSts.getEndTime().getTime() - lastPersonLocationSts.getStartTime().getTime()) / 1000);
        return lastPersonLocationSts;
    }
}
