package com.gs.cloud.warehouse.ics.function.process;

import com.gs.cloud.warehouse.ics.entity.PersonLocationStatistics;
import com.gs.cloud.warehouse.ics.entity.PersonTenMinuteTrace;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class PersonTenMinuteTraceFunction extends ProcessFunction<List<PersonLocationStatistics>, PersonTenMinuteTrace> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void processElement(List<PersonLocationStatistics> value,
                               Context ctx,
                               Collector<PersonTenMinuteTrace> out) throws Exception {
        PersonTenMinuteTrace personTenMinuteTrace = new PersonTenMinuteTrace();
        personTenMinuteTrace.setItemId(value.get(0).getItemId());
        personTenMinuteTrace.setCrewId(value.get(0).getCrewId());
        personTenMinuteTrace.setSliceTimestamp(value.get(0).getSliceTimestamp());
        Long totalDura = 0L;
        Long maxSpaceDura = 0L;
        String maxSpaceCode = "";
        String maxSpaceName = "";
        ArrayNode spaceInfoDetialArr = objectMapper.createArrayNode();
        for (int i = 0; i < value.size(); i++) {
            PersonLocationStatistics valeEle = value.get(i);
            //获取最大值
            totalDura = totalDura + valeEle.getDuration();
            if(valeEle.getDuration() >= maxSpaceDura) {
                maxSpaceDura= valeEle.getDuration();
                maxSpaceCode = valeEle.getSpaceCode();
                maxSpaceName = valeEle.getSpaceName();
            }
            //轨迹空间详细信息保存为json数据
            JsonNode traceJson = objectMapper.createObjectNode();
            ((ObjectNode)traceJson).put("spaceCode", valeEle.getSpaceCode());
            ((ObjectNode)traceJson).put("spaceName", valeEle.getSpaceName());
            ((ObjectNode)traceJson).put("duration", valeEle.getDuration());
            spaceInfoDetialArr.add(traceJson);
        }
        personTenMinuteTrace.setMaxSpaceDura(maxSpaceDura);
        personTenMinuteTrace.setReportTotalDura(totalDura);
        personTenMinuteTrace.setMaxSpaceCode(maxSpaceCode);
        personTenMinuteTrace.setMaxSpaceName(maxSpaceName);
        personTenMinuteTrace.setSpaceInfoDetial(spaceInfoDetialArr.toString());
        out.collect(personTenMinuteTrace);
    }
}
