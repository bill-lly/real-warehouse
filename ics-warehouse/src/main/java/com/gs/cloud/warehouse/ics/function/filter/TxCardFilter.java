package com.gs.cloud.warehouse.ics.function.filter;

import com.gs.cloud.warehouse.ics.entity.PersonLocationBeacon;
import com.gs.cloud.warehouse.ics.entity.TxCard;
import org.apache.flink.api.common.functions.FilterFunction;

public class TxCardFilter implements FilterFunction<TxCard> {
    @Override
    public boolean filter(TxCard value) throws Exception {
        if (value.getCrewId() == null ||value.getCrewId().equals("")
            || value.getRepTimestampMs() == null
            || value.getRepTimeT8().getTime() > value.getCloudRecvTime().getTime()
            ){
            return false;
        }

        //保留一份字符类型id数据
        value.setCrewIdStr(String.valueOf(value.getCrewId()));
        return true;
    }
}
