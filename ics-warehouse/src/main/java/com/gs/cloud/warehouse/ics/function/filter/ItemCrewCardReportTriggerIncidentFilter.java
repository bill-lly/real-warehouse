package com.gs.cloud.warehouse.ics.function.filter;

import com.gs.cloud.warehouse.ics.entity.ItemCrewCardReportTriggerIncident;
import com.gs.cloud.warehouse.ics.entity.TxCard;
import org.apache.flink.api.common.functions.FilterFunction;

public class ItemCrewCardReportTriggerIncidentFilter implements FilterFunction<ItemCrewCardReportTriggerIncident> {
    @Override
    public boolean filter(ItemCrewCardReportTriggerIncident value) throws Exception {
        if (value.getReportTime() == null
            || value.getReportTime().getTime() > value.getCloudTime().getTime()
            ){
            return false;
        }
        return true;
    }
}
