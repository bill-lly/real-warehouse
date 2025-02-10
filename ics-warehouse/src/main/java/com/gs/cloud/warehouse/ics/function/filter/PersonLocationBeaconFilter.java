package com.gs.cloud.warehouse.ics.function.filter;

import com.gs.cloud.warehouse.ics.entity.PersonLocationBeacon;
import org.apache.flink.api.common.functions.FilterFunction;

public class PersonLocationBeaconFilter implements FilterFunction<PersonLocationBeacon> {
    @Override
    public boolean filter(PersonLocationBeacon value) throws Exception {
        if (value.getSpaceCode() == null ||value.getSpaceCode().equals("")
            || value.getSpaceName() == null ||value.getSpaceName().equals("")
            || value.getCrewId() == null ||value.getCrewId().equals("")
            || value.getReportTimestamp() == null
            ){
            return false;
        }
        //空间名称中,替换成-
        value.setSpaceName(value.getSpaceName().replace(",","-"));
        return true;
    }
}
