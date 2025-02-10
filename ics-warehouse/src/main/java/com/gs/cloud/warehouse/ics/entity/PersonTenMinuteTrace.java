package com.gs.cloud.warehouse.ics.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;

import java.util.Date;

public class PersonTenMinuteTrace extends BaseEntity {
    //项目id
    private Long itemId;
    //保洁员id
    private Long crewId;
    //切片起始时间戳
    private Long sliceTimestamp;
    //总计上报时长
    private Long reportTotalDura;
    //最大空间滞留时长
    private Long maxSpaceDura;
    //最大滞留空间code
    private String maxSpaceCode;
    //最大滞留空间名称
    private String maxSpaceName;
    //轨迹空间详细信息
    private String spaceInfoDetial;

    public Long getCrewId() { return crewId; }

    public void setCrewId(Long crewId) { this.crewId = crewId; }

    public Long getItemId() { return itemId; }

    public void setItemId(Long itemId) { this.itemId = itemId; }

    public Long getSliceTimestamp() { return sliceTimestamp; }

    public void setSliceTimestamp(Long sliceTimestamp) { this.sliceTimestamp = sliceTimestamp; }

    public Long getReportTotalDura() { return reportTotalDura; }

    public void setReportTotalDura(Long reportTotalDura) { this.reportTotalDura = reportTotalDura; }

    public Long getMaxSpaceDura() { return maxSpaceDura; }

    public void setMaxSpaceDura(Long maxSpaceDura) { this.maxSpaceDura = maxSpaceDura; }

    public String getMaxSpaceCode() { return maxSpaceCode; }

    public void setMaxSpaceCode(String maxSpaceCode) { this.maxSpaceCode = maxSpaceCode; }

    public String getMaxSpaceName() { return maxSpaceName; }

    public void setMaxSpaceName(String maxSpaceName) { this.maxSpaceName = maxSpaceName; }

    public String getSpaceInfoDetial() { return spaceInfoDetial; }

    public void setSpaceInfoDetial(String spaceInfoDetial) { this.spaceInfoDetial = spaceInfoDetial; }

    @Override
    public String toString() {
        return "PersonLocationStatistics{" +
                "itemId=" + itemId +
                ", crewId=" + crewId +
                ", sliceTimestamp=" + sliceTimestamp +
                ", reportTotalDura=" + reportTotalDura +
                ", maxSpaceDura=" + maxSpaceDura +
                ", maxSpaceCode='" + maxSpaceCode + '\'' +
                ", maxSpaceName='" + maxSpaceName + '\'' +
                ", spaceInfoDetial='" + spaceInfoDetial + '\'' +
                '}';
    }

    @Override
    public Date getEventTime() { return null; }

    @Override
    public String getKey() { return null; }

    @Override
    public int compareTo(BaseEntity o) { return 0; }
}
