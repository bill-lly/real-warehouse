package com.gs.cloud.warehouse.ics.entity;

import com.gs.cloud.warehouse.entity.BaseEntity;

import java.util.Date;

public class PersonLocationStatistics extends BaseEntity {
    private Long itemId;
    private Long crewId;
    private String spaceCode;
    private String spaceName;
    private Long duration;
    private Date startTime;
    private Date endTime;
    private Long sliceTimestamp;

    public Long getItemId() { return itemId; }

    public void setItemId(Long itemId) { this.itemId = itemId; }

    public Long getCrewId() { return crewId; }

    public void setCrewId(Long crewId) { this.crewId = crewId; }

    public String getSpaceCode() { return spaceCode; }

    public void setSpaceCode(String spaceCode) { this.spaceCode = spaceCode; }

    public String getSpaceName() { return spaceName; }

    public void setSpaceName(String spaceName) { this.spaceName = spaceName; }

    public Long getDuration() { return duration; }

    public void setDuration(Long duration) { this.duration = duration; }

    public Date getStartTime() { return startTime; }

    public void setStartTime(Date startTime) { this.startTime = startTime; }

    public Date getEndTime() { return endTime; }

    public void setEndTime(Date endTime) { this.endTime = endTime; }

    public Long getSliceTimestamp() { return sliceTimestamp; }

    public void setSliceTimestamp(Long slice_timestamp) { this.sliceTimestamp = slice_timestamp; }

    @Override
    public String toString() {
        return "PersonLocationStatistics{" +
                "itemId=" + itemId +
                ", crewId=" + crewId +
                ", spaceCode='" + spaceCode + '\'' +
                ", spaceName='" + spaceName + '\'' +
                ", duration=" + duration +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", sliceTimestamp=" + sliceTimestamp +
                '}';
    }

    @Override
    public Date getEventTime() { return null; }

    @Override
    public String getKey() { return null; }

    @Override
    public int compareTo(BaseEntity o) { return 0; }
}
