package com.gs.cloud.warehouse.robot.iot.entity;

public class TaskStatus {

    private String status;
    private String taskPauseReason;
    private String id;
    private String name;
    private Integer progress;
    private Double cleaningMileage;
    private Double timeRemaining;
    private Double coverageArea;
    private Double estimateTime;
    private Double length;
    private Boolean loop;
    private Integer loopCount;
    private String taskMapId;
    private String taskMapName;
    private Long modifyTime;
    private String pictureUrl;
    private String taskQueueId;
    private Integer taskQueueType;
    private String tasks;
    private Double totalArea;
    private Integer usedCount;
    private String workModeId;
    private String workModeName;
    private Integer workModeType;
    private Long lastTime;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTaskPauseReason() {
        return taskPauseReason;
    }

    public void setTaskPauseReason(String taskPauseReason) {
        this.taskPauseReason = taskPauseReason;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getProgress() {
        return progress;
    }

    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    public Double getCleaningMileage() {
        return cleaningMileage;
    }

    public void setCleaningMileage(Double cleaningMileage) {
        this.cleaningMileage = cleaningMileage;
    }

    public Double getTimeRemaining() {
        return timeRemaining;
    }

    public void setTimeRemaining(Double timeRemaining) {
        this.timeRemaining = timeRemaining;
    }

    public Double getCoverageArea() {
        return coverageArea;
    }

    public void setCoverageArea(Double coverageArea) {
        this.coverageArea = coverageArea;
    }

    public Double getEstimateTime() {
        return estimateTime;
    }

    public void setEstimateTime(Double estimateTime) {
        this.estimateTime = estimateTime;
    }

    public Double getLength() {
        return length;
    }

    public void setLength(Double length) {
        this.length = length;
    }

    public Boolean getLoop() {
        return loop;
    }

    public void setLoop(Boolean loop) {
        this.loop = loop;
    }

    public Integer getLoopCount() {
        return loopCount;
    }

    public void setLoopCount(Integer loopCount) {
        this.loopCount = loopCount;
    }

    public String getTaskMapId() {
        return taskMapId;
    }

    public void setTaskMapId(String taskMapId) {
        this.taskMapId = taskMapId;
    }

    public String getTaskMapName() {
        return taskMapName;
    }

    public void setTaskMapName(String taskMapName) {
        this.taskMapName = taskMapName;
    }

    public Long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Long modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getPictureUrl() {
        return pictureUrl;
    }

    public void setPictureUrl(String pictureUrl) {
        this.pictureUrl = pictureUrl;
    }

    public String getTaskQueueId() {
        return taskQueueId;
    }

    public void setTaskQueueId(String taskQueueId) {
        this.taskQueueId = taskQueueId;
    }

    public Integer getTaskQueueType() {
        return taskQueueType;
    }

    public void setTaskQueueType(Integer taskQueueType) {
        this.taskQueueType = taskQueueType;
    }

    public String getTasks() {
        return tasks;
    }

    public void setTasks(String tasks) {
        this.tasks = tasks;
    }

    public Double getTotalArea() {
        return totalArea;
    }

    public void setTotalArea(Double totalArea) {
        this.totalArea = totalArea;
    }

    public Integer getUsedCount() {
        return usedCount;
    }

    public void setUsedCount(Integer usedCount) {
        this.usedCount = usedCount;
    }

    public String getWorkModeId() {
        return workModeId;
    }

    public void setWorkModeId(String workModeId) {
        this.workModeId = workModeId;
    }

    public String getWorkModeName() {
        return workModeName;
    }

    public void setWorkModeName(String workModeName) {
        this.workModeName = workModeName;
    }

    public Integer getWorkModeType() {
        return workModeType;
    }

    public void setWorkModeType(Integer workModeType) {
        this.workModeType = workModeType;
    }

    public Long getLastTime() {
        return lastTime;
    }

    public void setLastTime(Long lastTime) {
        this.lastTime = lastTime;
    }

    @Override
    public String toString() {
        return "taskStatus{" +
                "status='" + status + '\'' +
                ", taskPauseReason='" + taskPauseReason + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", progress=" + progress +
                ", cleaningMileage=" + cleaningMileage +
                ", timeRemaining=" + timeRemaining +
                ", coverageArea=" + coverageArea +
                ", estimateTime=" + estimateTime +
                ", length=" + length +
                ", loop=" + loop +
                ", loopCount=" + loopCount +
                ", taskMapId='" + taskMapId + '\'' +
                ", taskMapName='" + taskMapName + '\'' +
                ", modifyTime=" + modifyTime +
                ", pictureUrl='" + pictureUrl + '\'' +
                ", taskQueueId='" + taskQueueId + '\'' +
                ", taskQueueType=" + taskQueueType +
                ", tasks='" + tasks + '\'' +
                ", totalArea=" + totalArea +
                ", usedCount=" + usedCount +
                ", workModeId='" + workModeId + '\'' +
                ", workModeName='" + workModeName + '\'' +
                ", workModeType=" + workModeType +
                ", lastTime=" + lastTime +
                '}';
    }
}
