package com.gs.cloud.warehouse.ics.sink;

import com.gs.cloud.warehouse.ics.entity.ItemCrewCardReportTriggerIncident;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

// 定义一个枚举来区分操作类型
enum OperationType {
    INSERT,
    UPDATE
}

// 定义一个类来封装操作信息
class Operation {
    OperationType type;
    ItemCrewCardReportTriggerIncident element;

    public Operation(OperationType type, ItemCrewCardReportTriggerIncident element) {
        this.type = type;
        this.element = element;
    }
}

public class CardAbnormalJdbcSink extends RichSinkFunction<ItemCrewCardReportTriggerIncident> implements CheckpointedFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private transient Connection conn;
    private transient PreparedStatement insertStmt;
    private transient PreparedStatement updateStmt;
    private Properties properties;
    private int batchSize = 500;
    private long batchIntervalMs = 1000; // 时间间隔，单位毫秒
    private final ReentrantLock queueLock = new ReentrantLock();
    private List<Operation> operationQueue = new ArrayList<>();
    private long lastExecutionTime;
    // 新增状态变量：用于持久化未提交的操作队列
    private transient ListState<Operation> checkpointedState;

    public CardAbnormalJdbcSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection(
                properties.getProperty("ics.datav.jdbc.url"),
                properties.getProperty("ics.datav.jdbc.user.name"),
                properties.getProperty("ics.datav.jdbc.password")
        );
        String insertSql = "replace into ics_datav.t_card_abnormal " +
                "(item_id, crew_id, card_sn, card_abnormal_type, card_abnormal_status, metadata, closed_time, created_time, updated_time)" +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        insertStmt = conn.prepareStatement(insertSql);

        String updateSql = "update ics_datav.t_card_abnormal SET card_abnormal_status = ?, closed_time = ?, updated_time = ? WHERE card_sn = ? and card_abnormal_type = ? and closed_time is null";
        updateStmt = conn.prepareStatement(updateSql);
        lastExecutionTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(ItemCrewCardReportTriggerIncident element, Context context) throws Exception {
        queueLock.lock();
        try {
            if(element.getTriggerIncidentName().equals("工牌低电量")){
                operationQueue.add(new Operation(OperationType.INSERT, element));
            }
            if(element.getTriggerIncidentName().equals("工牌充电")){
                operationQueue.add(new Operation(OperationType.UPDATE, element));
            }
            if (shouldFlush()) {
                executeBatchOperations();
                operationQueue.clear();
                lastExecutionTime = System.currentTimeMillis();
            }
        } finally {
            queueLock.unlock();
        }
    }

    private boolean shouldFlush() {
        return operationQueue.size() >= batchSize ||
                (System.currentTimeMillis() - lastExecutionTime) >= batchIntervalMs;
    }

    private void executeBatchOperations() throws SQLException {
        try{
            conn.setAutoCommit(false); // 关闭自动提交
            for (Operation operation : operationQueue) {
                if (operation.type == OperationType.INSERT) {
                    ItemCrewCardReportTriggerIncident element = operation.element;
                    System.out.println(element);
                    insertStmt.setLong(1, element.getItemId());
                    insertStmt.setLong(2, element.getCrewId());
                    insertStmt.setString(3, element.getCardSn());
                    insertStmt.setString(4, "GP003");
                    insertStmt.setString(5, "ON");

                    JsonNode resultJson = objectMapper.createObjectNode();
                    ((ObjectNode) resultJson).put("last_electricity", element.getTrigIncidLastValue());
                    ((ObjectNode) resultJson).put("current_electricity", element.getTriggerIncidentValue());
                    ((ObjectNode) resultJson).put("threshold_value", element.getTrigIncidThresholdValue());
                    insertStmt.setString(6, resultJson.toString());
                    insertStmt.setTimestamp(7, null);
                    insertStmt.setTimestamp(8, new Timestamp(element.getCloudTime().getTime()));
                    insertStmt.setTimestamp(9, new Timestamp(element.getCloudTime().getTime()));
                    insertStmt.executeUpdate();
                } else if (operation.type == OperationType.UPDATE) {
                    ItemCrewCardReportTriggerIncident element = operation.element;
                    updateStmt.setString(1, "OFF");
                    updateStmt.setTimestamp(2, new Timestamp(element.getCloudTime().getTime()));
                    updateStmt.setTimestamp(3, new Timestamp(element.getCloudTime().getTime()));
                    updateStmt.setString(4, element.getCardSn());
                    updateStmt.setString(5, "GP003");
                    updateStmt.executeUpdate();
                }
            }
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            conn.rollback();
            throw new SQLException("Batch operation failed", e);
        }
    }


    @Override
    public void close() throws Exception {
        try {
            if (!operationQueue.isEmpty()) {
                executeBatchOperations();
            }
        } catch (SQLException e) {
            System.err.println("关闭时执行批处理出错: " + e.getMessage());
            e.printStackTrace();
        }
        if (insertStmt != null) {
            insertStmt.close();
        }
        if (updateStmt != null) {
            updateStmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        queueLock.lock();
        try {
            // Step 1: 强制提交当前缓冲区的所有操作
            if (!operationQueue.isEmpty()) {
                executeBatchOperations();
                operationQueue.clear();
            }

            // Step 2: 将剩余未提交操作（如果有）保存到 Checkpoint 状态
            checkpointedState.clear();
            checkpointedState.addAll(operationQueue);
        } finally {
            queueLock.unlock();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    // 定义状态描述符
        ListStateDescriptor<Operation> descriptor = new ListStateDescriptor<>(
                "operation-queue-state",
                TypeInformation.of(Operation.class)
        );

        // 初始化状态（任务恢复时加载数据）
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            // 从 Checkpoint 恢复未提交的操作
            queueLock.lock();
            try {
                for (Operation operation : checkpointedState.get()) {
                    operationQueue.add(operation);
                }
                // 立即提交恢复的数据（可选）
                if (!operationQueue.isEmpty()) {
                    executeBatchOperations();
                    operationQueue.clear();
                }
            } finally {
                queueLock.unlock();
            }
        }
    }
}