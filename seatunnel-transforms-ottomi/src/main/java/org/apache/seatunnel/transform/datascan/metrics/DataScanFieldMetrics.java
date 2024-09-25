package org.apache.seatunnel.transform.datascan.metrics;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:31
 */
@Data
public class DataScanFieldMetrics {

    private String fieldName;
    private String fieldLabelId;
    private String ruleName;
    private AtomicLong dealDataNum = new AtomicLong(0L);
    private AtomicLong neatDataNum = new AtomicLong(0L);
    private AtomicLong dirtyDataNum = new AtomicLong(0L);

    public DataScanFieldMetrics(String ruleName, String fieldName, String fieldLabelId) {
        this.ruleName = ruleName;
        this.fieldName = fieldName;
        this.fieldLabelId = fieldLabelId;
    }
}
