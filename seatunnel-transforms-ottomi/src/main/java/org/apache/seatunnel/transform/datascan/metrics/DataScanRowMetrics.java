package org.apache.seatunnel.transform.datascan.metrics;

import lombok.Data;

import java.util.List;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class DataScanRowMetrics {
    private String ruleId;
    private List<DataScanFieldMetrics> dataScanFieldMetrics;

    public DataScanRowMetrics(String ruleId) {
        this.ruleId = ruleId;
    }
}
