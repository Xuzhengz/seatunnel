package org.apache.seatunnel.transform.qualityanalysis.metrics;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class QualityAnalysisRowMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private String ruleId;
    private List<QualityAnalysisFieldMetrics> qualityAnalysisFieldMetrics;

    public QualityAnalysisRowMetrics(String ruleId) {
        this.ruleId = ruleId;
    }
}
