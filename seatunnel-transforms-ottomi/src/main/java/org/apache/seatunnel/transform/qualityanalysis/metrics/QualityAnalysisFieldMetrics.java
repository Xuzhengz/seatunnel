package org.apache.seatunnel.transform.qualityanalysis.metrics;

import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:31
 */
@Data
public class QualityAnalysisFieldMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fieldName;
    private String ruleName;
    private AtomicLong dealDataNum = new AtomicLong(0L);
    private AtomicLong neatDataNum = new AtomicLong(0L);
    private AtomicLong dirtyDataNum = new AtomicLong(0L);

    public QualityAnalysisFieldMetrics(String ruleName, String fieldName) {
        this.ruleName = ruleName;
        this.fieldName = fieldName;
    }
}
