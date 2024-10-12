package org.apache.seatunnel.transform.qualityanalysis.metrics;

import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class QualityAnalysisTableMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private AtomicLong dealDataNum = new AtomicLong(0L);
    private AtomicLong neatDataNum = new AtomicLong(0L);
    private AtomicLong dirtyDataNum = new AtomicLong(0L);
}
