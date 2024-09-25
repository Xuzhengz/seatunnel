package org.apache.seatunnel.transform.datascan.metrics;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class DataScanTableMetrics {

    private AtomicLong dealDataNum = new AtomicLong(0L);
    private AtomicLong neatDataNum = new AtomicLong(0L);
    private AtomicLong dirtyDataNum = new AtomicLong(0L);
}
