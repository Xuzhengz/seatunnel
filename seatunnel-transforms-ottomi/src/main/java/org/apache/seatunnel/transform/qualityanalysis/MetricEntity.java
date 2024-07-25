package org.apache.seatunnel.transform.qualityanalysis;

import cn.hutool.core.lang.Dict;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author 徐正洲
 * @date 2024/7/23 12:25
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEntity implements Serializable {
    private Dict ruleInfos;
    private boolean open;
    private long dirtyDataLimit;
    private String modelId;
    private String resource;
    private String resourceComment;
    private String dsId;
    private String dsName;
    private String metricApi;
    private String jobMode;
    private String deptId;
    private String deptName;
    private String tenantId;
    private String createBy;
}
