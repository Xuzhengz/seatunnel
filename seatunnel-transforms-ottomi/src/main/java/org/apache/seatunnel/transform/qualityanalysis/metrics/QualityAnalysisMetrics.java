package org.apache.seatunnel.transform.qualityanalysis.metrics;

import org.apache.seatunnel.transform.qualityanalysis.Constants;
import org.apache.seatunnel.transform.qualityanalysis.QualityAnalysisConfig;

import cn.hutool.core.lang.Dict;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class QualityAnalysisMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private QualityAnalysisTableMetrics qualityAnalysisTableMetrics;
    private List<QualityAnalysisRowMetrics> qualityAnalysisRowMetrics;
    // 数据源id
    private String dsId;
    // 数据源名称
    private String dsName;
    // 资源名称
    private String resource;
    // 资源描述
    private String resourceComment;
    // 模型id
    private String modelId;
    // 执行模式
    private String jobMode;
    // 部门id
    private String deptId;
    // 部门名称
    private String deptName;
    // 租户id
    private String tenantId;
    // 创建人
    private String createBy;

    public void initMetrics(QualityAnalysisConfig config) {
        // base info
        this.setDsId(config.getDsId());
        this.setDsName(config.getDsName());
        this.setResource(config.getResource());
        this.setResourceComment(config.getResourceComment());
        this.setModelId(config.getModelId());
        this.setJobMode(config.getJobMode());
        this.setDeptId(config.getDeptId());
        this.setDeptName(config.getDeptName());
        this.setTenantId(config.getTenantId());
        this.setCreateBy(config.getCreateBy());
        // table
        Dict ruleInfos = config.getRuleInfos();
        QualityAnalysisTableMetrics qualityAnalysisTableMetrics = new QualityAnalysisTableMetrics();
        this.setQualityAnalysisTableMetrics(qualityAnalysisTableMetrics);
        // row
        List<QualityAnalysisRowMetrics> qualityAnalysisRowMetricsList =
                new ArrayList<>(ruleInfos.keySet().size());
        for (Map.Entry<String, Object> ruleEntry : ruleInfos.entrySet()) {
            String ruleId = ruleEntry.getKey();
            QualityAnalysisRowMetrics qualityAnalysisRowMetrics =
                    new QualityAnalysisRowMetrics(ruleId);
            JSONObject value = (JSONObject) ruleEntry.getValue();
            String ruleName = value.getStr(Constants.RULE_NAME);
            List<String> columns = value.getBeanList(Constants.COLUMN_NAMES, String.class);
            List<QualityAnalysisFieldMetrics> qualityAnalysisFieldMetricsList =
                    new ArrayList<>(columns.size());
            for (String column : columns) {
                QualityAnalysisFieldMetrics qualityAnalysisFieldMetrics =
                        new QualityAnalysisFieldMetrics(ruleName, column);
                qualityAnalysisFieldMetricsList.add(qualityAnalysisFieldMetrics);
            }
            qualityAnalysisRowMetrics.setQualityAnalysisFieldMetrics(
                    qualityAnalysisFieldMetricsList);
            qualityAnalysisRowMetricsList.add(qualityAnalysisRowMetrics);
        }
        this.setQualityAnalysisRowMetrics(qualityAnalysisRowMetricsList);
    }

    public String getMetrics(QualityAnalysisConfig config) {
        Map<String, Object> metricsMap = new HashMap<>();
        metricsMap.put(Constants.DATASOURCE_ID, config.getDsId());
        metricsMap.put(Constants.DATASOURCE_NAME, config.getDsName());
        metricsMap.put(Constants.RESOURCE, config.getResource());
        metricsMap.put(Constants.RESOURCE_COMMENT, config.getResourceComment());
        metricsMap.put(Constants.MODEL_ID, config.getModelId());
        metricsMap.put(Constants.QUALITY_JOB_MODE, config.getJobMode());
        metricsMap.put(Constants.DEPT_ID, config.getDeptId());
        metricsMap.put(Constants.DEPT_NAME, config.getDeptName());
        metricsMap.put(Constants.TENANT_ID, config.getTenantId());
        metricsMap.put(Constants.CREATE_BY, config.getCreateBy());
        // set table metrics
        QualityAnalysisTableMetrics table = this.getQualityAnalysisTableMetrics();
        JSONObject tableMetrics = new JSONObject(table);
        metricsMap.put("table", tableMetrics);
        // set row metrics
        List<QualityAnalysisRowMetrics> qualityAnalysisRowMetricsList =
                this.getQualityAnalysisRowMetrics();
        Map<String, Object> row = new HashMap<>();
        for (QualityAnalysisRowMetrics qualityAnalysisRowMetrics : qualityAnalysisRowMetricsList) {
            List<QualityAnalysisFieldMetrics> dataScanFieldMetrics =
                    qualityAnalysisRowMetrics.getQualityAnalysisFieldMetrics();
            Map<String, Object> field = new HashMap<>();
            for (QualityAnalysisFieldMetrics qualityAnalysisFieldMetrics : dataScanFieldMetrics) {
                JSONObject fieldMetrics = new JSONObject(qualityAnalysisFieldMetrics);
                fieldMetrics.remove("fieldName");
                field.put(qualityAnalysisFieldMetrics.getFieldName(), fieldMetrics);
            }
            row.put(qualityAnalysisRowMetrics.getRuleId(), field);
        }
        metricsMap.put("row", row);
        return JSONUtil.toJsonStr(metricsMap);
    }

    public void cleanMetrics() {
        // clean table metrics
        QualityAnalysisTableMetrics table = this.getQualityAnalysisTableMetrics();
        table.setDealDataNum(new AtomicLong(0L));
        table.setNeatDataNum(new AtomicLong(0L));
        table.setDirtyDataNum(new AtomicLong(0L));
        // clean row metrics
        List<QualityAnalysisRowMetrics> qualityAnalysisRowMetricsList =
                this.getQualityAnalysisRowMetrics();
        for (QualityAnalysisRowMetrics qualityAnalysisRowMetrics : qualityAnalysisRowMetricsList) {
            List<QualityAnalysisFieldMetrics> dataScanFieldMetrics =
                    qualityAnalysisRowMetrics.getQualityAnalysisFieldMetrics();
            for (QualityAnalysisFieldMetrics qualityAnalysisFieldMetrics : dataScanFieldMetrics) {
                qualityAnalysisFieldMetrics.setDealDataNum(new AtomicLong(0L));
                qualityAnalysisFieldMetrics.setNeatDataNum(new AtomicLong(0L));
                qualityAnalysisFieldMetrics.setDirtyDataNum(new AtomicLong(0L));
            }
        }
    }
}
