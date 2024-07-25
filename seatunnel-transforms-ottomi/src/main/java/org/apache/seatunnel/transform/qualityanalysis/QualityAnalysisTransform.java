/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.qualityanalysis;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import cn.hutool.core.lang.Console;
import cn.hutool.core.lang.Dict;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.oceandatum.quality.common.rulebase.IRuleHandler;
import com.oceandatum.quality.common.rulebase.RuleFactory;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QualityAnalysisTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "QualityAnalysis";
    // 配置类
    private MetricEntity entity;
    // 指标信息
    private static Map<String, Object> metricsMap;
    // 脏数据量
    private static long dirtyDataNum = 0l;
    private static ScheduledExecutorService scheduledExecutorService;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    public QualityAnalysisTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        // 初始化配置信息
        initConfig(config);
        // 初始化统计类
        initMetric();
        // 开启统计线程
        if (!"batch".equals(entity.getJobMode())) {
            startMonitor();
        }
    }

    private void initConfig(ReadonlyConfig config) {
        try {
            entity =
                    MetricEntity.builder()
                            .ruleInfos(
                                    JSONUtil.toBean(
                                            config.getOptional(
                                                            QualityAnalysisTransformConfig
                                                                    .RULE_INFO)
                                                    .get(),
                                            Dict.class))
                            .open(
                                    config.getOptional(
                                                    QualityAnalysisTransformConfig
                                                            .OPEN_TRUE_OR_FALSE_OUTPUT)
                                            .get())
                            .dirtyDataLimit(
                                    config.getOptional(
                                                    QualityAnalysisTransformConfig.DIRTY_DATA_LIMIT)
                                            .get())
                            .modelId(
                                    config.getOptional(QualityAnalysisTransformConfig.MODEL_ID)
                                            .get())
                            .dsId(
                                    config.getOptional(QualityAnalysisTransformConfig.DATASOURCE_ID)
                                            .orElse(StrUtil.EMPTY))
                            .dsName(
                                    config.getOptional(
                                                    QualityAnalysisTransformConfig.DATASOURCE_NAME)
                                            .orElse(StrUtil.EMPTY))
                            .resource(
                                    config.getOptional(QualityAnalysisTransformConfig.RESOURCE)
                                            .orElse(StrUtil.EMPTY))
                            .resourceComment(
                                    config.getOptional(
                                                    QualityAnalysisTransformConfig.RESOURCE_COMMENT)
                                            .orElse(StrUtil.EMPTY))
                            .metricApi(
                                    config.getOptional(QualityAnalysisTransformConfig.METRIC_API)
                                            .get())
                            .jobMode(
                                    config.getOptional(QualityAnalysisTransformConfig.JOB_MODE)
                                            .get())
                            .deptId(
                                    config.getOptional(QualityAnalysisTransformConfig.DEPT_ID)
                                            .get())
                            .deptName(
                                    config.getOptional(QualityAnalysisTransformConfig.DEPT_NAME)
                                            .get())
                            .tenantId(
                                    config.getOptional(QualityAnalysisTransformConfig.TENANT_ID)
                                            .get())
                            .createBy(
                                    config.getOptional(QualityAnalysisTransformConfig.CREATE_BY)
                                            .get())
                            .preview(
                                    config.getOptional(QualityAnalysisTransformConfig.IS_PREVIEW)
                                            .orElse(false))
                            .warningApi(
                                    config.getOptional(QualityAnalysisTransformConfig.WARNING_API)
                                            .get())
                            .modelName(
                                    config.getOptional(QualityAnalysisTransformConfig.MODEL_NAME)
                                            .get())
                            .build();
        } catch (Exception e) {
            throw new RuntimeException("init transform config error：", e);
        }
    }

    private void initMetric() {
        try {
            metricsMap = new HashMap<>();
            metricsMap.put(Constants.DATASOURCE_ID, entity.getDsId());
            metricsMap.put(Constants.DATASOURCE_NAME, entity.getDsName());
            metricsMap.put(Constants.RESOURCE, entity.getResource());
            metricsMap.put(Constants.RESOURCE_COMMENT, entity.getResourceComment());
            metricsMap.put(Constants.MODEL_ID, entity.getModelId());
            metricsMap.put(Constants.QUALITY_JOB_MODE, entity.getJobMode());
            metricsMap.put(Constants.DEPT_ID, entity.getDeptId());
            metricsMap.put(Constants.DEPT_NAME, entity.getDeptName());
            metricsMap.put(Constants.TENANT_ID, entity.getTenantId());
            metricsMap.put(Constants.CREATE_BY, entity.getCreateBy());
            // table level
            Map<String, Object> tableMetric = new HashMap<>();
            tableMetric.put(Constants.DEAL_DATA_NUM, 0);
            tableMetric.put(Constants.NEAT_DATA_NUM, 0);
            tableMetric.put(Constants.DIRTY_DATA_NUM, 0);
            metricsMap.put(Constants.TABLE_LEVEL, tableMetric);
            // row level
            Map<String, Object> rowMetric = new HashMap<>();
            for (Map.Entry<String, Object> entry : entity.getRuleInfos().entrySet()) {
                Map<String, Object> ruleMetric = new HashMap<>();
                String ruleId = entry.getKey();
                JSONObject value = (JSONObject) entry.getValue();
                String ruleName = value.getStr(Constants.RULE_NAME);
                List<String> columns = value.getBeanList(Constants.COLUMN_NAMES, String.class);
                columns.forEach(
                        c -> {
                            Map<String, Object> columnMetric = new HashMap<>();
                            columnMetric.put(Constants.RULE_NAME, ruleName);
                            columnMetric.put(Constants.DEAL_DATA_NUM, 0);
                            columnMetric.put(Constants.NEAT_DATA_NUM, 0);
                            columnMetric.put(Constants.DIRTY_DATA_NUM, 0);
                            ruleMetric.put(c, columnMetric);
                        });
                rowMetric.put(ruleId, ruleMetric);
            }
            metricsMap.put(Constants.ROW_LEVEL, rowMetric);
        } catch (Exception e) {
            throw new RuntimeException("init metric config error：", e);
        }
    }

    private void startMonitor() {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(
                () ->
                        // TODO SEND STATISTICS
                        sendMetric(false),
                10,
                10,
                TimeUnit.SECONDS);
    }

    private void sendMetric(boolean isLast) {
        try {
            synchronized (metricsMap) {
                JSONObject tableLevel = new JSONObject(metricsMap.get(Constants.TABLE_LEVEL));
                Long dealNum = tableLevel.getLong(Constants.DEAL_DATA_NUM);
                if (dealNum > 0 || isLast) {
                    HttpUtil.createPost(entity.getMetricApi())
                            .body(JSONUtil.toJsonStr(metricsMap))
                            .executeAsync();
                    // clean
                    initMetric();
                }
            }
        } catch (Exception e) {
            Console.error("send quality metric error：" + e.getMessage());
        }
    }

    private void sendDirtyNotice() {
        try {
            Map<String, Object> formMap = new HashMap<>();
            formMap.put(Constants.CREATE_BY, entity.getCreateBy());
            formMap.put(Constants.MODEL_NAME, entity.getModelName());
            formMap.put(Constants.RESOURCE, entity.getResource());
            HttpUtil.createPost(entity.getWarningApi()).form(formMap).executeAsync();
        } catch (Exception e) {
            Console.error("send dirty data notice error：" + e.getMessage());
        }
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        // check info
        Set<String> rules = entity.getRuleInfos().keySet();
        List<Boolean> results = new ArrayList<>();
        for (String rule : rules) {
            // rule level collect check result info
            boolean result = runCheck(rule, inputRow);
            results.add(result);
        }
        // table level collect check result info
        boolean rowCheckResult;
        if (results.contains(false)) {
            dirtyDataNum++;
            rowCheckResult = false;
        } else {
            rowCheckResult = true;
        }
        // update metrics
        JSONObject tableLevel = new JSONObject(metricsMap.get(Constants.TABLE_LEVEL));
        tableLevel.set(Constants.DEAL_DATA_NUM, tableLevel.getLong(Constants.DEAL_DATA_NUM) + 1);
        if (rowCheckResult) {
            tableLevel.set(
                    Constants.NEAT_DATA_NUM, tableLevel.getLong(Constants.NEAT_DATA_NUM) + 1);
        } else {
            tableLevel.set(
                    Constants.DIRTY_DATA_NUM, tableLevel.getLong(Constants.DIRTY_DATA_NUM) + 1);
        }
        metricsMap.put(Constants.TABLE_LEVEL, tableLevel);
        // send dirty data notice
        if (entity.getDirtyDataLimit() > 0 && dirtyDataNum == entity.getDirtyDataLimit()) {
            Console.error("dirty data has exceed config limit,please check.");
            if (!entity.isPreview()) {
                sendDirtyNotice();
            }
        }
        // TODO IF OPEN THEN ADD CHECK_RESULT
        if (entity.isOpen()) {
            return new Object[] {String.valueOf(rowCheckResult)};
        }
        return new Object[0];
    }

    private boolean runCheck(String rule, SeaTunnelRowAccessor inputRow) {
        RowKind rowKind = inputRow.getRowKind();
        List<Boolean> results = new ArrayList<>();
        JSONObject obj = (JSONObject) entity.getRuleInfos().getObj(rule);
        String ruleType = obj.getStr(Constants.RULE_TYPE);
        String ruleCode = obj.getStr(Constants.RULE_CODE);
        List<Integer> columns = obj.getByPath(Constants.COLUMNS, List.class);
        List<String> columnNames = obj.getByPath(Constants.COLUMN_NAMES, List.class);
        IRuleHandler ruleHandler = RuleFactory.getRule(ruleType, ruleCode);
        JSONObject rowLevel = new JSONObject(metricsMap.get(Constants.ROW_LEVEL));
        JSONObject ruleLevel = rowLevel.getJSONObject(rule);
        for (int i = 0; i < columns.size(); i++) {
            Object value = inputRow.getField(i);
            boolean result = ruleHandler.doCheck((String.valueOf(value)));
            results.add(result);
            if (RowKind.INSERT.equals(rowKind)) {
                JSONObject columnLevel = ruleLevel.getJSONObject(columnNames.get(i));
                columnLevel.set(
                        Constants.DEAL_DATA_NUM, columnLevel.getLong(Constants.DEAL_DATA_NUM) + 1);
                if (result) {
                    columnLevel.set(
                            Constants.NEAT_DATA_NUM,
                            columnLevel.getLong(Constants.NEAT_DATA_NUM) + 1);
                } else {
                    columnLevel.set(
                            Constants.DIRTY_DATA_NUM,
                            columnLevel.getLong(Constants.DIRTY_DATA_NUM) + 1);
                }
                ruleLevel.set(columnNames.get(i), columnLevel);
            }
        }
        rowLevel.set(rule, ruleLevel);
        metricsMap.put(Constants.ROW_LEVEL, rowLevel);
        // simple rule check result
        return results.contains(false) ? false : true;
    }

    @Override
    protected Column[] getOutputColumns() {
        if (entity.isOpen()) {
            return Arrays.stream(new String[] {Constants.CHECK_RESULT_COLUMN})
                    .map(
                            fieldName ->
                                    PhysicalColumn.of(
                                            fieldName, BasicType.STRING_TYPE, 255, true, "", ""))
                    .toArray(Column[]::new);
        }
        return new Column[0];
    }

    @Override
    public void close() {
        if (ObjUtil.isNotEmpty(scheduledExecutorService)
                && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdownNow();
        }
        // last send metrics
        if (!entity.isPreview()) {
            sendMetric(true);
        }
    }
}
