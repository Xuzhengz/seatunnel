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
import org.apache.seatunnel.transform.qualityanalysis.metrics.QualityAnalysisFieldMetrics;
import org.apache.seatunnel.transform.qualityanalysis.metrics.QualityAnalysisMetrics;
import org.apache.seatunnel.transform.qualityanalysis.metrics.QualityAnalysisRowMetrics;
import org.apache.seatunnel.transform.qualityanalysis.metrics.QualityAnalysisTableMetrics;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class QualityAnalysisTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "QualityAnalysis";

    // 配置类
    private QualityAnalysisConfig config;
    // 指标类
    private QualityAnalysisMetrics qualityAnalysisMetrics;
    private final ReadonlyConfig readonlyConfig;
    private Map<String, List<QualityAnalysisFieldMetrics>> ruleMap;
    // 脏数据量
    private static long dirtyDataNum = 0L;
    private static ScheduledExecutorService scheduledExecutorService;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    public QualityAnalysisTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.readonlyConfig = config;
    }

    @Override
    public void open() {
        // 初始化配置信息
        initConfig(readonlyConfig);
        // 初始化指标信息
        qualityAnalysisMetrics = new QualityAnalysisMetrics();
        qualityAnalysisMetrics.initMetrics(config);
        ruleMap =
                qualityAnalysisMetrics.getQualityAnalysisRowMetrics().stream()
                        .collect(
                                Collectors.toMap(
                                        QualityAnalysisRowMetrics::getRuleId,
                                        QualityAnalysisRowMetrics::getQualityAnalysisFieldMetrics));
        // 开启统计线程
        if (!"batch".equals(config.getJobMode())) {
            startMonitor();
        }
    }

    private void initConfig(ReadonlyConfig config) {
        try {
            this.config =
                    QualityAnalysisConfig.builder()
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

    private void startMonitor() {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(
                () ->
                        // TODO SEND STATISTICS
                        sendMetric(false),
                0,
                10,
                TimeUnit.SECONDS);
    }

    private void sendMetric(boolean isLast) {
        try {
            AtomicLong dealDataNum =
                    qualityAnalysisMetrics.getQualityAnalysisTableMetrics().getDealDataNum();

            if (dealDataNum.get() > 0 || isLast) {
                HttpUtil.createPost(config.getMetricApi())
                        .body(qualityAnalysisMetrics.getMetrics(config))
                        .execute();
                // clean
                qualityAnalysisMetrics.cleanMetrics();
            }
        } catch (Exception e) {
            Console.error("send quality metric error：" + e.getMessage());
        }
    }

    private void sendDirtyNotice() {
        try {
            Map<String, Object> formMap = new HashMap<>();
            formMap.put(Constants.CREATE_BY, config.getCreateBy());
            formMap.put(Constants.MODEL_NAME, config.getModelName());
            formMap.put(Constants.RESOURCE, config.getResource());
            HttpUtil.createPost(config.getWarningApi()).form(formMap).executeAsync();
        } catch (Exception e) {
            Console.error("send dirty data notice error：" + e.getMessage());
        }
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        // check info
        Set<String> rules = config.getRuleInfos().keySet();
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
        QualityAnalysisTableMetrics qualityAnalysisTableMetrics =
                qualityAnalysisMetrics.getQualityAnalysisTableMetrics();
        qualityAnalysisTableMetrics.getDealDataNum().incrementAndGet();
        if (rowCheckResult) {
            qualityAnalysisTableMetrics.getNeatDataNum().incrementAndGet();
        } else {
            qualityAnalysisTableMetrics.getDirtyDataNum().incrementAndGet();
        }
        if (!config.isPreview()) {
            // send dirty data notice
            if (config.getDirtyDataLimit() > 0 && dirtyDataNum == config.getDirtyDataLimit()) {
                Console.error("dirty data has exceed config limit,please check.");
                sendDirtyNotice();
            }
        }
        // TODO IF OPEN THEN ADD CHECK_RESULT
        if (config.isOpen()) {
            return new Object[] {String.valueOf(rowCheckResult)};
        }
        return new Object[0];
    }

    private boolean runCheck(String rule, SeaTunnelRowAccessor inputRow) {
        RowKind rowKind = inputRow.getRowKind();
        List<Boolean> results = new ArrayList<>();
        JSONObject obj = (JSONObject) config.getRuleInfos().getObj(rule);
        String ruleType = obj.getStr(org.apache.seatunnel.transform.datascan.Constants.RULE_TYPE);
        String ruleCode = obj.getStr(org.apache.seatunnel.transform.datascan.Constants.RULE_CODE);
        List<String> columnNames =
                obj.getByPath(
                        org.apache.seatunnel.transform.datascan.Constants.COLUMN_NAMES, List.class);
        // doCheck
        IRuleHandler ruleHandler = RuleFactory.getRule(ruleType, ruleCode);
        List<QualityAnalysisFieldMetrics> qualityAnalysisFieldMetricsList = ruleMap.get(rule);
        for (QualityAnalysisFieldMetrics qualityAnalysisFieldMetrics :
                qualityAnalysisFieldMetricsList) {
            String fieldName = qualityAnalysisFieldMetrics.getFieldName();
            int index = columnNames.indexOf(fieldName);
            Object value = inputRow.getField(index);
            boolean result = ruleHandler.doCheck((String.valueOf(value)));
            results.add(result);
            // do insert type data
            if (RowKind.INSERT.equals(rowKind)) {
                qualityAnalysisFieldMetrics.getDealDataNum().incrementAndGet();
                if (result) {
                    qualityAnalysisFieldMetrics.getNeatDataNum().incrementAndGet();
                } else {
                    qualityAnalysisFieldMetrics.getDirtyDataNum().incrementAndGet();
                }
            }
        }
        // simple rule check result
        return !results.contains(false);
    }

    @Override
    protected Column[] getOutputColumns() {
        Boolean isOpen =
                readonlyConfig.get(QualityAnalysisTransformConfig.OPEN_TRUE_OR_FALSE_OUTPUT);
        if (isOpen) {
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
        if (!config.isPreview()) {
            sendMetric(true);
        }
    }
}
