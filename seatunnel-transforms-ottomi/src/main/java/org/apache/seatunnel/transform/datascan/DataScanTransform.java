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

package org.apache.seatunnel.transform.datascan;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import cn.hutool.core.lang.Console;
import cn.hutool.core.lang.Dict;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.oceandatum.quality.common.rulebase.IRuleHandler;
import com.oceandatum.quality.common.rulebase.RuleFactory;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DataScanTransform extends AbstractCatalogSupportTransform {

    public static final String PLUGIN_NAME = "DataScan";
    // 配置类
    private MetricEntity entity;
    // 指标信息
    private Map<String, Object> metricsMap;
    // 是否运行
    private static boolean RUNNING = true;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    public DataScanTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        // 初始化配置信息
        initConfig(config);
        // 初始化统计类
        initMetric();
    }

    private void initConfig(ReadonlyConfig config) {
        try {
            entity =
                    MetricEntity.builder()
                            .ruleInfos(
                                    JSONUtil.toBean(
                                            config.getOptional(DataScanTransformConfig.RULE_INFO)
                                                    .get(),
                                            Dict.class))
                            .redisHost(
                                    config.getOptional(DataScanTransformConfig.REDIS_HOST)
                                            .orElse("127.0.0.1"))
                            .redisPort(
                                    config.getOptional(DataScanTransformConfig.REDIS_PORT)
                                            .orElse(6379))
                            .redisDbNum(
                                    config.getOptional(DataScanTransformConfig.REDIS_DB_NUM)
                                            .orElse(0))
                            .redisKey(config.getOptional(DataScanTransformConfig.REDIS_KEY).get())
                            .redisUser(
                                    config.getOptional(DataScanTransformConfig.REDIS_USER)
                                            .orElse(null))
                            .redisPassword(
                                    config.getOptional(DataScanTransformConfig.REDIS_PASSWORD)
                                            .orElse(null))
                            .build();
        } catch (Exception e) {
            throw new RuntimeException("init data scan transform config error：", e);
        }
    }

    private void initMetric() {
        try {
            metricsMap = new ConcurrentHashMap<>();
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
                String fieldLabel = "";
                if (ruleId.contains(":")) {
                    String[] split = ruleId.split(":");
                    ruleId = split[0];
                    fieldLabel = split[1];
                }
                JSONObject value = (JSONObject) entry.getValue();
                String ruleName = value.getStr(Constants.RULE_NAME);
                List<String> columns = value.getBeanList(Constants.COLUMN_NAMES, String.class);
                for (String column : columns) {
                    Map<String, Object> columnMetric = new HashMap<>();
                    columnMetric.put(Constants.RULE_NAME, ruleName);
                    columnMetric.put(Constants.DEAL_DATA_NUM, 0);
                    columnMetric.put(Constants.NEAT_DATA_NUM, 0);
                    columnMetric.put(Constants.DIRTY_DATA_NUM, 0);
                    columnMetric.put(Constants.FIELD_LABEL_ID, fieldLabel);
                    ruleMetric.put(column, columnMetric);
                }
                rowMetric.put(ruleId, ruleMetric);
            }
            metricsMap.put(Constants.ROW_LEVEL, rowMetric);
            log.info("初始化metricsMap：{}", metricsMap);
        } catch (Exception e) {
            throw new RuntimeException("init metric config error：", e);
        }
    }

    private boolean runCheck(String rule, SeaTunnelRow inputRow) {
        log.info("规则id：" + rule);
        RowKind rowKind = inputRow.getRowKind();
        List<Boolean> results = new ArrayList<>();
        JSONObject obj = (JSONObject) entity.getRuleInfos().getObj(rule);
        String ruleType = obj.getStr(Constants.RULE_TYPE);
        log.info("规则类型：" + ruleType);
        String ruleCode = obj.getStr(Constants.RULE_CODE);
        log.info("规则Code：" + ruleCode);
        List<Integer> columns = obj.getByPath(Constants.COLUMNS, List.class);
        log.info("字段下标索引：" + columns);
        List<String> columnNames = obj.getByPath(Constants.COLUMN_NAMES, List.class);
        log.info("字段下标名称索引：" + columnNames);
        IRuleHandler ruleHandler = RuleFactory.getRule(ruleType, ruleCode);
        log.info("metricsMap：" + metricsMap);
        JSONObject rowLevel = new JSONObject(metricsMap.get(Constants.ROW_LEVEL));
        JSONObject ruleLevel = rowLevel.getJSONObject(rule);
        log.info("当前规则Id：" + rule);
        log.info("当前规则结构：" + ruleLevel);
        for (int i = 0; i < columns.size(); i++) {
            Object value = inputRow.getField(i);
            boolean result = ruleHandler.doCheck((String.valueOf(value)));
            results.add(result);
            if (RowKind.INSERT.equals(rowKind)) {
                log.info("当前字段名称：" + columnNames.get(i));
                JSONObject columnLevel = ruleLevel.getJSONObject(columnNames.get(i));
                log.info("当前字段数据结构：" + columnLevel.toString());
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
        return !results.contains(false);
    }

    @Override
    public void close() {
        synchronized (DataScanTransform.class) {
            if (RUNNING) {
                // last send metrics
                sendMetric();
                RUNNING = false;
                // close redis
                RedisClient.getInstance(
                                entity.getRedisHost(),
                                entity.getRedisPort(),
                                entity.getRedisPassword())
                        .close();
            }
        }
    }

    private void sendMetric() {
        try {
            RedisClient.getInstance(
                            entity.getRedisHost(), entity.getRedisPort(), entity.getRedisPassword())
                    .set(entity.getRedisKey(), JSONUtil.toJsonStr(metricsMap));
        } catch (Exception e) {
            Console.error("send data scan metric error：" + e.getMessage());
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
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
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema().copy();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
