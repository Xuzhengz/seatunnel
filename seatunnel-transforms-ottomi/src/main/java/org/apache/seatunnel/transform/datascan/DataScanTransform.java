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
import org.apache.seatunnel.transform.datascan.metrics.DataScanFieldMetrics;
import org.apache.seatunnel.transform.datascan.metrics.DataScanMetrics;
import org.apache.seatunnel.transform.datascan.metrics.DataScanRowMetrics;
import org.apache.seatunnel.transform.datascan.metrics.DataScanTableMetrics;

import cn.hutool.core.lang.Console;
import cn.hutool.core.lang.Dict;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.oceandatum.quality.common.rulebase.IRuleHandler;
import com.oceandatum.quality.common.rulebase.RuleFactory;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class DataScanTransform extends AbstractCatalogSupportTransform {

    public static final String PLUGIN_NAME = "DataScan";

    // 配置类
    private DataScanConfig dataScanConfig;
    // 指标类
    private DataScanMetrics scanMetrics;
    private final ReadonlyConfig readonlyConfig;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        // 初始化配置信息
        initConfig(readonlyConfig);
        // 初始化指标信息
        scanMetrics = new DataScanMetrics();
        scanMetrics.initMetrics(dataScanConfig.getRuleInfos());
    }

    public DataScanTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.readonlyConfig = config;
    }

    private void initConfig(ReadonlyConfig config) {
        try {
            dataScanConfig =
                    DataScanConfig.builder()
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

    private boolean runCheck(String rule, SeaTunnelRow inputRow) {
        RowKind rowKind = inputRow.getRowKind();
        List<Boolean> results = new ArrayList<>();
        JSONObject obj = (JSONObject) dataScanConfig.getRuleInfos().getObj(rule);
        String ruleType = obj.getStr(Constants.RULE_TYPE);
        String ruleCode = obj.getStr(Constants.RULE_CODE);
        List<String> columnNames = obj.getByPath(Constants.COLUMN_NAMES, List.class);
        // doCheck
        IRuleHandler ruleHandler = RuleFactory.getRule(ruleType, ruleCode);
        DataScanRowMetrics dataScanRowMetrics =
                scanMetrics.getDataScanRowMetrics().stream()
                        .filter(s -> s.getRuleId().equals(rule))
                        .findFirst()
                        .get();
        List<DataScanFieldMetrics> dataScanFieldMetrics =
                dataScanRowMetrics.getDataScanFieldMetrics();
        for (DataScanFieldMetrics dataScanFieldMetric : dataScanFieldMetrics) {
            String fieldName = dataScanFieldMetric.getFieldName();
            int index = columnNames.indexOf(fieldName);
            Object value = inputRow.getField(index);
            boolean result = ruleHandler.doCheck((String.valueOf(value)));
            results.add(result);
            // do insert type data
            if (RowKind.INSERT.equals(rowKind)) {
                dataScanFieldMetric.getDealDataNum().incrementAndGet();
                if (result) {
                    dataScanFieldMetric.getNeatDataNum().incrementAndGet();
                } else {
                    dataScanFieldMetric.getDirtyDataNum().incrementAndGet();
                }
            }
        }
        // simple rule check result
        return !results.contains(false);
    }

    @Override
    public void close() {
        synchronized (DataScanTransform.class) {
            // last send metrics
            Jedis jedis = new Jedis(dataScanConfig.getRedisHost(), dataScanConfig.getRedisPort());
            if (StrUtil.isNotEmpty(dataScanConfig.getRedisPassword())) {
                jedis.auth(dataScanConfig.getRedisPassword());
            }
            sendMetric(jedis);
        }
    }

    private void sendMetric(Jedis jedis) {
        try {
            boolean exists = jedis.exists(dataScanConfig.getRedisKey());
            if (!exists) {
                jedis.set(dataScanConfig.getRedisKey(), scanMetrics.getMetrics());
            } else {
                JSONObject metrics = new JSONObject(jedis.get(dataScanConfig.getRedisKey()));
                String newMetics = scanMetrics.updateMetrics(metrics);
                jedis.set(dataScanConfig.getRedisKey(), newMetics);
            }
        } catch (Exception e) {
            Console.error("send data scan metric error：" + e.getMessage());
        } finally {
            jedis.close();
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // check info
        Set<String> rules = dataScanConfig.getRuleInfos().keySet();
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
        DataScanTableMetrics dataScanTableMetrics = scanMetrics.getDataScanTableMetrics();
        dataScanTableMetrics.getDealDataNum().incrementAndGet();
        if (rowCheckResult) {
            dataScanTableMetrics.getNeatDataNum().incrementAndGet();
        } else {
            dataScanTableMetrics.getDirtyDataNum().incrementAndGet();
        }
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
