package org.apache.seatunnel.transform.datascan.metrics;

import org.apache.seatunnel.transform.datascan.Constants;

import cn.hutool.core.lang.Dict;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class DataScanMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private DataScanTableMetrics dataScanTableMetrics;
    private List<DataScanRowMetrics> dataScanRowMetrics;

    public void initMetrics(Dict ruleInfos) {
        // table
        DataScanTableMetrics dataScanTableMetrics = new DataScanTableMetrics();
        this.setDataScanTableMetrics(dataScanTableMetrics);
        // row
        List<DataScanRowMetrics> dataScanRowMetricsList =
                new ArrayList<>(ruleInfos.keySet().size());
        for (Map.Entry<String, Object> ruleEntry : ruleInfos.entrySet()) {
            String ruleId = ruleEntry.getKey();
            DataScanRowMetrics dataScanRowMetrics = new DataScanRowMetrics(ruleId);
            JSONObject value = (JSONObject) ruleEntry.getValue();
            String ruleName = value.getStr(Constants.RULE_NAME);
            String fieldLabel = value.getStr(Constants.FIELD_LABEL_ID);
            List<String> columns = value.getBeanList(Constants.COLUMN_NAMES, String.class);
            List<DataScanFieldMetrics> dataScanFieldMetricsList = new ArrayList<>(columns.size());
            for (String column : columns) {
                DataScanFieldMetrics dataScanFieldMetrics =
                        new DataScanFieldMetrics(ruleName, column, fieldLabel);
                dataScanFieldMetricsList.add(dataScanFieldMetrics);
            }
            dataScanRowMetrics.setDataScanFieldMetrics(dataScanFieldMetricsList);
            dataScanRowMetricsList.add(dataScanRowMetrics);
        }
        this.setDataScanRowMetrics(dataScanRowMetricsList);
    }

    public String getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        // set table metrics
        DataScanTableMetrics table = this.getDataScanTableMetrics();
        JSONObject tableMetrics = new JSONObject(table);
        metrics.put("table", tableMetrics);
        // set row metrics
        List<DataScanRowMetrics> scanRowMetricsList = this.getDataScanRowMetrics();
        Map<String, Object> row = new HashMap<>();
        for (DataScanRowMetrics scanRowMetrics : scanRowMetricsList) {
            List<DataScanFieldMetrics> dataScanFieldMetrics =
                    scanRowMetrics.getDataScanFieldMetrics();
            Map<String, Object> field = new HashMap<>();
            for (DataScanFieldMetrics dataScanFieldMetric : dataScanFieldMetrics) {
                JSONObject fieldMetrics = new JSONObject(dataScanFieldMetric);
                fieldMetrics.remove("fieldName");
                field.put(dataScanFieldMetric.getFieldName(), fieldMetrics);
            }
            row.put(scanRowMetrics.getRuleId(), field);
        }
        metrics.put("row", row);
        return JSONUtil.toJsonStr(metrics);
    }

    public String updateMetrics(JSONObject metrics) {
        JSONObject table = metrics.getJSONObject(Constants.TABLE_LEVEL);
        JSONObject row = metrics.getJSONObject(Constants.ROW_LEVEL);
        // update table
        DataScanTableMetrics dataScanTableMetrics = getDataScanTableMetrics();
        dataScanTableMetrics.getDealDataNum().getAndAdd(table.getLong(Constants.DEAL_DATA_NUM));
        dataScanTableMetrics.getNeatDataNum().getAndAdd(table.getLong(Constants.NEAT_DATA_NUM));
        dataScanTableMetrics.getDirtyDataNum().getAndAdd(table.getLong(Constants.DIRTY_DATA_NUM));
        // update row
        List<DataScanRowMetrics> scanRowMetricsList = getDataScanRowMetrics();
        for (String ruleId : row.keySet()) {
            JSONObject fieldObj = row.getJSONObject(ruleId);
            DataScanRowMetrics dataScanRowMetrics =
                    scanRowMetricsList.stream()
                            .filter(s -> s.getRuleId().equals(ruleId))
                            .findFirst()
                            .get();
            List<DataScanFieldMetrics> dataScanFieldMetrics =
                    dataScanRowMetrics.getDataScanFieldMetrics();
            for (DataScanFieldMetrics dataScanFieldMetric : dataScanFieldMetrics) {
                JSONObject field = fieldObj.getJSONObject(dataScanFieldMetric.getFieldName());
                dataScanFieldMetric
                        .getDealDataNum()
                        .getAndAdd(field.getLong(Constants.DEAL_DATA_NUM));
                dataScanFieldMetric
                        .getNeatDataNum()
                        .getAndAdd(field.getLong(Constants.NEAT_DATA_NUM));
                dataScanFieldMetric
                        .getDirtyDataNum()
                        .getAndAdd(field.getLong(Constants.DIRTY_DATA_NUM));
            }
        }
        return getMetrics();
    }
}
