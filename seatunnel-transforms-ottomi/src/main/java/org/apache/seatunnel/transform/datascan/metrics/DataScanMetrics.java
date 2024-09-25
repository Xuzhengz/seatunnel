package org.apache.seatunnel.transform.datascan.metrics;

import org.apache.seatunnel.transform.datascan.Constants;
import org.apache.seatunnel.transform.datascan.RedisClient;

import cn.hutool.core.lang.Dict;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @date 2024/9/25 下午8:01
 */
@Data
public class DataScanMetrics {

    private static volatile DataScanMetrics dataScanMetrics;

    private DataScanTableMetrics dataScanTableMetrics;
    private List<DataScanRowMetrics> dataScanRowMetrics;
    private boolean init = false;

    private DataScanMetrics() {}

    public static DataScanMetrics getInstance() {
        if (dataScanMetrics == null) {
            synchronized (RedisClient.class) {
                if (dataScanMetrics == null) {
                    dataScanMetrics = new DataScanMetrics();
                }
            }
        }
        return dataScanMetrics;
    }


    public void initMetrics(Dict ruleInfos) {
        if (!init) {
            DataScanMetrics scanMetrics = getInstance();
            // table
            DataScanTableMetrics dataScanTableMetrics = new DataScanTableMetrics();
            scanMetrics.setDataScanTableMetrics(dataScanTableMetrics);
            // row
            List<DataScanRowMetrics> dataScanRowMetricsList =
                    new ArrayList<>(ruleInfos.keySet().size());
            for (Map.Entry<String, Object> ruleEntry : ruleInfos.entrySet()) {
                String ruleId = ruleEntry.getKey();
                String fieldLabel = "";
                if (ruleId.contains(":")) {
                    String[] split = ruleId.split(":");
                    ruleId = split[0];
                    fieldLabel = split[1];
                }
                DataScanRowMetrics dataScanRowMetrics = new DataScanRowMetrics(ruleId);
                JSONObject value = (JSONObject) ruleEntry.getValue();
                String ruleName = value.getStr(Constants.RULE_NAME);
                List<String> columns = value.getBeanList(Constants.COLUMN_NAMES, String.class);
                List<DataScanFieldMetrics> dataScanFieldMetricsList =
                        new ArrayList<>(columns.size());
                for (String column : columns) {
                    DataScanFieldMetrics dataScanFieldMetrics =
                            new DataScanFieldMetrics(ruleName, column, fieldLabel);
                    dataScanFieldMetricsList.add(dataScanFieldMetrics);
                }
                dataScanRowMetrics.setDataScanFieldMetrics(dataScanFieldMetricsList);
                dataScanRowMetricsList.add(dataScanRowMetrics);
            }
            scanMetrics.setDataScanRowMetrics(dataScanRowMetricsList);
            init = true;
        }
    }

    public String getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        DataScanMetrics scanMetrics = getInstance();
        // set table metrics
        DataScanTableMetrics table = scanMetrics.getDataScanTableMetrics();
        JSONObject tableMetrics = new JSONObject(table);
        metrics.put("table", tableMetrics);
        // set row metrics
        List<DataScanRowMetrics> scanRowMetricsList = scanMetrics.getDataScanRowMetrics();
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
}
