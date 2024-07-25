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

/**
 * @author 徐正洲
 * @date 2024/7/22 17:23
 */
public class Constants {
    // 质检结果字段
    public static final String CHECK_RESULT_COLUMN = "check_result";
    // 总质检量
    public static final String DEAL_DATA_NUM = "dealDataNum";
    // 干净数据量
    public static final String NEAT_DATA_NUM = "neatDataNum";
    // 脏数据量
    public static final String DIRTY_DATA_NUM = "dirtyDataNum";
    // 数据源id
    public static final String DATASOURCE_ID = "dsId";
    // 数据源名称
    public static final String DATASOURCE_NAME = "dsName";
    // 资源名称
    public static final String RESOURCE = "resource";
    // 资源描述
    public static final String RESOURCE_COMMENT = "resourceComment";
    // 模型id
    public static final String MODEL_ID = "modelId";
    // 模型名称
    public static final String MODEL_NAME = "modelName";
    // 规则名称
    public static final String RULE_NAME = "ruleName";
    // 规则类型
    public static final String RULE_TYPE = "ruleType";
    // 规则code
    public static final String RULE_CODE = "ruleCode";
    // 质检字段下标
    public static final String COLUMNS = "columns";
    // 质检字段名称
    public static final String COLUMN_NAMES = "columnNames";
    // 表级质检
    public static final String TABLE_LEVEL = "table";
    // 行级质检
    public static final String ROW_LEVEL = "row";
    // 质检模式：离线/实时
    public static final String QUALITY_JOB_MODE = "jobMode";
    // 部门id
    public static final String DEPT_ID = "deptId";
    // 部门名称
    public static final String DEPT_NAME = "deptName";
    // 租户id
    public static final String TENANT_ID = "tenantId";
    // 创建人
    public static final String CREATE_BY = "createBy";
}
