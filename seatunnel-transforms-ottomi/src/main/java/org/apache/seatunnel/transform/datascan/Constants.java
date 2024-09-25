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

/**
 * @author 徐正洲
 * @date 2024/7/22 17:23
 */
public class Constants {
    // 总质检量
    public static final String DEAL_DATA_NUM = "dealDataNum";
    // 干净数据量
    public static final String NEAT_DATA_NUM = "neatDataNum";
    // 脏数据量
    public static final String DIRTY_DATA_NUM = "dirtyDataNum";
    // 规则名称
    public static final String RULE_NAME = "ruleName";
    // 规则类型
    public static final String RULE_TYPE = "ruleType";
    // 规则code
    public static final String RULE_CODE = "ruleCode";
    // 字段标签Id
    public static final String FIELD_LABEL_ID = "fieldLabelId";
    // 质检字段下标
    public static final String COLUMNS = "columns";
    // 质检字段名称
    public static final String COLUMN_NAMES = "columnNames";
    // 表级质检
    public static final String TABLE_LEVEL = "table";
    // 行级质检
    public static final String ROW_LEVEL = "row";
}
