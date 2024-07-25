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
    private String modelName;
    private String resource;
    private String resourceComment;
    private String dsId;
    private String dsName;
    private String metricApi;
    private String warningApi;
    private String jobMode;
    private String deptId;
    private String deptName;
    private String tenantId;
    private String createBy;
    private boolean preview;
}
