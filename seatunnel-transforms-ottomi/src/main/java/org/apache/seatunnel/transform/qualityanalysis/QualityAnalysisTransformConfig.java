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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class QualityAnalysisTransformConfig implements Serializable {

    public static final Option<String> RULE_INFO =
            Options.key("ruleInfo")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("rule and column info.");
    public static final Option<Boolean> OPEN_TRUE_OR_FALSE_OUTPUT =
            Options.key("open")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("open quality result branch stream output.");
    public static final Option<Long> DIRTY_DATA_LIMIT =
            Options.key("dirtyDataLimit")
                    .longType()
                    .defaultValue(0l)
                    .withDescription("dirty data limit,if exceed,will send notice to platform.");
    public static final Option<String> MODEL_ID =
            Options.key("modelId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("platform model id");
    public static final Option<String> DATASOURCE_ID =
            Options.key("dsId").stringType().noDefaultValue().withDescription("platform dsId id");
    public static final Option<String> DATASOURCE_NAME =
            Options.key("dsName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("platform dsId name");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table or topic or nosql...");
    public static final Option<String> RESOURCE_COMMENT =
            Options.key("resourceComment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table or topic or nosql comment...");
    public static final Option<String> METRIC_API =
            Options.key("metricApi")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("metric api,collect quality result to analysis");
    public static final Option<String> JOB_MODE =
            Options.key("jobMode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("job mode,eg: batch or streaming");
    public static final Option<String> DEPT_ID =
            Options.key("deptId").stringType().noDefaultValue().withDescription("dept id");
    public static final Option<String> DEPT_NAME =
            Options.key("deptName").stringType().noDefaultValue().withDescription("dept name");
    public static final Option<String> TENANT_ID =
            Options.key("tenantId").stringType().noDefaultValue().withDescription("tenant id");
    public static final Option<String> CREATE_BY =
            Options.key("createBy").stringType().noDefaultValue().withDescription("create id");
}
