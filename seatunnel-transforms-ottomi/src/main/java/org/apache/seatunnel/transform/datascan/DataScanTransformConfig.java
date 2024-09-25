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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class DataScanTransformConfig implements Serializable {

    public static final Option<String> RULE_INFO =
            Options.key("rule_info")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("rule and column info.");

    public static final Option<String> REDIS_HOST =
            Options.key("redis_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis hostname or ip");

    public static final Option<Integer> REDIS_PORT =
            Options.key("redis_port").intType().noDefaultValue().withDescription("redis port");

    public static final Option<Integer> REDIS_DB_NUM =
            Options.key("redis_db_num")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Redis database index id, it is connected to db 0 by default");

    public static final Option<String> REDIS_USER =
            Options.key("redis_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "redis authentication user, you need it when you connect to an encrypted cluster");

    public static final Option<String> REDIS_PASSWORD =
            Options.key("redis_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "redis authentication password, you need it when you connect to an encrypted cluster");

    public static final Option<String> REDIS_KEY =
            Options.key("redis_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The value of key you want to write to redis.");
}
