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

package org.apache.seatunnel.connectors.seatunnel.eventstore.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

@Data
public class EventStoreConfig implements Serializable {
    private String bootstrapServers;
    private String topic;
    private Properties eventStoreConfig;
    private String consumerGroup;
    private String format;
    private String fieldDelimiter;
    private StartMode startMode;
    private Long startModeTimestamp;
    private MessageFormatErrorHandleWay messageFormatErrorHandleWay;

    public static final String CONNECTOR_IDENTITY = "EventStore";
    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final Option<Map<String, String>> EVENTSTORE_CONFIG =
            Options.key("config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "In addition to the above parameters that must be specified by the Kafka producer or consumer client, "
                                    + "the user can also specify multiple non-mandatory parameters for the producer or consumer client, "
                                    + "covering all the producer parameters specified in the official Kafka document.");
    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kafka topic name. If there are multiple topics, use , to split, for example: \"tpc1,tpc2\".");
    public static final Option<String> BOOTSTRAP_SERVERS =
            Options.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka cluster address, separated by \",\".");
    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer.group")
                    .stringType()
                    .defaultValue("ottomi")
                    .withDescription(
                            "Kafka consumer group id, used to distinguish different consumer groups.");
    public static final Option<EventStoreConfig> SCHEMA =
            Options.key("schema")
                    .objectType(EventStoreConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "The structure of the data, including field names and field types.");
    public static final Option<MessageFormat> FORMAT =
            Options.key("format")
                    .enumType(MessageFormat.class)
                    .defaultValue(MessageFormat.JSON)
                    .withDescription(
                            "Data format. The default format is json. Optional text format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field_delimiter\" option.");
    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription("Customize the field delimiter for data format.");
    public static final Option<StartMode> START_MODE =
            Options.key("start_mode")
                    .objectType(StartMode.class)
                    .defaultValue(StartMode.GROUP_OFFSETS)
                    .withDescription(
                            "The initial consumption pattern of consumers,there are several types:\n"
                                    + "[earliest],[group_offsets],[latest],[specific_offsets],[timestamp]");
    public static final Option<Long> START_MODE_TIMESTAMP =
            Options.key("start_mode.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The time required for consumption mode to be timestamp.");
    // eventstore no client support get all partitions current offset
    //    public static final Option<Map<String, Long>> START_MODE_OFFSETS =
    //            Options.key("start_mode.offsets")
    //                    .type(new TypeReference<Map<String, Long>>() {
    //                    })
    //                    .noDefaultValue()
    //                    .withDescription(
    //                            "The offset required for consumption mode to be
    // specific_offsets.");
    public static final Option<MessageFormatErrorHandleWay> MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION =
            Options.key("format_error_handle_way")
                    .enumType(MessageFormatErrorHandleWay.class)
                    .defaultValue(MessageFormatErrorHandleWay.FAIL)
                    .withDescription(
                            "The processing method of data format error. The default value is fail, and the optional value is (fail, skip). "
                                    + "When fail is selected, data format error will block and an exception will be thrown. "
                                    + "When skip is selected, data format error will skip this line data.");

    public EventStoreConfig(Config config) {
        this.bootstrapServers = config.getString(BOOTSTRAP_SERVERS.key());
        this.topic = config.getString(TOPIC.key());
        if (config.hasPath(EVENTSTORE_CONFIG.key())) {
            Map<String, String> configMap =
                    (Map<String, String>) config.getAnyRef(EVENTSTORE_CONFIG.key());
            configMap.forEach((key, value) -> this.eventStoreConfig.put(key, value));
        } else {
            this.eventStoreConfig = new Properties();
        }
        if (config.hasPath(CONSUMER_GROUP.key())) {
            this.consumerGroup = config.getString(CONSUMER_GROUP.key());
        }
        if (config.hasPath(FORMAT.key())) {
            this.format = config.getString(FORMAT.key());
        }
        if (config.hasPath(FIELD_DELIMITER.key())) {
            this.fieldDelimiter = config.getString(FIELD_DELIMITER.key());
        }
        if (config.hasPath(START_MODE.key())) {
            this.startMode = config.getEnum(StartMode.class, START_MODE.key());
        }
        if (config.hasPath(START_MODE_TIMESTAMP.key())) {
            this.startModeTimestamp = config.getLong(START_MODE_TIMESTAMP.key());
        }
        if (config.hasPath(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION.key())) {
            this.messageFormatErrorHandleWay =
                    config.getEnum(
                            MessageFormatErrorHandleWay.class,
                            MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION.key());
        }
    }
}
