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

package org.apache.seatunnel.connectors.seatunnel.eventstore.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.eventstore.config.EventStoreConfig;
import org.apache.seatunnel.connectors.seatunnel.eventstore.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.eventstore.contants.Constants;
import org.apache.seatunnel.connectors.seatunnel.eventstore.exception.EventStoreConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.eventstore.exception.EventStoreConnectorException;
import org.apache.seatunnel.connectors.seatunnel.eventstore.split.EventStoreSplit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class EventStoreSourceReader<T> implements SourceReader<T, EventStoreSplit> {

    protected final Context context;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final EventStoreConfig config;
    private boolean running = true;
    private KafkaConsumer<String, String> consumer;
    private Duration pollWaitTime = Duration.ofMillis(200);

    public EventStoreSourceReader(
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            Context context,
            EventStoreConfig config) {
        this.context = context;
        this.deserializationSchema = deserializationSchema;
        this.config = config;
    }

    @Override
    public void open() {
        initConsumer();
    }

    @Override
    public void close() {
        try {
            this.running = false;
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            throw new EventStoreConnectorException(
                    EventStoreConnectorErrorCode.CONSUMER_CLOSE_FAILED, e);
        }
    }

    @Override
    public void pollNext(Collector output) {
        String value;
        while (running) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(pollWaitTime);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                value = consumerRecord.value();
                try {
                    deserializationSchema.deserialize(
                            value.getBytes(StandardCharsets.UTF_8), output);
                } catch (Exception e) {
                    log.error("Deserialize message failed, skip this message, message: {}", value);
                    //                    if (MessageFormatErrorHandleWay.SKIP.equals(
                    //                            config.getMessageFormatErrorHandleWay())) {
                    //                        log.error(
                    //                                "Deserialize message failed, skip this
                    // message, message: {}",
                    //                                value);
                    //                    } else {
                    //                        throw new EventStoreConnectorException(
                    //
                    // EventStoreConnectorErrorCode.CONSUME_DATA_FAILED, e);
                    //                    }
                }
                consumer.commitSync();
            }
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                running = false;
            }
        }
    }

    @Override
    public List<EventStoreSplit> snapshotState(long checkpointId) {
        return new ArrayList<>();
    }

    @Override
    public void addSplits(List splits) {
        // do nothing
    }

    @Override
    public void handleNoMoreSplits() {
        // do nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }

    private void initConsumer() {
        // init config
        Properties properties = config.getEventStoreConfig();
        System.setProperty("java.security.krb5.conf", properties.getProperty(Constants.KRB5_PATH));
        System.setProperty(
                "java.security.auth.login.config", properties.getProperty(Constants.JAAS_PATH));
        System.setProperty(
                "zookeeper.server.principal",
                properties.getProperty(Constants.ZOOKEEPER_PRINCIPAL));
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("group.id", config.getConsumerGroup());
        props.put("enable.auto.commit", false);
        setStartMode(properties);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("security.protocol", "SASL_PLAINTEXT"); // security.protocol
        props.put("sasl.mechanism", "GSSAPI"); // sasl.mechanism
        props.put("sasl.kerberos.service.name", "kafka"); // sasl.kerberos.service.name
        props.put(
                "sasl.kerberos.service.principal.instance",
                properties.getProperty(Constants.KAFKA_PRINCIPAL));
        log.info("配置信息设置成功!");
        // create consumer
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(config.getTopic()));
    }

    private void setStartMode(Properties properties) {
        StartMode startMode = this.config.getStartMode();
        switch (startMode) {
            case LATEST:
                properties.put("auto.offset.reset", "latest");
                break;
            case EARLIEST:
                properties.put("auto.offset.reset", "earliest");
            case TIMESTAMP:
                Set<TopicPartition> assignment = consumer.assignment();
                while (assignment.isEmpty()) {
                    consumer.poll(1000L);
                    assignment = consumer.assignment();
                }
                HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
                for (TopicPartition topicPartition : assignment) {
                    topicPartitionLongHashMap.put(
                            topicPartition, this.config.getStartModeTimestamp());
                }
                Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap =
                        consumer.offsetsForTimes(topicPartitionLongHashMap);
                for (TopicPartition topicPartition : assignment) {
                    OffsetAndTimestamp offsetAndTimestamp =
                            topicPartitionOffsetAndTimestampMap.get(topicPartition);
                    if (offsetAndTimestamp != null) {
                        consumer.seek(topicPartition, offsetAndTimestamp.offset());
                    }
                }
            default:
                break;
        }
    }
}
