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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.eventstore.config.EventStoreConfig;
import org.apache.seatunnel.connectors.seatunnel.eventstore.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.eventstore.exception.EventStoreConnectorException;
import org.apache.seatunnel.connectors.seatunnel.eventstore.split.EventStoreSplit;
import org.apache.seatunnel.connectors.seatunnel.eventstore.split.EventStoreSplitEnumeratorState;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class EventStoreSource
        implements SeaTunnelSource<SeaTunnelRow, EventStoreSplit, EventStoreSplitEnumeratorState>,
                SupportParallelism {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;
    private EventStoreConfig config;

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "EventStore";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        // check config info
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        config,
                        EventStoreConfig.BOOTSTRAP_SERVERS.key(),
                        EventStoreConfig.TOPIC.key(),
                        TableSchemaOptions.SCHEMA.key());
        if (!result.isSuccess()) {
            throw new EventStoreConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.config = new EventStoreConfig(config);
        setDeserialization(config);
    }

    @Override
    public SeaTunnelDataType getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public SourceReader<SeaTunnelRow, EventStoreSplit> createReader(
            SourceReader.Context readerContext) {
        return new EventStoreSourceReader<>(deserializationSchema, readerContext, config);
    }

    @Override
    public SourceSplitEnumerator<EventStoreSplit, EventStoreSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<EventStoreSplit> enumeratorContext) {
        return new EventStoreSplitEnumerator();
    }

    @Override
    public SourceSplitEnumerator<EventStoreSplit, EventStoreSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<EventStoreSplit> enumeratorContext,
            EventStoreSplitEnumeratorState checkpointState) {
        return new EventStoreSplitEnumerator();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {
        // TODO: format SPI
        // only support json deserializationSchema
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config);
        String format = this.config.getFormat().toUpperCase();
        if (MessageFormat.JSON.name().equals(format)) {
            this.deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
        } else {
            this.deserializationSchema =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                            .delimiter(this.config.getFieldDelimiter())
                            .build();
        }
    }
}
