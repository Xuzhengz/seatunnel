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
package org.apache.seatunnel.transform.groovy;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowContainerGenerator;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.execption.GroovyTransformErrorCode;

import org.codehaus.groovy.control.CompilationFailedException;

import cn.hutool.core.util.StrUtil;
import com.google.auto.service.AutoService;
import groovy.lang.GroovyClassLoader;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class GroovyTransform extends AbstractCatalogSupportTransform {

    public static String PLUGIN_NAME = "Groovy";

    public static final GroovyClassLoader GROOVY_CLASS_LOADER =
            new GroovyClassLoader(GroovyTransform.class.getClassLoader());
    private GroovyTransformConfig config;

    public OceanTransform oceanTransform;
    private SeaTunnelRowContainerGenerator rowContainerGenerator =
            SeaTunnelRowContainerGenerator.REUSE_ROW;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public void open() {
        initGroovyTransformer();
    }

    private void initGroovyTransformer() {
        String groovyRule = this.getGroovyRule(this.config.getCode(), null);
        Class groovyClass;
        try {
            groovyClass = GROOVY_CLASS_LOADER.parseClass(groovyRule);
        } catch (CompilationFailedException cfe) {
            throw new TransformException(
                    GroovyTransformErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, cfe.getMessage());
        }
        try {
            Object t = groovyClass.newInstance();
            if (!(t instanceof OceanTransform)) {
                throw new TransformException(
                        GroovyTransformErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION,
                        "编程错误 联系 ottomi");
            }
            this.oceanTransform = (OceanTransform) t;
        } catch (Throwable ex) {
            throw new TransformException(
                    GroovyTransformErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, ex.getMessage());
        }
    }

    private String getGroovyRule(String code, List<String> extraPackagesStrList) {
        StringBuffer sb = new StringBuffer();
        if (extraPackagesStrList != null) {
            for (String extraPackagesStr : extraPackagesStrList) {
                if (StrUtil.isNotEmpty(extraPackagesStr)) {
                    sb.append(extraPackagesStr);
                }
            }
        }
        sb.append("import org.apache.seatunnel.transform.groovy.OceanTransform;");
        sb.append("import java.util.*;");
        sb.append("public class CustomizedTransform implements OceanTransform").append("{");
        sb.append("public Object[] transformRow(Object[] data) {");
        sb.append(code);
        sb.append("}}");
        return sb.toString();
    }

    public GroovyTransform(GroovyTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] fields = inputRow.getFields();
        Object[] newFields = oceanTransform.transformRow(fields);
        SeaTunnelRow outputRow = rowContainerGenerator.apply(inputRow);
        for (int i = 0; i < newFields.length; i++) {
            outputRow.setField(i, newFields[i]);
        }
        return outputRow;
    }
}
