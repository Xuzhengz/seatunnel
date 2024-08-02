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
package org.apache.seatunnel.transform.entityextraction;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONObject;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class EntityExtractionConvertor implements ZetaUDF {

    private static final int API_OFFSET = 1;
    private static final int KEYWORDS_OFFSET = 2;
    private static final String NULL = "{}";

    @Override
    public String functionName() {
        return "Entity_Extraction";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        StringBuffer content = new StringBuffer();
        int size = args.size();
        Object api = args.get(size - API_OFFSET);
        Object keywords = args.get(size - KEYWORDS_OFFSET);
        for (int i = 0; i < args.size() - KEYWORDS_OFFSET; i++) {
            content.append(args.get(i));
        }
        JSONObject param = new JSONObject();
        param.set("content", content);
        param.set("schema", String.valueOf(keywords).split(StrUtil.COMMA));
        String result = HttpUtil.createPost((String) api).body(param.toString()).execute().body();
        if (NULL.equals(result)) {
            result = null;
        }
        return result;
    }
}
