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
package org.apache.seatunnel.transform.vector;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONObject;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class VectorConvertor implements ZetaUDF {

    @Override
    public String functionName() {
        return "VECTOR";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return ArrayType.DOUBLE_ARRAY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        Object data = args.get(0);
        Object api = args.get(1);
        JSONObject param = new JSONObject();
        param.set("content", String.valueOf(data));
        String result = HttpUtil.createPost((String) api).body(param.toString()).execute().body();
        result = result.replace("[", StrUtil.EMPTY).replace("]", StrUtil.EMPTY);
        String[] fields = result.split(StrUtil.COMMA);
        Double[] vectors = new Double[fields.length];
        for (int i = 0; i < fields.length; i++) {
            vectors[i] = Double.valueOf(fields[i]);
        }
        return vectors;
    }
}
