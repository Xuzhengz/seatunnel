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
package org.apache.seatunnel.transform.datamasking;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import org.apache.commons.lang3.StringUtils;

import cn.hutool.core.util.StrUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class ReplaceOuterClose implements ZetaUDF {

    @Override
    public String functionName() {
        return "REPLACE_OUTERCLOSE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        // REPLACE_OUTERCLOSE('{replace}','{arg2}',{from},{end})
        String replace = String.valueOf(args.get(0));
        String data = String.valueOf(args.get(1));
        int before = Integer.parseInt(args.get(2).toString());
        int after = Integer.parseInt(args.get(3).toString());
        if (StrUtil.isNotEmpty(replace) && StringUtils.isNotEmpty(data)) {
            data = StrUtil.replace(data, before - 1, after, replace);
        }
        return data;
    }
}
