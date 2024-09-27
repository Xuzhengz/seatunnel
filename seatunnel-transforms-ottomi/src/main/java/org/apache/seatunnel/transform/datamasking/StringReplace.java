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

import cn.hutool.core.lang.func.Func1;
import cn.hutool.core.util.StrUtil;
import com.google.auto.service.AutoService;

import java.util.List;
import java.util.regex.Matcher;

@AutoService(ZetaUDF.class)
public class StringReplace implements ZetaUDF {
    /** 特殊字符前遮盖，遮盖匹配到的字符 */
    private int index_all = 0;
    /** 特殊字符前遮盖，针对首次出现字符 */
    private int index_before = -1;
    /** 特殊字符后遮盖，针对首次出现字符 */
    private int index_after = 1;

    @Override
    public String functionName() {
        return "STRING_MASKING";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        // REPLACE('{replace}','{arg2}','{search}',-1)
        String replace = String.valueOf(args.get(0));
        String data = String.valueOf(args.get(1));
        String match = String.valueOf(args.get(2));
        int type = Integer.parseInt(args.get(3).toString());

        if (StrUtil.isNotEmpty(replace) && StrUtil.isNotEmpty(data) && StrUtil.isNotEmpty(match)) {
            if (index_all == type) {
                data = StrUtil.replace(data, match, (Func1<Matcher, String>) parameter -> replace);
                //                data = StrUtil.replace(data, 0, match, replace, false);
            } else if (index_before == type) {
                data = StrUtil.replace(data, 0, data.indexOf(match), replace);
            } else if (index_after == type) {
                data = StrUtil.replace(data, data.indexOf(match), data.length(), replace);
            }
        }
        return data;
    }
}
