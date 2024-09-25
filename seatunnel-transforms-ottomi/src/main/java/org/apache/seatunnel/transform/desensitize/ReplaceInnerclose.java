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
package org.apache.seatunnel.transform.desensitize;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import cn.hutool.core.util.StrUtil;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class ReplaceInnerclose implements ZetaUDF {

    @Override
    public String functionName() {
        return "REPLACE_INNERCLOSE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        // REPLACE_INNERCLOSE('{replace}','{arg2}',{from},{end})
        String replace = String.valueOf(args.get(0));
        String data = String.valueOf(args.get(1));
        int before = Integer.parseInt(args.get(2).toString());
        int after = Integer.parseInt(args.get(3).toString());
        return replaceFirstAndLast(data, before, after, replace);
    }

    public static String replaceFirstAndLast(String str, int n, int m, String replacement) {
        if (StrUtil.isEmpty(str) || n < 0 || m < 0) {
            return str;
        }
        int len = str.length();

        // 如果 n + m 超过了字符串长度，返回全是替换字符的字符串
        if (n + m >= len) {
            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < len; i++) {
                stringBuffer.append(replacement);
            }
            return stringBuffer.toString();
        }
        // 替换前 n 个字符
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < n; i++) {
            result.append(replacement);
        }
        // 中间部分
        result.append(str, n, len - m);
        // 替换后 m 个字符
        for (int i = 0; i < m; i++) {
            result.append(replacement);
        }
        return result.toString();
    }
}
