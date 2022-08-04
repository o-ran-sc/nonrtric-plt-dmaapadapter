/*-
 * ========================LICENSE_START=================================
 * O-RAN-SC
 * %%
 * Copyright (C) 2021 Nordix Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ========================LICENSE_END===================================
 */

package org.oran.dmaapadapter.filter;

import com.jayway.jsonpath.JsonPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsonPathFilter implements Filter {

    private String expression;
    private static final Logger logger = LoggerFactory.getLogger(JsonPathFilter.class);
    com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();

    public JsonPathFilter(String exp) {
        try {
            expression = exp;
        } catch (Exception e) {
            logger.warn("Could not parse Json Path expression: {}, reason: {}", exp, e.getMessage());
        }
    }

    @Override
    public String filter(String jsonString) {
        try {
            Object o = JsonPath.parse(jsonString).read(this.expression, Object.class);
            return o == null ? "" : gson.toJson(o);
        } catch (Exception e) {
            return "";
        }

    }
}
