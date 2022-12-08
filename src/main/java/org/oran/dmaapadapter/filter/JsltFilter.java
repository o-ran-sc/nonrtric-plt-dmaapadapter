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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;

import org.oran.dmaapadapter.tasks.TopicListener.DataFromTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsltFilter implements Filter {

    private Expression expression;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JsltFilter.class);

    public JsltFilter(String exp) {
        try {
            expression = Parser.compileString(exp);
        } catch (Exception e) {
            logger.warn("Could not parse JSLT expression: {}, reason: {}", exp, e.getMessage());
        }
    }

    @Override
    public FilteredData filter(DataFromTopic data) {
        if (expression == null) {
            return new FilteredData(data.infoTypeId, data.key, data.value);
        }
        try {
            JsonFactory factory = mapper.getFactory();
            JsonParser parser = factory.createParser(data.value);
            JsonNode actualObj = mapper.readTree(parser);

            JsonNode filteredNode = expression.apply(actualObj);
            if (filteredNode == NullNode.instance) {
                return FilteredData.empty();
            }
            return new FilteredData(data.infoTypeId, data.key, mapper.writeValueAsBytes(filteredNode));
        } catch (Exception e) {
            return FilteredData.empty();
        }
    }

}
