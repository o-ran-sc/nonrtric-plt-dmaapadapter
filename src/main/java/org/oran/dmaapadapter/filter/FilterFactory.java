/*-
 * ========================LICENSE_START=================================
 * O-RAN-SC
 * %%
 * Copyright (C) 2022 Nordix Foundation
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

import com.google.gson.GsonBuilder;

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterFactory {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static com.google.gson.Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    private FilterFactory() {
    }

    public static Filter create(Object filter, Filter.Type type) {
        switch (type) {
            case PM_DATA:
                return new PmReportFilter(createPmFilterData(filter.toString()));
            case REGEXP:
                return new RegexpFilter(filter.toString());
            case JSLT:
                return new JsltFilter(filter.toString());
            case JSON_PATH:
                return new JsonPathFilter(filter.toString());
            case NONE:
                return null;
            default:
                logger.error("Not handeled filter type: {}", type);
                return null;
        }
    }

    private static PmReportFilter.FilterData createPmFilterData(Object filter) {
        String str = gson.toJson(filter);
        return gson.fromJson(str, PmReportFilter.FilterData.class);
    }

}
