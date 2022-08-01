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

package org.oran.dmaapadapter.repository;

import com.google.gson.GsonBuilder;

import java.lang.invoke.MethodHandles;
import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.repository.filters.Filter;
import org.oran.dmaapadapter.repository.filters.JsltFilter;
import org.oran.dmaapadapter.repository.filters.JsonPathFilter;
import org.oran.dmaapadapter.repository.filters.PmReportFilter;
import org.oran.dmaapadapter.repository.filters.RegexpFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ToString
public class Job {

    private static com.google.gson.Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static class Parameters {
        public static final String REGEXP_TYPE = "regexp";
        public static final String PM_FILTER_TYPE = "pmdata";
        public static final String JSLT_FILTER_TYPE = "jslt";
        public static final String JSON_PATH_FILTER_TYPE = "json-path";

        @Setter
        private String filterType = REGEXP_TYPE;
        private Object filter;
        @Getter
        private BufferTimeout bufferTimeout;

        private Integer maxConcurrency;

        @Getter
        private String kafkaOutputTopic;

        public Parameters() {}

        public Parameters(Object filter, String filterType, BufferTimeout bufferTimeout, Integer maxConcurrency,
                String kafkaOutputTopic) {
            this.filter = filter;
            this.bufferTimeout = bufferTimeout;
            this.maxConcurrency = maxConcurrency;
            this.filterType = filterType;
            this.kafkaOutputTopic = kafkaOutputTopic;
        }

        public int getMaxConcurrency() {
            return maxConcurrency == null || maxConcurrency == 0 ? 1 : maxConcurrency;
        }

        public String getFilterAsString() {
            return this.filter.toString();
        }

        public PmReportFilter.FilterData getPmFilter() {
            String str = gson.toJson(this.filter);
            return gson.fromJson(str, PmReportFilter.FilterData.class);
        }

        public enum FilterType {
            REGEXP, JSLT, JSON_PATH, PM_DATA, NONE
        }

        public FilterType getFilterType() {
            if (filter == null || filterType == null) {
                return FilterType.NONE;
            } else if (filterType.equalsIgnoreCase(JSLT_FILTER_TYPE)) {
                return FilterType.JSLT;
            } else if (filterType.equalsIgnoreCase(JSON_PATH_FILTER_TYPE)) {
                return FilterType.JSON_PATH;
            } else if (filterType.equalsIgnoreCase(REGEXP_TYPE)) {
                return FilterType.REGEXP;
            } else if (filterType.equalsIgnoreCase(PM_FILTER_TYPE)) {
                return FilterType.PM_DATA;
            } else {
                logger.warn("Unsupported filter type: {}", this.filterType);
                return FilterType.NONE;
            }
        }
    }

    public static class BufferTimeout {
        public BufferTimeout(int maxSize, long maxTimeMiliseconds) {
            this.maxSize = maxSize;
            this.maxTimeMiliseconds = maxTimeMiliseconds;
        }

        public BufferTimeout() {}

        @Getter
        private int maxSize;

        private long maxTimeMiliseconds;

        public Duration getMaxTime() {
            return Duration.ofMillis(maxTimeMiliseconds);
        }
    }

    @Getter
    private final String id;

    @Getter
    private final String callbackUrl;

    @Getter
    private final InfoType type;

    @Getter
    private final String owner;

    @Getter
    private final Parameters parameters;

    @Getter
    private final String lastUpdated;

    private final Filter filter;

    @Getter
    private final AsyncRestClient consumerRestClient;

    public Job(String id, String callbackUrl, InfoType type, String owner, String lastUpdated, Parameters parameters,
            AsyncRestClient consumerRestClient) {
        this.id = id;
        this.callbackUrl = callbackUrl;
        this.type = type;
        this.owner = owner;
        this.lastUpdated = lastUpdated;
        this.parameters = parameters;
        filter = createFilter(parameters);
        this.consumerRestClient = consumerRestClient;
    }

    private static Filter createFilter(Parameters parameters) {

        if (parameters.filter == null) {
            return null;
        }

        switch (parameters.getFilterType()) {
            case PM_DATA:
                return new PmReportFilter(parameters.getPmFilter());
            case REGEXP:
                return new RegexpFilter(parameters.getFilterAsString());
            case JSLT:
                return new JsltFilter(parameters.getFilterAsString());
            case JSON_PATH:
                return new JsonPathFilter(parameters.getFilterAsString());
            case NONE:
                return null;
            default:
                logger.error("Not handeled filter type: {}", parameters.getFilterType());
                return null;
        }
    }

    public String filter(String data) {
        if (filter == null) {
            logger.debug("No filter used");
            return data;
        }
        return filter.filter(data);
    }

    public boolean isBuffered() {
        return parameters != null && parameters.bufferTimeout != null && parameters.bufferTimeout.maxSize > 0
                && parameters.bufferTimeout.maxTimeMiliseconds > 0;
    }

}
