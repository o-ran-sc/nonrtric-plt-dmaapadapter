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

import com.google.common.base.Strings;

import java.time.Duration;

import lombok.Getter;

import org.immutables.gson.Gson;
import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.repository.filters.Filter;
import org.oran.dmaapadapter.repository.filters.PmReportFilter;
import org.oran.dmaapadapter.repository.filters.RegexpFilter;

public class Job {

    @Gson.TypeAdapters
    public static class Parameters {
        @Getter
        private String filter;
        @Getter
        private BufferTimeout bufferTimeout;

        private Integer maxConcurrency;

        @Getter
        private PmReportFilter.FilterData pmFilter;

        public Parameters() {}

        public Parameters(String filter, BufferTimeout bufferTimeout, Integer maxConcurrency,
                PmReportFilter.FilterData pmFilter) {
            this.filter = filter;
            this.bufferTimeout = bufferTimeout;
            this.maxConcurrency = maxConcurrency;
            this.pmFilter = pmFilter;
        }

        public int getMaxConcurrency() {
            return maxConcurrency == null || maxConcurrency == 0 ? 1 : maxConcurrency;
        }
    }

    @Gson.TypeAdapters
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
        if (parameters != null && !Strings.isNullOrEmpty(parameters.filter)) {
            filter = new RegexpFilter(parameters.filter);
        } else if (parameters != null && parameters.pmFilter != null) {
            filter = new PmReportFilter(parameters.pmFilter);
        } else {
            filter = null;
        }
        this.consumerRestClient = consumerRestClient;

    }

    public String filter(String data) {
        if (filter == null) {
            return data;
        }
        return filter.filter(data);
    }

    public boolean isBuffered() {
        return parameters != null && parameters.bufferTimeout != null && parameters.bufferTimeout.maxSize > 0
                && parameters.bufferTimeout.maxTimeMiliseconds > 0;
    }

}
