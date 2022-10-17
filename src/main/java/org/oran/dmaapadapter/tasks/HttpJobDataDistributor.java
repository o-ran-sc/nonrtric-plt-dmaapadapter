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

package org.oran.dmaapadapter.tasks;

import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.repository.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The class streams data from a multi cast sink and sends the data to the Job
 * owner via REST calls.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public class HttpJobDataDistributor extends JobDataDistributor {
    private static final Logger logger = LoggerFactory.getLogger(HttpJobDataDistributor.class);

    public HttpJobDataDistributor(Job job, ApplicationConfig config, Flux<TopicListener.DataFromTopic> input) {
        super(job, config, input);
    }

    @Override
    protected Mono<String> sendToClient(Filter.FilteredData output) {
        Job job = this.getJob();
        logger.debug("Sending to consumer {} {} {}", job.getId(), job.getCallbackUrl(), output);
        MediaType contentType = job.isBuffered() || job.getType().isJson() ? MediaType.APPLICATION_JSON : null;
        return job.getConsumerRestClient().post("", output.getValueAString(), contentType);
    }

}
