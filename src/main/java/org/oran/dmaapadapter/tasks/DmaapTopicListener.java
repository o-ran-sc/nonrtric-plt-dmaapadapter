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

package org.oran.dmaapadapter.tasks;

import java.time.Duration;

import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.datastore.DataStore;
import org.oran.dmaapadapter.repository.InfoType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The class fetches incoming requests from DMAAP and sends them further to the
 * consumers that has a job for this InformationType.
 */
public class DmaapTopicListener implements TopicListener {
    private static final Duration TIME_BETWEEN_DMAAP_RETRIES = Duration.ofSeconds(3);
    private static final Logger logger = LoggerFactory.getLogger(DmaapTopicListener.class);

    private final AsyncRestClient dmaapRestClient;
    private final ApplicationConfig applicationConfig;
    private final InfoType type;
    private final com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();
    private Flux<DataFromTopic> dataFromDmaap;
    private final DataStore dataStore;

    public DmaapTopicListener(ApplicationConfig applicationConfig, InfoType type, SecurityContext securityContext) {
        AsyncRestClientFactory restclientFactory =
                new AsyncRestClientFactory(applicationConfig.getWebClientConfig(), securityContext);
        this.dmaapRestClient = restclientFactory.createRestClientNoHttpProxy("");
        this.applicationConfig = applicationConfig;
        this.type = type;
        this.dataStore = DataStore.create(applicationConfig);
    }

    @Override
    public Flux<DataFromTopic> getFlux() {
        if (this.dataFromDmaap == null) {
            this.dataFromDmaap = startFetchFromDmaap();
        }
        return this.dataFromDmaap;
    }

    private Flux<DataFromTopic> startFetchFromDmaap() {
        return Flux.range(0, Integer.MAX_VALUE) //
                .flatMap(notUsed -> getFromMessageRouter(getDmaapUrl()), 1) //
                .doOnNext(input -> logger.debug("Received from DMaap: {} :{}", this.type.getDmaapTopicUrl(), input)) //
                .doOnError(t -> logger.error("DmaapTopicListener error: {}", t.getMessage())) //
                .doFinally(sig -> logger.error("DmaapTopicListener stopped, reason: {}", sig)) //
                .map(input -> new DataFromTopic(this.type.getId(), null, null, input.getBytes()))
                .flatMap(data -> KafkaTopicListener.getDataFromFileIfNewPmFileEvent(data, type, dataStore), 100)
                .publish() //
                .autoConnect();

    }

    private String getDmaapUrl() {
        return this.applicationConfig.getDmaapBaseUrl() + type.getDmaapTopicUrl();
    }

    private Mono<String> handleDmaapErrorResponse(Throwable t) {
        logger.debug("error from DMAAP {} {}", t.getMessage(), type.getDmaapTopicUrl());
        return Mono.delay(TIME_BETWEEN_DMAAP_RETRIES) //
                .flatMap(notUsed -> Mono.empty());
    }

    private Flux<String> getFromMessageRouter(String topicUrl) {
        logger.trace("getFromMessageRouter {}", topicUrl);
        return dmaapRestClient.get(topicUrl) //
                .filter(body -> body.length() > 3) // DMAAP will return "[]" sometimes. That is thrown away.
                .flatMapMany(this::splitJsonArray) //
                .doOnNext(message -> logger.debug("Message from DMaaP topic: {} : {}", topicUrl, message)) //
                .onErrorResume(this::handleDmaapErrorResponse); //
    }

    private Flux<String> splitJsonArray(String body) {
        String[] messages = gson.fromJson(body, String[].class);
        return Flux.fromArray(messages);
    }

}
