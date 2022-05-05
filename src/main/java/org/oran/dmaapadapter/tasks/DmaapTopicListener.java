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
import java.util.Collection;
import java.util.LinkedList;

import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.InfoType;
import org.oran.dmaapadapter.repository.Jobs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

/**
 * The class fetches incoming requests from DMAAP and sends them further to the
 * consumers that has a job for this InformationType.
 */
public class DmaapTopicListener implements TopicListener {
    private static final Duration TIME_BETWEEN_DMAAP_RETRIES = Duration.ofSeconds(3);
    private static final Logger logger = LoggerFactory.getLogger(DmaapTopicListener.class);

    private final AsyncRestClient dmaapRestClient;
    protected final ApplicationConfig applicationConfig;
    protected final InfoType type;
    protected final Jobs jobs;
    private final com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();
    private Many<String> output;
    private Disposable topicReceiverTask;

    public DmaapTopicListener(ApplicationConfig applicationConfig, InfoType type, Jobs jobs) {
        AsyncRestClientFactory restclientFactory = new AsyncRestClientFactory(applicationConfig.getWebClientConfig());
        this.dmaapRestClient = restclientFactory.createRestClientNoHttpProxy("");
        this.applicationConfig = applicationConfig;
        this.type = type;
        this.jobs = jobs;

    }

    @Override
    public Many<String> getOutput() {
        return this.output;
    }

    @Override
    public void start() {
        stop();

        final int CONSUMER_BACKPRESSURE_BUFFER_SIZE = 1024 * 10;
        this.output = Sinks.many().multicast().onBackpressureBuffer(CONSUMER_BACKPRESSURE_BUFFER_SIZE);

        topicReceiverTask = Flux.range(0, Integer.MAX_VALUE) //
                .flatMap(notUsed -> getFromMessageRouter(getDmaapUrl()), 1) //
                .doOnNext(this::onReceivedData) //
                .subscribe(//
                        null, //
                        throwable -> logger.error("DmaapMessageConsumer error: {}", throwable.getMessage()), //
                        this::onComplete); //
    }

    @Override
    public void stop() {
        if (topicReceiverTask != null) {
            topicReceiverTask.dispose();
            topicReceiverTask = null;
        }
    }

    private void onComplete() {
        logger.warn("DmaapMessageConsumer completed {}", type.getId());
        start();
    }

    private void onReceivedData(String input) {
        logger.debug("Received from DMAAP topic: {} :{}", this.type.getDmaapTopicUrl(), input);
        output.emitNext(input, Sinks.EmitFailureHandler.FAIL_FAST);
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
                .flatMapMany(this::splitArray) //
                .doOnNext(message -> logger.debug("Message from DMAAP topic: {} : {}", topicUrl, message)) //
                .onErrorResume(this::handleDmaapErrorResponse); //
    }

    private Flux<String> splitArray(String body) {
        Collection<String> messages = gson.fromJson(body, LinkedList.class);
        return Flux.fromIterable(messages);
    }

}
