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
import org.springframework.http.MediaType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

/**
 * The class fetches incoming requests from DMAAP and sends them further to the
 * consumers that has a job for this InformationType.
 */
public class DmaapTopicConsumer {
    private static final Duration TIME_BETWEEN_DMAAP_RETRIES = Duration.ofSeconds(10);
    private static final Logger logger = LoggerFactory.getLogger(DmaapTopicConsumer.class);

    private final AsyncRestClient dmaapRestClient;
    protected final ApplicationConfig applicationConfig;
    protected final InfoType type;
    protected final Jobs jobs;
    private final com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();

    public DmaapTopicConsumer(ApplicationConfig applicationConfig, InfoType type, Jobs jobs) {
        AsyncRestClientFactory restclientFactory = new AsyncRestClientFactory(applicationConfig.getWebClientConfig());
        this.dmaapRestClient = restclientFactory.createRestClientNoHttpProxy("");
        this.applicationConfig = applicationConfig;
        this.type = type;
        this.jobs = jobs;
    }

    public void start() {
        Flux.range(0, Integer.MAX_VALUE) //
                .flatMap(notUsed -> getFromMessageRouter(getDmaapUrl()), 1) //
                .flatMap(this::pushDataToConsumers) //
                .subscribe(//
                        null, //
                        throwable -> logger.error("DmaapMessageConsumer error: {}", throwable.getMessage()), //
                        this::onComplete); //
    }

    private void onComplete() {
        logger.warn("DmaapMessageConsumer completed {}", type.getId());
        start();
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
                .flatMapMany(body -> toMessages(body)) //
                .doOnNext(message -> logger.debug("Message from DMAAP topic: {} : {}", topicUrl, message)) //
                .onErrorResume(this::handleDmaapErrorResponse); //
    }

    private Flux<String> toMessages(String body) {
        Collection<String> messages = gson.fromJson(body, LinkedList.class);
        return Flux.fromIterable(messages);
    }

    private Mono<String> handleConsumerErrorResponse(Throwable t) {
        logger.warn("error from CONSUMER {}", t.getMessage());
        return Mono.empty();
    }

    protected Flux<String> pushDataToConsumers(String input) {
        logger.debug("Received data {}", input);
        final int CONCURRENCY = 50;

        // Distibute the body to all jobs for this type
        return Flux.fromIterable(this.jobs.getJobsForType(this.type)) //
                .map(job -> Tuples.of(job, job.filter(input))) //
                .filter(t -> !t.getT2().isEmpty()) //
                .doOnNext(touple -> logger.debug("Sending to consumer {}", touple.getT1().getCallbackUrl())) //
                .flatMap(touple -> touple.getT1().getConsumerRestClient().post("", touple.getT2(),
                        MediaType.APPLICATION_JSON), CONCURRENCY) //
                .onErrorResume(this::handleConsumerErrorResponse);
    }

}
