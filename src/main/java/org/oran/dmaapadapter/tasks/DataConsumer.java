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

import lombok.Getter;

import org.oran.dmaapadapter.repository.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The class streams data from a multi cast sink and sends the data to the Job
 * owner via REST calls.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public abstract class DataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DataConsumer.class);
    @Getter
    private final Job job;
    private Disposable subscription;
    private final ErrorStats errorStats = new ErrorStats();

    private class ErrorStats {
        private int consumerFaultCounter = 0;
        private boolean irrecoverableError = false; // eg. overflow

        public void handleOkFromConsumer() {
            this.consumerFaultCounter = 0;
        }

        public void handleException(Throwable t) {
            if (t instanceof WebClientResponseException) {
                ++this.consumerFaultCounter;
            } else {
                irrecoverableError = true;
            }
        }

        public boolean isItHopeless() {
            final int STOP_AFTER_ERRORS = 5;
            return irrecoverableError || consumerFaultCounter > STOP_AFTER_ERRORS;
        }

        public void resetIrrecoverableErrors() {
            irrecoverableError = false;
        }
    }

    protected DataConsumer(Job job) {
        this.job = job;
    }

    public synchronized void start(Flux<TopicListener.Output> input) {
        stop();
        this.errorStats.resetIrrecoverableErrors();
        this.subscription = handleReceivedMessage(input, job) //
                .flatMap(this::sendToClient, job.getParameters().getMaxConcurrency()) //
                .onErrorResume(this::handleError) //
                .subscribe(this::handleConsumerSentOk, //
                        this::handleExceptionInStream, //
                        () -> logger.warn("HttpDataConsumer stopped jobId: {}", job.getId()));
    }

    private void handleExceptionInStream(Throwable t) {
        logger.warn("HttpDataConsumer exception: {}, jobId: {}", t.getMessage(), job.getId());
        stop();
    }

    protected abstract Mono<String> sendToClient(TopicListener.Output output);

    public synchronized void stop() {
        if (this.subscription != null) {
            this.subscription.dispose();
            this.subscription = null;
        }
    }

    public synchronized boolean isRunning() {
        return this.subscription != null;
    }

    private Flux<TopicListener.Output> handleReceivedMessage(Flux<TopicListener.Output> inputFlux, Job job) {
        Flux<TopicListener.Output> result =
                inputFlux.map(input -> new TopicListener.Output(input.key, job.filter(input.value))) //
                        .filter(t -> !t.value.isEmpty()); //

        if (job.isBuffered()) {
            result = result.map(input -> quoteNonJson(input.value, job)) //
                    .bufferTimeout( //
                            job.getParameters().getBufferTimeout().getMaxSize(), //
                            job.getParameters().getBufferTimeout().getMaxTime()) //
                    .map(buffered -> new TopicListener.Output("", buffered.toString()));
        }
        return result;
    }

    private String quoteNonJson(String str, Job job) {
        return job.getType().isJson() ? str : quote(str);
    }

    private String quote(String str) {
        final String q = "\"";
        return q + str.replace(q, "\\\"") + q;
    }

    private Mono<String> handleError(Throwable t) {
        logger.warn("exception: {} job: {}", t.getMessage(), job.getId());
        this.errorStats.handleException(t);
        if (this.errorStats.isItHopeless()) {
            return Mono.error(t);
        } else {
            return Mono.empty(); // Ignore
        }
    }

    private void handleConsumerSentOk(String data) {
        this.errorStats.handleOkFromConsumer();
    }

}
