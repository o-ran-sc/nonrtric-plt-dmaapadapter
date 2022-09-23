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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.r1.ConsumerJobInfo;
import org.oran.dmaapadapter.r1.FileReadyJobData;
import org.oran.dmaapadapter.repository.InfoType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/**
 * The class streams incoming requests from a Kafka topic and sends them further
 * to a multi cast sink, which several other streams can connect to.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public class KafkaTopicListener implements TopicListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicListener.class);
    private final ApplicationConfig applicationConfig;
    private final InfoType type;
    private Flux<DataFromTopic> dataFromTopic;

    @ToString
    @Builder
    public static class NewFileEvent {
        @Getter
        private String filename;

        @Getter
        private String objectStoreBucket;
    }

    public KafkaTopicListener(ApplicationConfig applConfig, InfoType type, SecurityContext securityContext) {
        this.applicationConfig = applConfig;
        this.type = type;
        createInputDataJob(securityContext).subscribe();
    }

    @Override
    public Flux<DataFromTopic> getFlux() {
        if (this.dataFromTopic == null) {
            this.dataFromTopic = startReceiveFromTopic(this.type.getKafkaClientId(this.applicationConfig));
        }
        return this.dataFromTopic;
    }

    private Mono<String> createInputDataJob(SecurityContext securityContext) {
        if (type.getInputDataTypeId() == null) {
            return Mono.just("");
        }

        AsyncRestClientFactory restClientFactory =
                new AsyncRestClientFactory(applicationConfig.getWebClientConfig(), securityContext);
        AsyncRestClient restClient = restClientFactory.createRestClientNoHttpProxy("");

        com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();
        FileReadyJobData jobData = new FileReadyJobData(type.getKafkaInputTopic());
        ConsumerJobInfo info = new ConsumerJobInfo(type.getInputDataTypeId(), jobData, "DmaapAdapter", "", "");

        final String JOB_ID = "5b3f4db6-3d9e-11ed-b878-0242ac120002";
        String body = gson.toJson(info);

        return Mono.delay(Duration.ofSeconds(3)) // this is for the unit test to work
                .flatMap(x -> restClient.put(consumerJobUrl(JOB_ID), body))
                .doOnError(t -> logger.warn("Could not create job of type {}, reason: {}", type.getInputDataTypeId(),
                        t.getMessage()))
                .onErrorResume(t -> Mono.empty()) //
                .doOnNext(n -> logger.info("Created job: {}, type: {}", JOB_ID, type.getInputDataTypeId()));
    }

    private String consumerJobUrl(String jobId) {
        return applicationConfig.getIcsBaseUrl() + "/data-consumer/v1/info-jobs/" + jobId;

    }

    private Flux<DataFromTopic> startReceiveFromTopic(String clientId) {
        logger.debug("Listening to kafka topic: {} type :{}", this.type.getKafkaInputTopic(), type.getId());

        return KafkaReceiver.create(kafkaInputProperties(clientId)) //
                .receiveAutoAck() //
                .concatMap(consumerRecord -> consumerRecord) //
                .doOnNext(input -> logger.trace("Received from kafka topic: {} :{}", this.type.getKafkaInputTopic(),
                        input.value())) //
                .doOnError(t -> logger.error("KafkaTopicReceiver error: {}", t.getMessage())) //
                .doFinally(sig -> logger.error("KafkaTopicReceiver stopped, reason: {}", sig)) //
                .filter(t -> !t.value().isEmpty() || !t.key().isEmpty()) //
                .map(input -> new DataFromTopic(input.key(), input.value())) //
                .publish() //
                .autoConnect(1);
    }

    private ReceiverOptions<String, String> kafkaInputProperties(String clientId) {
        Map<String, Object> consumerProps = new HashMap<>();
        if (this.applicationConfig.getKafkaBootStrapServers().isEmpty()) {
            logger.error("No kafka boostrap server is setup");
        }
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationConfig.getKafkaBootStrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.type.getKafkaGroupId());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.applicationConfig.getKafkaMaxPollRecords());

        return ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(this.type.getKafkaInputTopic()));
    }

}
