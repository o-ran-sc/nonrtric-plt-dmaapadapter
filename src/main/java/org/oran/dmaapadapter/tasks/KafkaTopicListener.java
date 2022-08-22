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

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.InfoType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.nio.charset.Charset;
import java.nio.file.Files;

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
    private final String kafkaClientId;

    private static Gson gson = new GsonBuilder() //
            .disableHtmlEscaping() //
            .create(); //

    @ToString
    @Builder
    public static class NewFileEvent {
        @Getter
        private String filename;
    }

    public KafkaTopicListener(ApplicationConfig applicationConfig, InfoType type, String kafkaClientId) {
        this.applicationConfig = applicationConfig;
        this.type = type;
        this.kafkaClientId = kafkaClientId;
    }

    @Override
    public Flux<DataFromTopic> getFlux() {
        if (this.dataFromTopic == null) {
            this.dataFromTopic = startReceiveFromTopic(this.kafkaClientId);
        }
        return this.dataFromTopic;
    }

    private Flux<DataFromTopic> startReceiveFromTopic(String clientId) {
        logger.debug("Listening to kafka topic: {} type :{}", this.type.getKafkaInputTopic(), type.getId());
        return KafkaReceiver.create(kafkaInputProperties(clientId)) //
                .receive() //
                .doOnNext(input -> logger.debug("Received from kafka topic: {} :{}", this.type.getKafkaInputTopic(),
                        input.value())) //
                .doOnError(t -> logger.error("KafkaTopicReceiver error: {}", t.getMessage())) //
                .doFinally(sig -> logger.error("KafkaTopicReceiver stopped, reason: {}", sig)) //
                .filter(t -> !t.value().isEmpty() || !t.key().isEmpty()) //
                .map(input -> new DataFromTopic(input.key(), input.value())) //
                .map(this::getDataFromFileIfNewPmFileEvent) //
                .publish() //
                .autoConnect(1);
    }

    private DataFromTopic getDataFromFileIfNewPmFileEvent(DataFromTopic data) {

        if (!applicationConfig.getPmFilesPath().isEmpty()
                && this.type.getDataType() == InfoType.DataType.PM_DATA
                && data.value.length() < 1000) {
            try {
                NewFileEvent ev = gson.fromJson(data.value, NewFileEvent.class);
                Path path = Path.of(this.applicationConfig.getPmFilesPath(), ev.getFilename());
                String pmReportJson = Files.readString(path, Charset.defaultCharset());
                return new DataFromTopic(data.key, pmReportJson);
            } catch (Exception e) {
                return data;
            }
        } else {
            return data;
        }
    }

    private ReceiverOptions<String, String> kafkaInputProperties(String clientId) {
        Map<String, Object> consumerProps = new HashMap<>();
        if (this.applicationConfig.getKafkaBootStrapServers().isEmpty()) {
            logger.error("No kafka boostrap server is setup");
        }
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationConfig.getKafkaBootStrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "osc-dmaap-adapter-" + this.type.getId());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        return ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(this.type.getKafkaInputTopic()));
    }

}
