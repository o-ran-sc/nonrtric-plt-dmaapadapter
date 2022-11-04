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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.datastore.DataStore;
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
    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();
    private final DataStore dataStore;

    public KafkaTopicListener(ApplicationConfig applConfig, InfoType type) {
        this.applicationConfig = applConfig;
        this.type = type;
        this.dataStore = DataStore.create(applConfig);
    }

    @Override
    public Flux<DataFromTopic> getFlux() {
        if (this.dataFromTopic == null) {
            this.dataFromTopic = startReceiveFromTopic(this.type.getKafkaClientId(this.applicationConfig));
        }
        return this.dataFromTopic;
    }

    private Flux<DataFromTopic> startReceiveFromTopic(String clientId) {
        logger.debug("Listening to kafka topic: {} type :{}", this.type.getKafkaInputTopic(), type.getId());

        return KafkaReceiver.create(kafkaInputProperties(clientId)) //
                .receiveAutoAck() //
                .concatMap(consumerRecord -> consumerRecord) //
                .doOnNext(input -> logger.trace("Received from kafka topic: {}", this.type.getKafkaInputTopic())) //
                .doOnError(t -> logger.error("Received error: {}", t.getMessage())) //
                .onErrorResume(t -> Mono.empty()) //
                .doFinally(
                        sig -> logger.error("KafkaTopicListener stopped, type: {}, reason: {}", this.type.getId(), sig)) //
                .filter(t -> t.value().length > 0 || t.key().length > 0) //
                .map(input -> new DataFromTopic(input.key(), input.value(), DataFromTopic.findZipped(input.headers()))) //
                .flatMap(data -> getDataFromFileIfNewPmFileEvent(data, type, dataStore), 100) //
                .publish() //
                .autoConnect(1);
    }

    private ReceiverOptions<byte[], byte[]> kafkaInputProperties(String clientId) {
        Map<String, Object> consumerProps = new HashMap<>();
        if (this.applicationConfig.getKafkaBootStrapServers().isEmpty()) {
            logger.error("No kafka boostrap server is setup");
        }
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationConfig.getKafkaBootStrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.type.getKafkaGroupId());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        return ReceiverOptions.<byte[], byte[]>create(consumerProps)
                .subscription(Collections.singleton(this.type.getKafkaInputTopic()));
    }

    public static Mono<DataFromTopic> getDataFromFileIfNewPmFileEvent(DataFromTopic data, InfoType type,
            DataStore fileStore) {
        if (type.getDataType() != InfoType.DataType.PM_DATA) {
            return Mono.just(data);
        }

        try {
            NewFileEvent ev = gson.fromJson(data.valueAsString(), NewFileEvent.class);

            if (ev.getFilename() == null) {
                logger.warn("Ignoring received message: {}", data);
                return Mono.empty();
            }
            logger.trace("Reading PM measurements, type: {}, inputTopic: {}", type.getId(), type.getKafkaInputTopic());
            return fileStore.readObject(DataStore.Bucket.FILES, ev.getFilename()) //
                    .map(bytes -> unzip(bytes, ev.getFilename())) //
                    .map(bytes -> new DataFromTopic(data.key, bytes, false));

        } catch (Exception e) {
            return Mono.just(data);
        }
    }

    public static byte[] unzip(byte[] bytes) throws IOException {
        try (final GZIPInputStream gzipInput = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            return gzipInput.readAllBytes();
        }
    }

    private static byte[] unzip(byte[] bytes, String fileName) {
        try {
            return fileName.endsWith(".gz") ? unzip(bytes) : bytes;
        } catch (IOException e) {
            logger.error("Error while decompression, file: {}, reason: {}", fileName, e.getMessage());
            return new byte[0];
        }

    }

}
