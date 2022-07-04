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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.InfoType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
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
    private Flux<Output> output;

    public KafkaTopicListener(ApplicationConfig applicationConfig, InfoType type) {
        this.applicationConfig = applicationConfig;
        this.type = type;
    }

    @Override
    public Flux<Output> getOutput() {
        if (this.output == null) {
            this.output = createOutput();
        }
        return this.output;
    }

    private Flux<Output> createOutput() {
        logger.debug("Listening to kafka topic: {} type :{}", this.type.getKafkaInputTopic(), type.getId());
        return KafkaReceiver.create(kafkaInputProperties()) //
                .receive() //
                .doOnNext(input -> logger.debug("Received from kafka topic: {} :{}", this.type.getKafkaInputTopic(),
                        input.value())) //
                .doOnError(t -> logger.error("KafkaTopicReceiver error: {}", t.getMessage())) //
                .doFinally(sig -> logger.error("KafkaTopicReceiver stopped, reason: {}", sig)) //
                .publish() //
                .autoConnect() //
                .map(input -> new Output(input.key(), input.value())); //
    }

    private ReceiverOptions<String, String> kafkaInputProperties() {
        Map<String, Object> consumerProps = new HashMap<>();
        if (this.applicationConfig.getKafkaBootStrapServers().isEmpty()) {
            logger.error("No kafka boostrap server is setup");
        }
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationConfig.getKafkaBootStrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "osc-dmaap-adapter");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(this.type.getKafkaInputTopic()));
    }

}
