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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

/**
 * The class streams data from a multi cast sink and sends the data to the Job
 * owner via REST calls.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public class KafkaDataConsumer extends DataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataConsumer.class);

    private KafkaSender<String, String> sender;
    private final ApplicationConfig appConfig;

    public KafkaDataConsumer(Job job, ApplicationConfig appConfig) {
        super(job);
        this.appConfig = appConfig;
    }

    @Override
    protected Mono<String> sendToClient(DataToConsumer data) {
        Job job = this.getJob();

        logger.debug("Sending data '{}' to Kafka topic: {}", data, this.getJob().getParameters().getKafkaOutputTopic());

        SenderRecord<String, String, Integer> senderRecord = senderRecord(data, job);

        return this.sender.send(Mono.just(senderRecord)) //
                .collectList() //
                .map(x -> data.value);
    }

    @Override
    public synchronized void start(Flux<TopicListener.DataFromTopic> input) {
        super.start(input);
        SenderOptions<String, String> senderOptions = senderOptions(appConfig);
        this.sender = KafkaSender.create(senderOptions);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        if (sender != null) {
            sender.close();
            sender = null;
        }
    }

    private static SenderOptions<String, String> senderOptions(ApplicationConfig config) {
        String bootstrapServers = config.getKafkaBootStrapServers();

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return SenderOptions.create(props);
    }

    private SenderRecord<String, String, Integer> senderRecord(DataToConsumer output, Job infoJob) {
        int correlationMetadata = 2;
        String topic = infoJob.getParameters().getKafkaOutputTopic();
        return SenderRecord.create(new ProducerRecord<>(topic, output.key, output.value), correlationMetadata);
    }

}
