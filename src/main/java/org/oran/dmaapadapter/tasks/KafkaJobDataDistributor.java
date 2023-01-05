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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.repository.Job.Parameters.KafkaDeliveryInfo;
import org.oran.dmaapadapter.repository.Jobs.JobGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

/**
 * The class streams data from a multi cast sink and sends the data to the Job
 * owner via REST calls.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public class KafkaJobDataDistributor extends JobDataDistributor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaJobDataDistributor.class);

    private KafkaSender<byte[], byte[]> sender;

    public KafkaJobDataDistributor(JobGroup jobGroup, ApplicationConfig appConfig) {
        super(jobGroup, appConfig);

        SenderOptions<byte[], byte[]> senderOptions = senderOptions(appConfig, jobGroup.getDeliveryInfo());
        this.sender = KafkaSender.create(senderOptions);
    }

    @Override
    protected Mono<String> sendToClient(Filter.FilteredData data) {

        SenderRecord<byte[], byte[], Integer> senderRecord = senderRecord(data, this.getJobGroup().getDeliveryInfo());

        logger.trace("Sending data '{}' to Kafka topic: {}", StringUtils.truncate(data.getValueAString(), 10),
                this.getJobGroup().getDeliveryInfo());

        return this.sender.send(Mono.just(senderRecord)) //
                .doOnNext(n -> logger.debug("Sent data to Kafka topic: {}", this.getJobGroup().getDeliveryInfo())) //
                .doOnError(t -> logger.warn("Failed to send to Kafka, job: {}, reason: {}", this.getJobGroup().getId(),
                        t.getMessage())) //
                .onErrorResume(t -> Mono.empty()) //
                .collectList() //
                .map(x -> "ok");

    }

    @Override
    public synchronized void stop() {
        super.stop();
        if (sender != null) {
            sender.close();
            sender = null;
        }
    }

    private static SenderOptions<byte[], byte[]> senderOptions(ApplicationConfig config,
            KafkaDeliveryInfo deliveryInfo) {

        String bootstrapServers = deliveryInfo.getBootStrapServers();
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = config.getKafkaBootStrapServers();
        }

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return SenderOptions.create(props);
    }

    private SenderRecord<byte[], byte[], Integer> senderRecord(Filter.FilteredData output,
            KafkaDeliveryInfo deliveryInfo) {
        int correlationMetadata = 2;
        var producerRecord =
                new ProducerRecord<>(deliveryInfo.getTopic(), null, null, output.key, output.value, output.headers());
        return SenderRecord.create(producerRecord, correlationMetadata);
    }

}
