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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.filter.Filter.FilteredData;
import org.oran.dmaapadapter.repository.Job;
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

    public KafkaJobDataDistributor(Job job, ApplicationConfig appConfig) {
        super(job, appConfig);

        SenderOptions<byte[], byte[]> senderOptions = senderOptions(appConfig);
        this.sender = KafkaSender.create(senderOptions);
    }

    @Override
    protected Mono<String> sendToClient(Filter.FilteredData data) {
        Job job = this.getJob();
        SenderRecord<byte[], byte[], Integer> senderRecord = senderRecord(data, job);

        logger.trace("Sending data '{}' to Kafka topic: {}", StringUtils.truncate(data.getValueAString(), 10),
                job.getParameters().getKafkaOutputTopic());

        return this.sender.send(Mono.just(senderRecord)) //
                .doOnNext(n -> logger.debug("Sent data to Kafka topic: {}", job.getParameters().getKafkaOutputTopic())) //
                .doOnError(
                        t -> logger.warn("Failed to send to Kafka, job: {}, reason: {}", job.getId(), t.getMessage())) //
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

    private static SenderOptions<byte[], byte[]> senderOptions(ApplicationConfig config) {
        String bootstrapServers = config.getKafkaBootStrapServers();

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return SenderOptions.create(props);
    }

    private SenderRecord<byte[], byte[], Integer> senderRecord(Filter.FilteredData output, Job infoJob) {
        int correlationMetadata = 2;
        String topic = infoJob.getParameters().getKafkaOutputTopic();
        var producerRecord = new ProducerRecord<>(topic, null, null, output.key, output.value, headers(output));
        return SenderRecord.create(producerRecord, correlationMetadata);
    }

    private Iterable<Header> headers(Filter.FilteredData output) {
        ArrayList<Header> result = new ArrayList<>();
        if (output.isZipped()) {
            Header h = new RecordHeader(FilteredData.ZIP_PROPERTY, null);
            result.add(h);
        }
        return result;
    }

}
