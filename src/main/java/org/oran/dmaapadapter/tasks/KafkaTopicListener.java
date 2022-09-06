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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.InfoType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

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

    @Getter
    private static S3AsyncClient s3AsynchClient;

    private static Gson gson = new GsonBuilder() //
            .disableHtmlEscaping() //
            .create(); //

    @ToString
    @Builder
    public static class NewFileEvent {
        @Getter
        private String filename;

        @Getter
        private String objectStoreBucket;
    }

    public KafkaTopicListener(ApplicationConfig applicationConfig, InfoType type) {
        this.applicationConfig = applicationConfig;
        this.type = type;
        if (applicationConfig.isS3Enabled()) {
            synchronized (KafkaTopicListener.class) {
                if (s3AsynchClient == null) {
                    s3AsynchClient = getS3AsyncClientBuilder().build();
                }
            }
        }
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
                .doOnNext(input -> logger.debug("Received from kafka topic: {} :{}", this.type.getKafkaInputTopic(),
                        input.value())) //
                .doOnError(t -> logger.error("KafkaTopicReceiver error: {}", t.getMessage())) //
                .doFinally(sig -> logger.error("KafkaTopicReceiver stopped, reason: {}", sig)) //
                .filter(t -> !t.value().isEmpty() || !t.key().isEmpty()) //
                .map(input -> new DataFromTopic(input.key(), input.value())) //
                .flatMap(this::getDataFromFileIfNewPmFileEvent, 100) //
                .publish() //
                .autoConnect(1);
    }

    private S3AsyncClientBuilder getS3AsyncClientBuilder() {
        URI uri = URI.create(this.applicationConfig.getS3EndpointOverride());
        return S3AsyncClient.builder() //
                .region(Region.US_EAST_1) //
                .endpointOverride(uri) //
                .credentialsProvider(StaticCredentialsProvider.create( //
                        AwsBasicCredentials.create(this.applicationConfig.getS3AccessKeyId(), //
                                this.applicationConfig.getS3SecretAccessKey())));

    }

    private Mono<String> getDataFromS3Object(String bucket, String key) {
        if (!this.applicationConfig.isS3Enabled()) {
            logger.error("Missing S3 confinguration in application.yaml, ignoring bucket: {}, key: {}", bucket, key);
            return Mono.empty();
        }

        GetObjectRequest request = GetObjectRequest.builder() //
                .bucket(bucket) //
                .key(key) //
                .build();

        CompletableFuture<ResponseBytes<GetObjectResponse>> future = s3AsynchClient.getObject(request,
                AsyncResponseTransformer.toBytes());

        return Mono.fromFuture(future) //
                .map(b -> new String(b.asByteArray(), Charset.defaultCharset())) //
                .doOnError(t -> logger.error("Failed to get file from S3 {}", t.getMessage())) //
                .doOnEach(n -> logger.debug("Read file from S3: {} {}", bucket, key)) //
                .onErrorResume(t -> Mono.empty());
    }

    private Mono<DataFromTopic> getDataFromFileIfNewPmFileEvent(DataFromTopic data) {
        if (this.type.getDataType() != InfoType.DataType.PM_DATA || data.value.length() > 1000) {
            return Mono.just(data);
        }

        try {
            NewFileEvent ev = gson.fromJson(data.value, NewFileEvent.class);
            if (ev.getObjectStoreBucket() != null) {
                if (applicationConfig.isS3Enabled()) {
                    return getDataFromS3Object(ev.getObjectStoreBucket(), ev.getFilename()) //
                            .map(str -> new DataFromTopic(data.key, str));
                } else {
                    logger.error("S3 is not configured in application.yaml, ignoring: {}", data);
                    return Mono.empty();
                }
            } else {
                if (applicationConfig.getPmFilesPath().isEmpty() || ev.filename == null) {
                    logger.debug("Passing data {}", data);
                    return Mono.just(data);
                } else {
                    Path path = Path.of(this.applicationConfig.getPmFilesPath(), ev.getFilename());
                    String pmReportJson = Files.readString(path, Charset.defaultCharset());
                    return Mono.just(new DataFromTopic(data.key, pmReportJson));
                }
            }
        } catch (Exception e) {
            return Mono.just(data);
        }
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
