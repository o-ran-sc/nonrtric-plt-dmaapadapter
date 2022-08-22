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

package org.oran.dmaapadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonParser;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.configuration.WebClientConfig;
import org.oran.dmaapadapter.configuration.WebClientConfig.HttpProxyConfig;
import org.oran.dmaapadapter.exceptions.ServiceException;
import org.oran.dmaapadapter.filter.PmReportFilter;
import org.oran.dmaapadapter.r1.ConsumerJobInfo;
import org.oran.dmaapadapter.repository.InfoType;
import org.oran.dmaapadapter.repository.InfoTypes;
import org.oran.dmaapadapter.repository.Job;
import org.oran.dmaapadapter.repository.Jobs;
import org.oran.dmaapadapter.tasks.KafkaTopicListener;
import org.oran.dmaapadapter.tasks.TopicListener;
import org.oran.dmaapadapter.tasks.TopicListeners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;

import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@SuppressWarnings("java:S3577") // Rename class
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT)
@TestPropertySource(properties = { //
        "server.ssl.key-store=./config/keystore.jks", //
        "app.webclient.trust-store=./config/truststore.jks", //
        "app.configuration-filepath=./src/test/resources/test_application_configuration.json", //
        "app.pm-files-path=./src/test/resources/" }) //
class IntegrationWithKafka {

    final String TYPE_ID = "KafkaInformationType";
    final String PM_TYPE_ID = "PmInformationTypeKafka";

    @Autowired
    private ApplicationConfig applicationConfig;

    @Autowired
    private Jobs jobs;

    @Autowired
    private InfoTypes types;

    @Autowired
    private ConsumerController consumerController;

    @Autowired
    private IcsSimulatorController icsSimulatorController;

    @Autowired
    private TopicListeners topicListeners;

    @Autowired
    private SecurityContext securityContext;

    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();

    private final Logger logger = LoggerFactory.getLogger(IntegrationWithKafka.class);

    @LocalServerPort
    int localServerHttpPort;

    static class TestApplicationConfig extends ApplicationConfig {
        @Override
        public String getIcsBaseUrl() {
            return thisProcessUrl();
        }

        @Override
        public String getDmaapBaseUrl() {
            return thisProcessUrl();
        }

        @Override
        public String getSelfUrl() {
            return thisProcessUrl();
        }

        private String thisProcessUrl() {
            final String url = "https://localhost:" + getLocalServerHttpPort();
            return url;
        }
    }

    /**
     * Overrides the BeanFactory.
     */
    @TestConfiguration
    static class TestBeanFactory extends BeanFactory {

        @Override
        @Bean
        public ServletWebServerFactory servletContainer() {
            return new TomcatServletWebServerFactory();
        }

        @Override
        @Bean
        public ApplicationConfig getApplicationConfig() {
            TestApplicationConfig cfg = new TestApplicationConfig();
            return cfg;
        }
    }

    private static class KafkaReceiver {
        public final String OUTPUT_TOPIC;
        private TopicListener.DataFromTopic receivedKafkaOutput;
        private final Logger logger = LoggerFactory.getLogger(IntegrationWithKafka.class);

        int count = 0;

        public KafkaReceiver(ApplicationConfig applicationConfig, String outputTopic) {
            this.OUTPUT_TOPIC = outputTopic;

            // Create a listener to the output topic. The KafkaTopicListener happens to be
            // suitable for that,
            InfoType type = InfoType.builder().id("id").kafkaInputTopic(OUTPUT_TOPIC).dataType("dataType").build();

            KafkaTopicListener topicListener = new KafkaTopicListener(applicationConfig, type,
                    "TestClientId" + "_" + outputTopic);

            topicListener.getFlux() //
                    .doOnNext(this::set) //
                    .doFinally(sig -> logger.info("Finally " + sig)) //
                    .subscribe();
        }

        private void set(TopicListener.DataFromTopic receivedKafkaOutput) {
            this.receivedKafkaOutput = receivedKafkaOutput;
            this.count++;
            logger.debug("*** received {}, {}", OUTPUT_TOPIC, receivedKafkaOutput);
        }

        synchronized String lastKey() {
            return this.receivedKafkaOutput.key;
        }

        synchronized String lastValue() {
            return this.receivedKafkaOutput.value;
        }

        void reset() {
            count = 0;
            this.receivedKafkaOutput = new TopicListener.DataFromTopic("", "");
        }
    }

    private static KafkaReceiver kafkaReceiver;
    private static KafkaReceiver kafkaReceiver2;

    @BeforeEach
    void init() {
        if (kafkaReceiver == null) {
            kafkaReceiver = new KafkaReceiver(this.applicationConfig, "ouputTopic");
            kafkaReceiver2 = new KafkaReceiver(this.applicationConfig, "ouputTopic2");
        }
        kafkaReceiver.reset();
        kafkaReceiver2.reset();
    }

    @AfterEach
    void reset() {
        for (Job job : this.jobs.getAll()) {
            this.icsSimulatorController.deleteJob(job.getId(), restClient());
        }
        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
        await().untilAsserted(() -> assertThat(this.topicListeners.getDataDistributors().keySet()).isEmpty());

        this.consumerController.testResults.reset();
        this.icsSimulatorController.testResults.reset();
    }

    private AsyncRestClient restClient(boolean useTrustValidation) {
        WebClientConfig config = this.applicationConfig.getWebClientConfig();
        HttpProxyConfig httpProxyConfig = HttpProxyConfig.builder() //
                .httpProxyHost("") //
                .httpProxyPort(0) //
                .build();
        config = WebClientConfig.builder() //
                .keyStoreType(config.getKeyStoreType()) //
                .keyStorePassword(config.getKeyStorePassword()) //
                .keyStore(config.getKeyStore()) //
                .keyPassword(config.getKeyPassword()) //
                .isTrustStoreUsed(useTrustValidation) //
                .trustStore(config.getTrustStore()) //
                .trustStorePassword(config.getTrustStorePassword()) //
                .httpProxyConfig(httpProxyConfig).build();

        AsyncRestClientFactory restClientFactory = new AsyncRestClientFactory(config, securityContext);
        return restClientFactory.createRestClientNoHttpProxy(baseUrl());
    }

    private AsyncRestClient restClient() {
        return restClient(false);
    }

    private String baseUrl() {
        return "https://localhost:" + this.applicationConfig.getLocalServerHttpPort();
    }

    private static Object jobParametersAsJsonObject(String filter, long maxTimeMiliseconds, int maxSize,
            int maxConcurrency) {
        Job.BufferTimeout buffer = maxSize > 0 ? new Job.BufferTimeout(maxSize, maxTimeMiliseconds) : null;
        Job.Parameters param = new Job.Parameters(filter, Job.Parameters.REGEXP_TYPE, buffer, maxConcurrency, null);
        String str = gson.toJson(param);
        return jsonObject(str);
    }

    private static Object jsonObject(String json) {
        try {
            return JsonParser.parseString(json).getAsJsonObject();
        } catch (Exception e) {
            throw new NullPointerException(e.toString());
        }
    }

    ConsumerJobInfo consumerJobInfo(String filter, Duration maxTime, int maxSize, int maxConcurrency) {
        try {
            String targetUri = baseUrl() + ConsumerController.CONSUMER_TARGET_URL;
            return new ConsumerJobInfo(TYPE_ID,
                    jobParametersAsJsonObject(filter, maxTime.toMillis(), maxSize, maxConcurrency), "owner", targetUri,
                    "");
        } catch (Exception e) {
            return null;
        }
    }

    ConsumerJobInfo consumerJobInfoKafka(String topic, PmReportFilter.FilterData filterData) {
        try {
            Job.Parameters param = new Job.Parameters(filterData, Job.Parameters.PM_FILTER_TYPE, null, 1, topic);
            String str = gson.toJson(param);
            Object parametersObj = jsonObject(str);

            return new ConsumerJobInfo(PM_TYPE_ID, parametersObj, "owner", null, "");
        } catch (Exception e) {
            return null;
        }
    }

    ConsumerJobInfo consumerJobInfoKafka(String topic) {
        try {
            Job.Parameters param = new Job.Parameters(null, null, null, 1, topic);
            String str = gson.toJson(param);
            Object parametersObj = jsonObject(str);

            return new ConsumerJobInfo(TYPE_ID, parametersObj, "owner", null, "");
        } catch (Exception e) {
            return null;
        }
    }

    private SenderOptions<String, String> senderOptions() {
        String bootstrapServers = this.applicationConfig.getKafkaBootStrapServers();

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producerx");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return SenderOptions.create(props);
    }

    private SenderRecord<String, String, Integer> senderRecord(String data, String key, String typeId) {
        final InfoType infoType = this.types.get(typeId);
        int correlationMetadata = 2;
        return SenderRecord.create(new ProducerRecord<>(infoType.getKafkaInputTopic(), key, data), correlationMetadata);
    }

    private void sendDataToStream(Flux<SenderRecord<String, String, Integer>> dataToSend) {
        final KafkaSender<String, String> sender = KafkaSender.create(senderOptions());

        sender.send(dataToSend) //
                .doOnError(e -> logger.error("Send failed", e)) //
                .blockLast();

        sender.close();
    }

    private void verifiedReceivedByConsumer(String... strings) {
        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(strings.length));
        for (String s : strings) {
            assertTrue(consumer.hasReceived(s));
        }
    }

    private void verifiedReceivedByConsumerLast(String s) {
        ConsumerController.TestResults consumer = this.consumerController.testResults;

        await().untilAsserted(() -> assertThat(last(consumer.receivedBodies)).isEqualTo(s));
    }

    private String last(List<String> l) {
        return l.isEmpty() ? "" : l.get(l.size() - 1);
    }

    @SuppressWarnings("squid:S2925") // "Thread.sleep" should not be used in tests.
    private static void waitForKafkaListener() throws InterruptedException {
        Thread.sleep(4000);
    }

    @Test
    void simpleCase() throws Exception {
        final String JOB_ID = "ID";

        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        this.icsSimulatorController.addJob(consumerJobInfo(null, Duration.ZERO, 0, 1), JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));
        waitForKafkaListener();

        var dataToSend = Flux.just(senderRecord("Message", "", TYPE_ID));
        sendDataToStream(dataToSend);

        verifiedReceivedByConsumer("Message");
    }

    @Test
    void kafkaIntegrationTest() throws Exception {
        final String JOB_ID1 = "ID1";
        final String JOB_ID2 = "ID2";

        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        // Create two jobs. One buffering and one with a filter
        this.icsSimulatorController.addJob(consumerJobInfo(null, Duration.ofMillis(400), 10, 20), JOB_ID1,
                restClient());
        this.icsSimulatorController.addJob(consumerJobInfo("^Message_1$", Duration.ZERO, 0, 1), JOB_ID2, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(2));
        waitForKafkaListener();

        var dataToSend = Flux.range(1, 3).map(i -> senderRecord("Message_" + i, "", TYPE_ID)); // Message_1,
                                                                                               // Message_2
                                                                                               // etc.
        sendDataToStream(dataToSend);

        verifiedReceivedByConsumer("Message_1", "[\"Message_1\", \"Message_2\", \"Message_3\"]");
    }

    @Test
    void sendToKafkaConsumer() throws ServiceException, InterruptedException {
        final String JOB_ID = "ID";

        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        this.icsSimulatorController.addJob(consumerJobInfoKafka(kafkaReceiver.OUTPUT_TOPIC), JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));
        waitForKafkaListener();

        String sendString = "testData " + Instant.now();
        String sendKey = "key " + Instant.now();
        var dataToSend = Flux.just(senderRecord(sendString, sendKey, TYPE_ID));
        sendDataToStream(dataToSend);

        await().untilAsserted(() -> assertThat(kafkaReceiver.lastValue()).isEqualTo(sendString));
        assertThat(kafkaReceiver.lastKey()).isEqualTo(sendKey);
    }

    @SuppressWarnings("squid:S2925") // "Thread.sleep" should not be used in tests.
    @Test
    void kafkaCharacteristics() throws Exception {
        final String JOB_ID = "kafkaCharacteristics";

        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        this.icsSimulatorController.addJob(consumerJobInfoKafka(kafkaReceiver.OUTPUT_TOPIC), JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));
        waitForKafkaListener();

        final int NO_OF_OBJECTS = 100000;

        Instant startTime = Instant.now();

        var dataToSend = Flux.range(1, NO_OF_OBJECTS).map(i -> senderRecord("Message_" + i, "", TYPE_ID)); // Message_1,
                                                                                                           // etc.
        sendDataToStream(dataToSend);

        while (!kafkaReceiver.lastValue().equals("Message_" + NO_OF_OBJECTS)) {
            logger.info("sleeping {}", kafkaReceiver.lastValue());
            Thread.sleep(1000 * 1);
        }

        final long durationSeconds = Instant.now().getEpochSecond() - startTime.getEpochSecond();
        logger.info("*** Duration :" + durationSeconds + ", objects/second: " + NO_OF_OBJECTS / durationSeconds);
    }

    @SuppressWarnings("squid:S2925") // "Thread.sleep" should not be used in tests.
    @Test
    void kafkaCharacteristics_pmFilter() throws Exception {
        // Filter PM reports and sent to two jobs over Kafka

        final String JOB_ID = "kafkaCharacteristics";
        final String JOB_ID2 = "kafkaCharacteristics2";

        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.getMeasTypes().add("succImmediateAssignProcs");
        filterData.getMeasObjClass().add("UtranCell");

        this.icsSimulatorController.addJob(consumerJobInfoKafka(kafkaReceiver.OUTPUT_TOPIC, filterData), JOB_ID,
                restClient());
        this.icsSimulatorController.addJob(consumerJobInfoKafka(kafkaReceiver2.OUTPUT_TOPIC, filterData), JOB_ID2,
                restClient());

        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(2));
        waitForKafkaListener();

        final int NO_OF_OBJECTS = 100000;

        Instant startTime = Instant.now();

        KafkaTopicListener.NewFileEvent event = KafkaTopicListener.NewFileEvent.builder().filename("pm_report.json")
                .build();
        String eventAsString = gson.toJson(event);

        String path = "./src/test/resources/pm_report.json";
        String pmReportJson = Files.readString(Path.of(path), Charset.defaultCharset());

        var dataToSend = Flux.range(1, NO_OF_OBJECTS).map(i -> senderRecord(eventAsString, "key", PM_TYPE_ID));
        sendDataToStream(dataToSend);

        while (kafkaReceiver.count != NO_OF_OBJECTS) {
            logger.info("sleeping {}", kafkaReceiver.count);
            Thread.sleep(1000 * 1);
        }

        // System.out.println(kafkaReceiver.receivedKafkaOutput.value);

        final long durationSeconds = Instant.now().getEpochSecond() - startTime.getEpochSecond();
        logger.info("*** Duration :" + durationSeconds + ", objects/second: " + NO_OF_OBJECTS / durationSeconds);
        logger.info("***  kafkaReceiver2 :" + kafkaReceiver.count);
    }

    @Test
    void kafkaDeleteJobShouldNotStopListener() throws Exception {
        final String JOB_ID1 = "ID1";
        final String JOB_ID2 = "ID2";

        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        // Create two jobs.
        this.icsSimulatorController.addJob(consumerJobInfo(null, Duration.ofMillis(400), 1000, 1), JOB_ID1,
                restClient());
        this.icsSimulatorController.addJob(consumerJobInfo(null, Duration.ZERO, 0, 1), JOB_ID2, restClient());

        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(2));

        var dataToSend = Flux.range(1, 100).map(i -> senderRecord("Message_" + i, "", TYPE_ID)); // Message_1,
                                                                                                 // Message_2
                                                                                                 // etc.
        sendDataToStream(dataToSend); // this should not overflow

        // Delete jobs, recreate one
        this.icsSimulatorController.deleteJob(JOB_ID1, restClient());
        this.icsSimulatorController.deleteJob(JOB_ID2, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
        this.icsSimulatorController.addJob(consumerJobInfo(null, Duration.ZERO, 0, 1), JOB_ID2, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        dataToSend = Flux.just(senderRecord("Howdy", "", TYPE_ID));
        sendDataToStream(dataToSend);

        verifiedReceivedByConsumerLast("Howdy");
    }

}
