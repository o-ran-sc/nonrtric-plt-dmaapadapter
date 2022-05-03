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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.configuration.ImmutableHttpProxyConfig;
import org.oran.dmaapadapter.configuration.ImmutableWebClientConfig;
import org.oran.dmaapadapter.configuration.WebClientConfig;
import org.oran.dmaapadapter.configuration.WebClientConfig.HttpProxyConfig;
import org.oran.dmaapadapter.r1.ConsumerJobInfo;
import org.oran.dmaapadapter.repository.Job;
import org.oran.dmaapadapter.repository.Jobs;
import org.oran.dmaapadapter.tasks.ProducerRegstrationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SuppressWarnings("java:S3577") // Rename class
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT)
@TestPropertySource(properties = { //
        "server.ssl.key-store=./config/keystore.jks", //
        "app.webclient.trust-store=./config/truststore.jks", //
        "app.configuration-filepath=./src/test/resources/test_application_configuration.json", //
        "app.ics-base-url=https://localhost:8434" //
})
class IntegrationWithIcs {

    private static final String DMAAP_JOB_ID = "DMAAP_JOB_ID";
    private static final String DMAAP_TYPE_ID = "DmaapInformationType";
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    private ApplicationConfig applicationConfig;

    @Autowired
    private ProducerRegstrationTask producerRegstrationTask;

    @Autowired
    private Jobs jobs;

    @Autowired
    private ConsumerController consumerController;

    private static Gson gson = new GsonBuilder().create();

    static class TestApplicationConfig extends ApplicationConfig {

        @Override
        public String getIcsBaseUrl() {
            return "https://localhost:8434";
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

    @AfterEach
    void reset() {
        this.consumerController.testResults.reset();
        assertThat(this.jobs.size()).isZero();
    }

    private AsyncRestClient restClient(boolean useTrustValidation) {
        WebClientConfig config = this.applicationConfig.getWebClientConfig();
        HttpProxyConfig httpProxyConfig = ImmutableHttpProxyConfig.builder() //
                .httpProxyHost("") //
                .httpProxyPort(0) //
                .build();
        config = ImmutableWebClientConfig.builder() //
                .keyStoreType(config.keyStoreType()) //
                .keyStorePassword(config.keyStorePassword()) //
                .keyStore(config.keyStore()) //
                .keyPassword(config.keyPassword()) //
                .isTrustStoreUsed(useTrustValidation) //
                .trustStore(config.trustStore()) //
                .trustStorePassword(config.trustStorePassword()) //
                .httpProxyConfig(httpProxyConfig).build();

        AsyncRestClientFactory restClientFactory = new AsyncRestClientFactory(config);
        return restClientFactory.createRestClientNoHttpProxy(selfBaseUrl());
    }

    private AsyncRestClient restClient() {
        return restClient(false);
    }

    private String selfBaseUrl() {
        return "https://localhost:" + this.applicationConfig.getLocalServerHttpPort();
    }

    private String icsBaseUrl() {
        return applicationConfig.getIcsBaseUrl();
    }

    private String jobUrl(String jobId) {
        return icsBaseUrl() + "/data-consumer/v1/info-jobs/" + jobId + "?typeCheck=true";
    }

    private void createInformationJobInIcs(String typeId, String jobId, String filter) {
        createInformationJobInIcs(jobId, consumerJobInfo(typeId, filter));
    }

    private void createInformationJobInIcs(String jobId, ConsumerJobInfo jobInfo) {
        String body = gson.toJson(jobInfo);
        restClient().putForEntity(jobUrl(jobId), body).block();
        logger.info("Created job {}, {}", jobId, body);
    }

    private void deleteInformationJobInIcs(String jobId) {
        try {
            restClient().delete(jobUrl(jobId)).block();
        } catch (Exception e) {
            logger.warn("Couldnot delete job: {}  reason: {}", jobId, e.getMessage());
        }
    }

    private ConsumerJobInfo consumerJobInfo(String typeId, String filter) {
        return consumerJobInfo(typeId, DMAAP_JOB_ID, filter);
    }

    private Object jsonObject(String json) {
        try {
            return JsonParser.parseString(json).getAsJsonObject();
        } catch (Exception e) {
            throw new NullPointerException(e.toString());
        }
    }

    private String quote(String str) {
        return "\"" + str + "\"";
    }

    private String reQuote(String str) {
        return str.replaceAll("'", "\\\"");
    }

    private String consumerUri() {
        return selfBaseUrl() + ConsumerController.CONSUMER_TARGET_URL;
    }

    private ConsumerJobInfo consumerJobInfo(String typeId, String infoJobId, String filter) {
        try {

            String jsonStr = "{ \"filter\" :" + quote(filter) + "}";
            return new ConsumerJobInfo(typeId, jsonObject(jsonStr), "owner", consumerUri(), "");
        } catch (Exception e) {
            logger.error("Error {}", e.getMessage());
            return null;
        }
    }

    @Test
    void testCreateKafkaJob() {
        await().untilAsserted(() -> assertThat(producerRegstrationTask.isRegisteredInIcs()).isTrue());
        final String TYPE_ID = "KafkaInformationType";

        Job.Parameters param =
                new Job.Parameters("filter", Job.Parameters.REGEXP_TYPE, new Job.BufferTimeout(123, 456), 1);

        ConsumerJobInfo jobInfo =
                new ConsumerJobInfo(TYPE_ID, jsonObject(gson.toJson(param)), "owner", consumerUri(), "");
        String body = gson.toJson(jobInfo);

        restClient().putForEntity(jobUrl("KAFKA_JOB_ID"), body).block();

        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        deleteInformationJobInIcs("KAFKA_JOB_ID");
        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
    }

    @Test
    void testKafkaJobParameterOutOfRange() {
        await().untilAsserted(() -> assertThat(producerRegstrationTask.isRegisteredInIcs()).isTrue());
        final String TYPE_ID = "KafkaInformationType";

        Job.Parameters param =
                new Job.Parameters("filter", Job.Parameters.REGEXP_TYPE, new Job.BufferTimeout(123, 170 * 1000), 1);

        ConsumerJobInfo jobInfo =
                new ConsumerJobInfo(TYPE_ID, jsonObject(gson.toJson(param)), "owner", consumerUri(), "");
        String body = gson.toJson(jobInfo);

        ApplicationTest.testErrorCode(restClient().put(jobUrl("KAFKA_JOB_ID"), body), HttpStatus.BAD_REQUEST,
                "Json validation failure");
    }

    @Test
    void testDmaapMessage() throws Exception {
        await().untilAsserted(() -> assertThat(producerRegstrationTask.isRegisteredInIcs()).isTrue());

        createInformationJobInIcs(DMAAP_TYPE_ID, DMAAP_JOB_ID, ".*DmaapResponse.*");

        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        DmaapSimulatorController.addResponse("DmaapResponse1");
        DmaapSimulatorController.addResponse("DmaapResponse2");
        DmaapSimulatorController.addResponse("Junk");

        ConsumerController.TestResults results = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(results.receivedBodies).hasSize(2));
        assertThat(results.receivedBodies.get(0)).isEqualTo("DmaapResponse1");

        deleteInformationJobInIcs(DMAAP_JOB_ID);

        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
    }

    @Test
    void testPmFilter() throws Exception {
        await().untilAsserted(() -> assertThat(producerRegstrationTask.isRegisteredInIcs()).isTrue());
        final String TYPE_ID = "PmInformationType";

        String jsonStr =
                reQuote("{ 'filterType' : 'pmdata', 'filter': { 'measTypes': [ 'succImmediateAssignProcs' ] } }");

        ConsumerJobInfo jobInfo = new ConsumerJobInfo(TYPE_ID, jsonObject(jsonStr), "owner", consumerUri(), "");

        createInformationJobInIcs(DMAAP_JOB_ID, jobInfo);
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        String path = "./src/test/resources/pm_report.json";
        String pmReportJson = Files.readString(Path.of(path), Charset.defaultCharset());
        DmaapSimulatorController.addPmResponse(pmReportJson);

        ConsumerController.TestResults results = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(results.receivedBodies).hasSize(1));

        String filtered = results.receivedBodies.get(0);
        assertThat(filtered).contains("succImmediateAssignProcs").doesNotContain("attTCHSeizures");

        logger.info(filtered);

        deleteInformationJobInIcs(DMAAP_JOB_ID);
        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
    }
}
