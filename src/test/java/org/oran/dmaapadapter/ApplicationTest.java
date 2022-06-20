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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.configuration.WebClientConfig;
import org.oran.dmaapadapter.configuration.WebClientConfig.HttpProxyConfig;
import org.oran.dmaapadapter.controllers.ProducerCallbacksController;
import org.oran.dmaapadapter.r1.ConsumerJobInfo;
import org.oran.dmaapadapter.r1.ProducerJobInfo;
import org.oran.dmaapadapter.repository.InfoTypes;
import org.oran.dmaapadapter.repository.Job;
import org.oran.dmaapadapter.repository.Jobs;
import org.oran.dmaapadapter.repository.filters.PmReport;
import org.oran.dmaapadapter.repository.filters.PmReportFilter;
import org.oran.dmaapadapter.tasks.DataConsumer;
import org.oran.dmaapadapter.tasks.ProducerRegstrationTask;
import org.oran.dmaapadapter.tasks.TopicListener;
import org.oran.dmaapadapter.tasks.TopicListeners;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = { //
        "server.ssl.key-store=./config/keystore.jks", //
        "app.webclient.trust-store=./config/truststore.jks", //
        "app.webclient.trust-store-used=true", //
        "app.configuration-filepath=./src/test/resources/test_application_configuration.json"//
})
class ApplicationTest {

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
    TopicListeners topicListeners;

    @Autowired
    ProducerRegstrationTask producerRegistrationTask;

    @Autowired
    private SecurityContext securityContext;

    private com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();

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

    @BeforeEach
    void setPort() {
        this.applicationConfig.setLocalServerHttpPort(this.localServerHttpPort);
    }

    @AfterEach
    void reset() {
        this.consumerController.testResults.reset();
        this.icsSimulatorController.testResults.reset();
        this.jobs.clear();
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

    private Object jsonObjectRegexp() {
        return jsonObjectFilter(".*", Job.Parameters.REGEXP_TYPE);

    }

    private Object jsonObjectJsonPath() {
        return jsonObjectFilter("$", Job.Parameters.JSON_PATH_FILTER_TYPE);
    }

    private String quote(String str) {
        final String q = "\"";
        return q + str.replace(q, "\\\"") + q;
    }

    private Object jsonObjectFilter(String filter, String filterType) {
        return toJson("{" + quote("filter") + ":" + quote(filter) + "," + quote("filterType") + ":" + quote(filterType)
                + "}");
    }

    private Object toJson(String json) {
        try {
            return JsonParser.parseString(json).getAsJsonObject();
        } catch (Exception e) {
            throw new NullPointerException(e.toString());
        }
    }

    private ConsumerJobInfo consumerJobInfo(String typeId, String infoJobId, Object filter) {
        try {
            String targetUri = baseUrl() + ConsumerController.CONSUMER_TARGET_URL;
            return new ConsumerJobInfo(typeId, filter, "owner", targetUri, "");
        } catch (Exception e) {
            return null;
        }
    }

    private void waitForRegistration() {
        // Register producer, Register types
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        producerRegistrationTask.supervisionTask().block();

        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());
        assertThat(producerRegistrationTask.isRegisteredInIcs()).isTrue();
        assertThat(icsSimulatorController.testResults.types).hasSize(this.types.size());
    }

    @Test
    void generateApiDoc() throws IOException {
        String url = "https://localhost:" + applicationConfig.getLocalServerHttpPort() + "/v3/api-docs";
        ResponseEntity<String> resp = restClient().getForEntity(url).block();
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        JSONObject jsonObj = new JSONObject(resp.getBody());
        assertThat(jsonObj.remove("servers")).isNotNull();

        String indented = (jsonObj).toString(4);
        String docDir = "api/";
        Files.createDirectories(Paths.get(docDir));
        try (PrintStream out = new PrintStream(new FileOutputStream(docDir + "api.json"))) {
            out.print(indented);
        }
    }

    @Test
    void testResponseCodes() throws Exception {
        String supervisionUrl = baseUrl() + ProducerCallbacksController.SUPERVISION_URL;
        ResponseEntity<String> resp = restClient().getForEntity(supervisionUrl).block();
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);

        String jobUrl = baseUrl() + ProducerCallbacksController.JOB_URL;
        resp = restClient().deleteForEntity(jobUrl + "/junk").block();
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);

        ProducerJobInfo info = new ProducerJobInfo(null, "id", "typeId", "targetUri", "owner", "lastUpdated");
        String body = gson.toJson(info);
        testErrorCode(restClient().post(jobUrl, body, MediaType.APPLICATION_JSON), HttpStatus.NOT_FOUND,
                "Could not find type");
    }

    @Test
    void testReceiveAndPostDataFromKafka() throws Exception {
        final String JOB_ID = "ID";
        final String TYPE_ID = "KafkaInformationType";
        waitForRegistration();

        // Create a job
        Job.Parameters param = new Job.Parameters(null, null, new Job.BufferTimeout(123, 456), 1, null);
        String targetUri = baseUrl() + ConsumerController.CONSUMER_TARGET_URL;
        ConsumerJobInfo kafkaJobInfo = new ConsumerJobInfo(TYPE_ID, toJson(gson.toJson(param)), "owner", targetUri, "");

        this.icsSimulatorController.addJob(kafkaJobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        DataConsumer kafkaConsumer = this.topicListeners.getDataConsumers().get(TYPE_ID, JOB_ID);

        // Handle received data from Kafka, check that it has been posted to the
        // consumer
        kafkaConsumer.start(Flux.just(new TopicListener.Output("key", "data")));

        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(1));
        assertThat(consumer.receivedBodies.get(0)).isEqualTo("[\"data\"]");
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "application/json");

        // Test send an exception
        kafkaConsumer.start(Flux.error(new NullPointerException()));

        // Test regular restart of stopped
        kafkaConsumer.stop();
        this.topicListeners.restartNonRunningKafkaTopics();
        await().untilAsserted(() -> assertThat(kafkaConsumer.isRunning()).isTrue());
    }

    @Test
    void testReceiveAndPostDataFromDmaapBuffering() throws Exception {
        final String JOB_ID = "testReceiveAndPostDataFromDmaap";

        // Register producer, Register types
        waitForRegistration();

        // Create a job
        Job.Parameters param = new Job.Parameters(null, null, new Job.BufferTimeout(123, 456), 1, null);
        ConsumerJobInfo jobInfo = consumerJobInfo("DmaapInformationType", JOB_ID, toJson(gson.toJson(param)));
        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        // Return two messages from DMAAP and verify that these are sent to the owner of
        // the job (consumer)
        DmaapSimulatorController.addResponse("[\"DmaapResponse1\", \"DmaapResponse2\"]");
        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(1));
        assertThat(consumer.receivedBodies.get(0)).isEqualTo("[\"DmaapResponse1\", \"DmaapResponse2\"]");
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "application/json");

        String jobUrl = baseUrl() + ProducerCallbacksController.JOB_URL;
        String jobs = restClient().get(jobUrl).block();
        assertThat(jobs).contains(JOB_ID);

        // Delete the job
        this.icsSimulatorController.deleteJob(JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
    }

    @Test
    void testReceiveAndPostDataFromDmaapNonBuffering() throws Exception {
        final String JOB_ID = "testReceiveAndPostDataFromDmaapNonBuffering";

        // Register producer, Register types
        waitForRegistration();

        // Create a job
        Job.Parameters param = new Job.Parameters(null, null, null, 1, null);
        ConsumerJobInfo jobInfo = consumerJobInfo("DmaapInformationType", JOB_ID, toJson(gson.toJson(param)));
        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        // Return two messages from DMAAP and verify that these are sent to the owner of
        // the job (consumer)
        DmaapSimulatorController.addResponse("[\"DmaapResponse1\", \"DmaapResponse2\"]");
        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(2));
        assertThat(consumer.receivedBodies.get(0)).isEqualTo("DmaapResponse1");
        assertThat(consumer.receivedBodies.get(1)).isEqualTo("DmaapResponse2");
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "text/plain;charset=UTF-8");

        // Delete the job
        this.icsSimulatorController.deleteJob(JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isZero());
    }

    static class PmReportArray extends ArrayList<PmReport> {
    };

    @Test
    void testPmFiltering() throws Exception {
        // Create a job
        final String JOB_ID = "ID";

        // Register producer, Register types
        waitForRegistration();

        // Create a job with a PM filter
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();

        filterData.getMeasTypes().add("succImmediateAssignProcs");
        filterData.getMeasObjInstIds().add("UtranCell=Gbg-997");
        filterData.getSourceNames().add("O-DU-1122");
        filterData.getMeasuredEntityDns().add("ManagedElement=RNC-Gbg-1");
        Job.Parameters param = new Job.Parameters(filterData, Job.Parameters.PM_FILTER_TYPE,
                new Job.BufferTimeout(123, 456), null, null);
        String paramJson = gson.toJson(param);
        ConsumerJobInfo jobInfo = consumerJobInfo("PmInformationType", "EI_PM_JOB_ID", toJson(paramJson));

        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        // Return one messagefrom DMAAP and verify that the job (consumer) receives a
        // filtered PM message
        String path = "./src/test/resources/pm_report.json";
        String pmReportJson = Files.readString(Path.of(path), Charset.defaultCharset());
        DmaapSimulatorController.addPmResponse(pmReportJson);

        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(1));
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "application/json");
        String receivedFiltered = consumer.receivedBodies.get(0);
        assertThat(receivedFiltered).contains("succImmediateAssignProcs").doesNotContain("\"p\":2").contains("\"p\":1");

        PmReportArray reportsParsed = gson.fromJson(receivedFiltered, PmReportArray.class);
        assertThat(reportsParsed).hasSize(1);

    }

    @Test
    void testJsltFiltering() throws Exception {
        final String JOB_ID = "ID";

        // Register producer, Register types
        waitForRegistration();

        // Create a job with a PM filter
        String expresssion = "if(.event.commonEventHeader.sourceName == \"O-DU-1122\")" //
                + ".";
        Job.Parameters param = new Job.Parameters(expresssion, Job.Parameters.JSLT_FILTER_TYPE, null, null, null);
        String paramJson = gson.toJson(param);
        ConsumerJobInfo jobInfo = consumerJobInfo("PmInformationType", JOB_ID, toJson(paramJson));

        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        // Return one messagefrom DMAAP and verify that the job (consumer) receives a
        // filtered PM message
        String path = "./src/test/resources/pm_report.json";
        String pmReportJson = Files.readString(Path.of(path), Charset.defaultCharset());
        DmaapSimulatorController.addPmResponse(pmReportJson);

        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(1));
        String receivedFiltered = consumer.receivedBodies.get(0);
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "application/json");
        assertThat(receivedFiltered).contains("event");
    }

    @Test
    void testAuthToken() throws Exception {

        // Create an auth token
        final String AUTH_TOKEN = "testToken";
        Path authFile = Files.createTempFile("icsTestAuthToken", ".txt");
        Files.write(authFile, AUTH_TOKEN.getBytes());
        this.securityContext.setAuthTokenFilePath(authFile);

        final String JOB_ID = "ID";

        // Register producer, Register types
        waitForRegistration();

        // Create a job with a PM filter

        ConsumerJobInfo jobInfo = consumerJobInfo("DmaapInformationType", JOB_ID, jsonObjectRegexp());

        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        DmaapSimulatorController.addResponse("[\"Hello\"]");

        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedHeaders).hasSize(1));
        String received = consumer.receivedBodies.get(0);
        assertThat(received).isEqualTo("Hello");
        // This is the only time it is verified that mime type is plaintext when isJson
        // is false and buffering is not used
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "text/plain;charset=UTF-8");

        // Check that the auth token was received by the consumer
        assertThat(consumer.receivedHeaders).hasSize(1);
        Map<String, String> headers = consumer.receivedHeaders.get(0);
        assertThat(headers).containsEntry("authorization", "Bearer " + AUTH_TOKEN);
        Files.delete(authFile);
    }

    @Test
    void testJsonPathFiltering() throws Exception {
        final String JOB_ID = "ID";

        // Register producer, Register types
        waitForRegistration();

        // Create a job with JsonPath Filtering
        ConsumerJobInfo jobInfo = consumerJobInfo("PmInformationType", JOB_ID, this.jsonObjectJsonPath());

        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));

        // Return one messagefrom DMAAP and verify that the job (consumer) receives a
        // filtered PM message
        String path = "./src/test/resources/pm_report.json";
        String pmReportJson = Files.readString(Path.of(path), Charset.defaultCharset());
        DmaapSimulatorController.addPmResponse(pmReportJson);

        ConsumerController.TestResults consumer = this.consumerController.testResults;
        await().untilAsserted(() -> assertThat(consumer.receivedBodies).hasSize(1));
        String receivedFiltered = consumer.receivedBodies.get(0);
        assertThat(consumer.receivedHeaders.get(0)).containsEntry("content-type", "application/json");
        assertThat(receivedFiltered).contains("event");
    }

    @Test
    void testPmFilteringKafka() throws Exception {
        // Test that the schema for kafka and pm filtering is OK.

        // Create a job
        final String JOB_ID = "ID";

        // Register producer, Register types
        waitForRegistration();

        // Create a job with a PM filter
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.getMeasTypes().add("succImmediateAssignProcs");
        Job.Parameters param = new Job.Parameters(filterData, Job.Parameters.PM_FILTER_TYPE, null, null, null);
        String paramJson = gson.toJson(param);

        ConsumerJobInfo jobInfo = consumerJobInfo("PmInformationTypeKafka", "EI_PM_JOB_ID", toJson(paramJson));
        this.icsSimulatorController.addJob(jobInfo, JOB_ID, restClient());
        await().untilAsserted(() -> assertThat(this.jobs.size()).isEqualTo(1));
    }

    @Test
    void testReRegister() throws Exception {
        // Wait foir register types and producer
        waitForRegistration();

        // Clear the registration, should trigger a re-register
        icsSimulatorController.testResults.reset();
        producerRegistrationTask.supervisionTask().block();
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo).isNotNull());
        assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds).hasSize(this.types.size());

        // Just clear the registerred types, should trigger a re-register
        icsSimulatorController.testResults.types.clear();
        await().untilAsserted(() -> assertThat(icsSimulatorController.testResults.registrationInfo.supportedTypeIds)
                .hasSize(this.types.size()));
    }

    public static void testErrorCode(Mono<?> request, HttpStatus expStatus, String responseContains) {
        testErrorCode(request, expStatus, responseContains, true);
    }

    public static void testErrorCode(Mono<?> request, HttpStatus expStatus, String responseContains,
            boolean expectApplicationProblemJsonMediaType) {
        StepVerifier.create(request) //
                .expectSubscription() //
                .expectErrorMatches(
                        t -> checkWebClientError(t, expStatus, responseContains, expectApplicationProblemJsonMediaType)) //
                .verify();
    }

    private static boolean checkWebClientError(Throwable throwable, HttpStatus expStatus, String responseContains,
            boolean expectApplicationProblemJsonMediaType) {
        assertTrue(throwable instanceof WebClientResponseException);
        WebClientResponseException responseException = (WebClientResponseException) throwable;
        assertThat(responseException.getStatusCode()).isEqualTo(expStatus);
        assertThat(responseException.getResponseBodyAsString()).contains(responseContains);
        if (expectApplicationProblemJsonMediaType) {
            assertThat(responseException.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        }
        return true;
    }
}
