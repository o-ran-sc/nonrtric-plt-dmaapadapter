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

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import lombok.Getter;

import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.exceptions.ServiceException;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.filter.PmReportFilter;
import org.oran.dmaapadapter.repository.InfoType;
import org.oran.dmaapadapter.repository.Job;
import org.oran.dmaapadapter.tasks.KafkaTopicListener.NewFileEvent;
import org.oran.dmaapadapter.tasks.TopicListener.DataFromTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The class streams data from a multi cast sink and sends the data to the Job
 * owner via REST calls.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public abstract class JobDataDistributor {
    private static final Logger logger = LoggerFactory.getLogger(JobDataDistributor.class);
    @Getter
    private final Job job;
    private Disposable subscription;
    private final ErrorStats errorStats = new ErrorStats();
    private final ApplicationConfig applConfig;

    private final DataStore fileStore;
    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();

    private class ErrorStats {
        private int consumerFaultCounter = 0;
        private boolean irrecoverableError = false; // eg. overflow

        public void handleOkFromConsumer() {
            this.consumerFaultCounter = 0;
        }

        public void handleException(Throwable t) {
            if (t instanceof WebClientResponseException) {
                ++this.consumerFaultCounter;
            } else {
                irrecoverableError = true;
            }
        }

        public boolean isItHopeless() {
            final int STOP_AFTER_ERRORS = 5;
            return irrecoverableError || consumerFaultCounter > STOP_AFTER_ERRORS;
        }

        public void resetIrrecoverableErrors() {
            irrecoverableError = false;
        }
    }

    protected JobDataDistributor(Job job, ApplicationConfig applConfig) {
        this.job = job;
        this.applConfig = applConfig;
        this.fileStore = applConfig.isS3Enabled() ? new S3ObjectStore(applConfig) : new FileStore(applConfig);
    }

    public synchronized void start(Flux<TopicListener.DataFromTopic> input) {
        stop();

        collectHistoricalData();

        this.errorStats.resetIrrecoverableErrors();
        this.subscription = filterAndBuffer(input, this.job) //
                .flatMap(this::sendToClient, job.getParameters().getMaxConcurrency()) //
                .onErrorResume(this::handleError) //
                .subscribe(this::handleSentOk, //
                        this::handleExceptionInStream, //
                        () -> logger.warn("HttpDataConsumer stopped jobId: {}", job.getId()));
    }

    static class LockedException extends ServiceException {
        public LockedException(String file) {
            super(file, HttpStatus.NOT_FOUND);
        }
    }

    private void collectHistoricalData() {
        PmReportFilter filter = job.getFilter() instanceof PmReportFilter ? (PmReportFilter) job.getFilter() : null;

        if (filter != null) {
            this.fileStore.createLock(collectHistoricalDataLockName()) //
                    .flatMap(isLockGranted -> isLockGranted ? Mono.just(isLockGranted)
                            : Mono.error(new LockedException(collectHistoricalDataLockName()))) //
                    .flatMapMany(b -> Flux.fromIterable(filter.getFilterData().getSourceNames()))
                    .flatMap(sourceName -> fileStore.listFiles(DataStore.Bucket.FILES, sourceName), 1) //
                    .filter(fileName -> filterStartTime(filter.getFilterData().getPmRopStartTime(), fileName)) //
                    .map(this::createFakeEvent) //
                    .flatMap(event -> filterAndBuffer(event, this.job), 1) //
                    .flatMap(this::sendToClient, 1) //
                    .onErrorResume(this::handleCollectHistoricalDataError) //
                    .collectList() //
                    .flatMap(list -> fileStore.deleteLock(collectHistoricalDataLockName())) //
                    .subscribe();
        }
    }

    private Mono<String> handleCollectHistoricalDataError(Throwable t) {

        if (t instanceof LockedException) {
            logger.debug("Locked exception: {} job: {}", t.getMessage(), job.getId());
            return Mono.empty(); // Ignore
        } else {
            return fileStore.deleteLock(collectHistoricalDataLockName()) //
                    .map(bool -> "OK") //
                    .onErrorResume(t2 -> Mono.empty());
        }
    }

    private String collectHistoricalDataLockName() {
        return "collectHistoricalDataLock" + this.job.getId();
    }

    private Flux<TopicListener.DataFromTopic> createFakeEvent(String fileName) {

        NewFileEvent ev = new NewFileEvent(fileName, this.applConfig.getS3Bucket());

        return Flux.just(new TopicListener.DataFromTopic("", gson.toJson(ev)));
    }

    private boolean filterStartTime(String startTimeStr, String fileName) {
        // A20000626.2315+0200-2330+0200_HTTPS-6-73.xml.gz101.json
        try {
            String fileTimePart = fileName.substring(fileName.lastIndexOf("/") + 2);
            fileTimePart = fileTimePart.substring(0, 18);

            DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyyMMdd.HHmmZ").toFormatter();

            OffsetDateTime fileStartTime = OffsetDateTime.parse(fileTimePart, formatter);
            OffsetDateTime startTime = OffsetDateTime.parse(startTimeStr);

            return startTime.isBefore(fileStartTime);
        } catch (Exception e) {
            logger.warn("Time parsing exception: {}", e.getMessage());
            return false;
        }
    }

    private void handleExceptionInStream(Throwable t) {
        logger.warn("HttpDataConsumer exception: {}, jobId: {}", t.getMessage(), job.getId());
        stop();
    }

    protected abstract Mono<String> sendToClient(Filter.FilteredData output);

    public synchronized void stop() {
        if (this.subscription != null) {
            this.subscription.dispose();
            this.subscription = null;
        }
    }

    public synchronized boolean isRunning() {
        return this.subscription != null;
    }

    private Flux<Filter.FilteredData> filterAndBuffer(Flux<TopicListener.DataFromTopic> inputFlux, Job job) {
        Flux<Filter.FilteredData> filtered = //
                inputFlux.doOnNext(data -> job.getStatistics().received(data.value)) //
                        .flatMap(this::getDataFromFileIfNewPmFileEvent, 100) //
                        .map(job::filter) //
                        .filter(f -> !f.isEmpty()) //
                        .doOnNext(f -> job.getStatistics().filtered(f.value)); //

        if (job.isBuffered()) {
            filtered = filtered.map(input -> quoteNonJson(input.value, job)) //
                    .bufferTimeout( //
                            job.getParameters().getBufferTimeout().getMaxSize(), //
                            job.getParameters().getBufferTimeout().getMaxTime()) //
                    .map(buffered -> new Filter.FilteredData("", buffered.toString()));
        }
        return filtered;
    }

    private Mono<DataFromTopic> getDataFromFileIfNewPmFileEvent(DataFromTopic data) {
        if (this.job.getType().getDataType() != InfoType.DataType.PM_DATA || data.value.length() > 1000) {
            return Mono.just(data);
        }

        try {
            NewFileEvent ev = gson.fromJson(data.value, NewFileEvent.class);
            if (ev.getObjectStoreBucket() != null) {
                if (this.applConfig.isS3Enabled()) {
                    return fileStore.readFile(ev.getObjectStoreBucket(), ev.getFilename()) //
                            .map(str -> new DataFromTopic(data.key, str));
                } else {
                    logger.error("S3 is not configured in application.yaml, ignoring: {}", data);
                    return Mono.empty();
                }
            } else {
                if (applConfig.getPmFilesPath().isEmpty() || ev.getFilename() == null) {
                    logger.debug("Passing data {}", data);
                    return Mono.just(data);
                } else {
                    Path path = Path.of(this.applConfig.getPmFilesPath(), ev.getFilename());
                    String pmReportJson = Files.readString(path, Charset.defaultCharset());
                    return Mono.just(new DataFromTopic(data.key, pmReportJson));
                }
            }
        } catch (Exception e) {
            return Mono.just(data);
        }
    }

    private String quoteNonJson(String str, Job job) {
        return job.getType().isJson() ? str : quote(str);
    }

    private String quote(String str) {
        final String q = "\"";
        return q + str.replace(q, "\\\"") + q;
    }

    private Mono<String> handleError(Throwable t) {
        logger.warn("exception: {} job: {}", t.getMessage(), job.getId());
        this.errorStats.handleException(t);
        if (this.errorStats.isItHopeless()) {
            return Mono.error(t);
        } else {
            return Mono.empty(); // Ignore
        }
    }

    private void handleSentOk(String data) {
        this.errorStats.handleOkFromConsumer();
    }

}
