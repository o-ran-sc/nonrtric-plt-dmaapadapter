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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.zip.GZIPOutputStream;

import lombok.Getter;

import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.datastore.DataStore;
import org.oran.dmaapadapter.exceptions.ServiceException;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.filter.PmReportFilter;
import org.oran.dmaapadapter.repository.Job;
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

    private final DataStore dataStore;
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

    protected JobDataDistributor(Job job, ApplicationConfig applConfig, Flux<TopicListener.DataFromTopic> input) {
        this.job = job;
        this.applConfig = applConfig;
        this.dataStore = DataStore.create(applConfig);
        this.dataStore.create(DataStore.Bucket.FILES).subscribe();
        this.dataStore.create(DataStore.Bucket.LOCKS).subscribe();

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

    public void collectHistoricalData() {
        PmReportFilter filter = job.getFilter() instanceof PmReportFilter ? (PmReportFilter) job.getFilter() : null;

        if (filter != null && filter.getFilterData().getPmRopStartTime() != null) {
            this.dataStore.createLock(collectHistoricalDataLockName()) //
                    .flatMap(isLockGranted -> Boolean.TRUE.equals(isLockGranted) ? Mono.just(isLockGranted)
                            : Mono.error(new LockedException(collectHistoricalDataLockName()))) //
                    .doOnNext(n -> logger.debug("Checking historical PM ROP files, jobId: {}", this.job.getId())) //
                    .doOnError(t -> logger.debug("Skipping check of historical PM ROP files, already done. jobId: {}",
                            this.job.getId())) //
                    .flatMapMany(b -> Flux.fromIterable(filter.getFilterData().getSourceNames())) //
                    .doOnNext(sourceName -> logger.debug("Checking source name: {}, jobId: {}", sourceName,
                            this.job.getId())) //
                    .flatMap(sourceName -> dataStore.listObjects(DataStore.Bucket.FILES, sourceName), 1) //
                    .filter(fileName -> filterStartTime(filter.getFilterData().getPmRopStartTime(), fileName)) //
                    .map(this::createFakeEvent) //
                    .flatMap(data -> KafkaTopicListener.getDataFromFileIfNewPmFileEvent(data, this.job.getType(),
                            dataStore), 100)
                    .map(job::filter) //
                    .map(this::gzip) //
                    .flatMap(this::sendToClient, 1) //
                    .onErrorResume(this::handleCollectHistoricalDataError) //
                    .subscribe();
        }
    }

    private Filter.FilteredData gzip(Filter.FilteredData data) {
        if (job.getParameters().isGzip()) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(out);
                gzip.write(data.value);
                gzip.flush();
                gzip.close();
                byte[] zipped = out.toByteArray();
                return new Filter.FilteredData(data.key, zipped);
            } catch (IOException e) {
                logger.error("Unexpected exception when zipping: {}", e.getMessage());
                return data;
            }
        } else {
            return data;
        }
    }

    private Mono<String> handleCollectHistoricalDataError(Throwable t) {
        if (t instanceof LockedException) {
            logger.debug("Locked exception: {} job: {}", t.getMessage(), job.getId());
            return Mono.empty(); // Ignore
        } else {
            logger.error("Exception: {} job: {}", t.getMessage(), job.getId());
            return tryDeleteLockFile() //
                    .map(bool -> "OK");
        }
    }

    private String collectHistoricalDataLockName() {
        return "collectHistoricalDataLock" + this.job.getId();
    }

    private TopicListener.DataFromTopic createFakeEvent(String fileName) {

        NewFileEvent ev = new NewFileEvent(fileName);

        return new TopicListener.DataFromTopic(null, gson.toJson(ev).getBytes());
    }

    private boolean filterStartTime(String startTimeStr, String fileName) {
        // A20000626.2315+0200-2330+0200_HTTPS-6-73.json
        try {
            if (fileName.endsWith(".json") || fileName.endsWith(".json.gz")) {

                String fileTimePart = fileName.substring(fileName.lastIndexOf("/") + 2);
                fileTimePart = fileTimePart.substring(0, 18);

                DateTimeFormatter formatter =
                        new DateTimeFormatterBuilder().appendPattern("yyyyMMdd.HHmmZ").toFormatter();

                OffsetDateTime fileStartTime = OffsetDateTime.parse(fileTimePart, formatter);
                OffsetDateTime startTime = OffsetDateTime.parse(startTimeStr);
                boolean isBefore = startTime.isBefore(fileStartTime);
                logger.debug("Checking file: {}, fileStartTime: {}, filterStartTime: {}, isBefore: {}", fileName,
                        fileStartTime, startTime, isBefore);
                return isBefore;
            } else {
                return false;
            }

        } catch (Exception e) {
            logger.warn("Time parsing exception: {}", e.getMessage());
            return false;
        }
    }

    private void handleExceptionInStream(Throwable t) {
        logger.warn("JobDataDistributor exception: {}, jobId: {}", t.getMessage(), job.getId());
        stop();
    }

    protected abstract Mono<String> sendToClient(Filter.FilteredData output);

    public synchronized void stop() {
        if (this.subscription != null) {
            this.subscription.dispose();
            this.subscription = null;
        }
        tryDeleteLockFile().subscribe();
    }

    private Mono<Boolean> tryDeleteLockFile() {
        return dataStore.deleteLock(collectHistoricalDataLockName()) //
                .doOnNext(res -> logger.debug("Removed lockfile {} {}", collectHistoricalDataLockName(), res))
                .onErrorResume(t -> Mono.just(false));
    }

    public synchronized boolean isRunning() {
        return this.subscription != null;
    }

    private Flux<Filter.FilteredData> filterAndBuffer(Flux<TopicListener.DataFromTopic> inputFlux, Job job) {
        Flux<Filter.FilteredData> filtered = //
                inputFlux.doOnNext(data -> job.getStatistics().received(data.value)) //
                        .map(job::filter) //
                        .map(this::gzip) //
                        .filter(f -> !f.isEmpty()) //
                        .doOnNext(f -> job.getStatistics().filtered(f.value)); //

        if (job.isBuffered()) {
            filtered = filtered.map(input -> quoteNonJson(input.getValueAString(), job)) //
                    .bufferTimeout( //
                            job.getParameters().getBufferTimeout().getMaxSize(), //
                            job.getParameters().getBufferTimeout().getMaxTime()) //
                    .map(buffered -> new Filter.FilteredData(null, buffered.toString().getBytes()));
        }
        return filtered;
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
