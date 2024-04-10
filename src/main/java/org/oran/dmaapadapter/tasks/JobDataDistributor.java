/*-
 * ========================LICENSE_START=================================
 * O-RAN-SC
 * %%
 * Copyright (C) 2021 Nordix Foundation
 * Copyright (C) 2024 OpenInfra Foundation Europe. All rights reserved.
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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.zip.GZIPOutputStream;

import lombok.Getter;

import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.datastore.DataStore;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.filter.PmReportFilter;
import org.oran.dmaapadapter.repository.Job;
import org.oran.dmaapadapter.repository.Jobs.JobGroup;
import org.oran.dmaapadapter.repository.Jobs.JobGroupPm;
import org.oran.dmaapadapter.repository.Jobs.JobGroupSingle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

/**
 * The class streams data from a multi cast sink and sends the data to the Job
 * owner via REST calls.
 */
@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
public abstract class JobDataDistributor {
    private static final Logger logger = LoggerFactory.getLogger(JobDataDistributor.class);

    @Getter
    private final JobGroup jobGroup;
    private Disposable subscription;
    private final ErrorStats errorStats = new ErrorStats();

    private final DataStore dataStore;
    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();
    private final ApplicationConfig applConfig;

    private class ErrorStats {
        @Getter
        private int consumerFaultCounter = 0;

        public void handleOkFromConsumer() {
            this.consumerFaultCounter = 0;
        }

        public void handleException(Throwable t) {
            ++this.consumerFaultCounter;
        }
    }

    protected JobDataDistributor(JobGroup jobGroup, ApplicationConfig applConfig) {
        this.applConfig = applConfig;
        this.jobGroup = jobGroup;
        this.dataStore = DataStore.create(applConfig);
        this.dataStore.create(DataStore.Bucket.FILES).subscribe();
        this.dataStore.create(DataStore.Bucket.LOCKS).subscribe();
    }

    public void start(Flux<TopicListener.DataFromTopic> input) {
        logger.debug("Starting distribution, to topic: {}", jobGroup.getId());
        PmReportFilter filter = getPmReportFilter(this.jobGroup);
        final RetryBackoffSpec retry = Retry.fixedDelay(3, Duration.ofSeconds(5));
        if (filter == null || filter.getFilterData().getPmRopEndTime() == null) {
            this.subscription = filterAndBuffer(input, this.jobGroup) //
                    .doOnNext(filtered -> logger.debug("received data"))
                    .flatMap(filtered -> this.sendToClient(filtered).retryWhen(retry)) //                                                    //
                    .onErrorResume(this::handleError) //
                    .subscribe(this::handleSentOk, //
                            this::handleExceptionInStream, //
                            () -> logger.warn("JobDataDistributor stopped jobId: {}", jobGroup.getId()));
        }

        if (filter != null && filter.getFilterData().getPmRopStartTime() != null) {
            this.dataStore.createLock(collectHistoricalDataLockName()) //
                    .doOnNext(isLockGranted -> {
                        if (isLockGranted.booleanValue()) {
                            logger.debug("Checking historical PM ROP files, jobId: {}", this.jobGroup.getId());
                        } else {
                            logger.debug("Skipping check of historical PM ROP files, already done. jobId: {}",
                                    this.jobGroup.getId());
                        }
                    }) //
                    .filter(isLockGranted -> isLockGranted) //
                    .flatMapMany(b -> Flux.fromIterable(filter.getFilterData().getSourceNames())) //
                    .doOnNext(sourceName -> logger.debug("Checking source name: {}, jobId: {}", sourceName,
                            this.jobGroup.getId())) //
                    .flatMap(sourceName -> dataStore.listObjects(DataStore.Bucket.FILES, sourceName), 1) //
                    .filter(this::isRopFile) //
                    .filter(fileName -> filterStartTime(filter.getFilterData(), fileName)) //
                    .filter(fileName -> filterEndTime(filter.getFilterData(), fileName)) //
                    .map(this::createFakeEvent) //
                    .flatMap(data -> KafkaTopicListener.getDataFromFileIfNewPmFileEvent(data, this.jobGroup.getType(),
                            dataStore), 100)
                    .map(jobGroup::filter) //
                    .map(this::gzip) //
                    .flatMap(filtered -> this.sendToClient(filtered).retryWhen(retry), 1) //
                    .onErrorResume(this::handleCollectHistoricalDataError) //
                    .doFinally(sig -> sendLastStoredRecord()) //
                    .subscribe();
        }
    }

    private void sendLastStoredRecord() {
        String data = "{}";
        Filter.FilteredData output = new Filter.FilteredData(this.jobGroup.getType().getId(), null, data.getBytes());

        sendToClient(output).subscribe();
    }

    private static PmReportFilter getPmReportFilter(JobGroup jobGroup) {

        if (jobGroup instanceof JobGroupPm) {
            return ((JobGroupPm) jobGroup).getFilter();
        } else if (jobGroup instanceof JobGroupSingle) {
            Filter f = ((JobGroupSingle) jobGroup).getJob().getFilter();
            return (f instanceof PmReportFilter) ? (PmReportFilter) f : null;
        }
        return null;
    }

    private Filter.FilteredData gzip(Filter.FilteredData data) {
        if (this.applConfig.isZipOutput()) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(out);
                gzip.write(data.value);
                gzip.flush();
                gzip.close();
                byte[] zipped = out.toByteArray();
                return new Filter.FilteredData(data.infoTypeId, data.key, zipped, true);
            } catch (IOException e) {
                logger.error("Unexpected exception when zipping: {}", e.getMessage());
                return data;
            }
        } else {
            return data;
        }
    }

    private Mono<String> handleCollectHistoricalDataError(Throwable t) {
        logger.error("Exception: {} job: {}", t.getMessage(), jobGroup.getId());
        return tryDeleteLockFile() //
                .map(bool -> "OK");
    }

    private String collectHistoricalDataLockName() {
        return "collectHistoricalDataLock" + this.jobGroup.getId();
    }

    private TopicListener.DataFromTopic createFakeEvent(String fileName) {
        NewFileEvent ev = new NewFileEvent(fileName);
        return new TopicListener.DataFromTopic(this.jobGroup.getType().getId(), null, null, gson.toJson(ev).getBytes());
    }

    private static String fileTimePartFromRopFileName(String fileName) {
        // "O-DU-1122/A20000626.2315+0200-2330+0200_HTTPS-6-73.json"
        return fileName.substring(fileName.lastIndexOf("/") + 2);
    }

    private static boolean filterStartTime(PmReportFilter.FilterData filter, String fileName) {
        try {
            OffsetDateTime fileStartTime = getStartTimeFromFileName(fileName);
            OffsetDateTime startTime = OffsetDateTime.parse(filter.getPmRopStartTime());
            boolean isMatch = fileStartTime.isAfter(startTime);
            logger.debug("Checking file: {}, fileStartTime: {}, filterStartTime: {}, isAfter: {}", fileName,
                    fileStartTime, startTime, isMatch);
            return isMatch;
        } catch (Exception e) {
            logger.warn("Time parsing exception: {}", e.getMessage());
            return false;
        }
    }

    private boolean isRopFile(String fileName) {
        return fileName.endsWith(".json") || fileName.endsWith(".json.gz");
    }

    private static boolean filterEndTime(PmReportFilter.FilterData filter, String fileName) {
        if (filter.getPmRopEndTime() == null) {
            return true;
        }
        try {
            OffsetDateTime fileEndTime = getEndTimeFromFileName(fileName);
            OffsetDateTime endTime = OffsetDateTime.parse(filter.getPmRopEndTime());
            boolean isMatch = fileEndTime.isBefore(endTime);
            logger.debug("Checking file: {}, fileEndTime: {}, endTime: {}, isBefore: {}", fileName, fileEndTime,
                    endTime, isMatch);
            return isMatch;

        } catch (Exception e) {
            logger.warn("Time parsing exception: {}", e.getMessage());
            return false;
        }
    }

    private static OffsetDateTime getStartTimeFromFileName(String fileName) {
        String fileTimePart = fileTimePartFromRopFileName(fileName);
        // A20000626.2315+0200-2330+0200_HTTPS-6-73.json
        fileTimePart = fileTimePart.substring(0, 18);
        return parseFileDate(fileTimePart);
    }

    private static OffsetDateTime getEndTimeFromFileName(String fileName) {
        String fileTimePart = fileTimePartFromRopFileName(fileName);
        // A20000626.2315+0200-2330+0200_HTTPS-6-73.json
        fileTimePart = fileTimePart.substring(0, 9) + fileTimePart.substring(19, 28);
        return parseFileDate(fileTimePart);
    }

    private static OffsetDateTime parseFileDate(String timeStr) {
        DateTimeFormatter startTimeFormatter =
                new DateTimeFormatterBuilder().appendPattern("yyyyMMdd.HHmmZ").toFormatter();
        return OffsetDateTime.parse(timeStr, startTimeFormatter);
    }

    private void handleExceptionInStream(Throwable t) {
        logger.warn("JobDataDistributor exception: {}, jobId: {}", t.getMessage(), jobGroup.getId());
    }

    protected abstract Mono<String> sendToClient(Filter.FilteredData output);

    public synchronized void stop() {
        if (this.subscription != null) {
            logger.debug("Stopped, job: {}", jobGroup.getId());
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

    private Flux<Filter.FilteredData> filterAndBuffer(Flux<TopicListener.DataFromTopic> inputFlux, JobGroup jobGroup) {
        Flux<Filter.FilteredData> filtered = //
                inputFlux //
                        .doOnNext(data -> logger.trace("Received data, job {}", jobGroup.getId())) //
                        .doOnNext(data -> jobGroup.getJobs().forEach(job -> job.getStatistics().received(data.value))) //
                        .map(jobGroup::filter) //
                        .map(this::gzip) //
                        .filter(f -> !f.isEmpty()) //
                        .doOnNext(f -> jobGroup.getJobs().forEach(job -> job.getStatistics().filtered(f.value))) //
                        .doOnNext(data -> logger.trace("Filtered data, job {}", jobGroup.getId())) //
        ; //

        Job job = jobGroup.getJobs().iterator().next();
        if (job.isBuffered()) {
            filtered = filtered.map(input -> quoteNonJson(input.getValueAString(), job)) //
                    .bufferTimeout( //
                            job.getParameters().getBufferTimeout().getMaxSize(), //
                            job.getParameters().getBufferTimeout().getMaxTime()) //
                    .map(buffered -> new Filter.FilteredData(this.getJobGroup().getType().getId(), null,
                            buffered.toString().getBytes()));
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
        logger.warn("exception: {} job: {}", t.getMessage(), jobGroup.getId());
        this.errorStats.handleException(t);
        return Mono.empty(); // Ignore
    }

    private void handleSentOk(String data) {
        this.errorStats.handleOkFromConsumer();
    }

}