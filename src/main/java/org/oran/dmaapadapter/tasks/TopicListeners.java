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

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

import org.apache.logging.log4j.util.Strings;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.InfoType;
import org.oran.dmaapadapter.repository.InfoTypes;
import org.oran.dmaapadapter.repository.Job;
import org.oran.dmaapadapter.repository.Jobs;
import org.oran.dmaapadapter.repository.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
@Component
@EnableScheduling
public class TopicListeners {
    private static final Logger logger = LoggerFactory.getLogger(TopicListeners.class);

    private final Map<String, TopicListener> kafkaTopicListeners = new HashMap<>(); // Key is typeId
    private final Map<String, TopicListener> dmaapTopicListeners = new HashMap<>(); // Key is typeId

    @Getter
    private final MultiMap<DataConsumer> dataConsumers = new MultiMap<>(); // Key is typeId, jobId

    private final ApplicationConfig appConfig;

    private static final int CONSUMER_SUPERVISION_INTERVAL_MS = 1000 * 60 * 3;

    public TopicListeners(@Autowired ApplicationConfig appConfig, @Autowired InfoTypes types, @Autowired Jobs jobs,
            @Autowired SecurityContext securityContext) {
        this.appConfig = appConfig;

        for (InfoType type : types.getAll()) {
            if (type.isKafkaTopicDefined()) {
                KafkaTopicListener topicConsumer = new KafkaTopicListener(appConfig, type);
                kafkaTopicListeners.put(type.getId(), topicConsumer);
            }
            if (type.isDmaapTopicDefined()) {
                DmaapTopicListener topicListener = new DmaapTopicListener(appConfig, type, securityContext);
                dmaapTopicListeners.put(type.getId(), topicListener);
            }
        }

        jobs.addObserver(new Jobs.Observer() {
            @Override
            public void onJobbAdded(Job job) {
                addJob(job);
            }

            @Override
            public void onJobRemoved(Job job) {
                removeJob(job);
            }
        });
    }

    public synchronized void addJob(Job job) {
        removeJob(job);
        logger.debug("Job added {}", job.getId());
        if (job.getType().isKafkaTopicDefined()) {
            addConsumer(job, dataConsumers, kafkaTopicListeners);
        }

        if (job.getType().isDmaapTopicDefined()) {
            addConsumer(job, dataConsumers, dmaapTopicListeners);
        }
    }

    private DataConsumer createConsumer(Job job) {
        return !Strings.isEmpty(job.getParameters().getKafkaOutputTopic()) ? new KafkaDataConsumer(job, appConfig)
                : new HttpDataConsumer(job);
    }

    private void addConsumer(Job job, MultiMap<DataConsumer> consumers, Map<String, TopicListener> topicListeners) {
        TopicListener topicListener = topicListeners.get(job.getType().getId());
        if (consumers.get(job.getType().getId()).isEmpty()) {
            topicListener.start();
        }
        DataConsumer consumer = createConsumer(job);
        consumer.start(topicListener.getOutput().asFlux());
        consumers.put(job.getType().getId(), job.getId(), consumer);
    }

    public synchronized void removeJob(Job job) {
        removeJob(job, dataConsumers);
    }

    private static void removeJob(Job job, MultiMap<DataConsumer> consumers) {
        DataConsumer consumer = consumers.remove(job.getType().getId(), job.getId());
        if (consumer != null) {
            logger.debug("Job removed {}", job.getId());
            consumer.stop();
        }
    }

    @Scheduled(fixedRate = CONSUMER_SUPERVISION_INTERVAL_MS)
    public synchronized void restartNonRunningKafkaTopics() {
        for (DataConsumer consumer : this.dataConsumers.values()) {
            if (!consumer.isRunning()) {
                restartTopicAndConsumers(this.kafkaTopicListeners, this.dataConsumers, consumer);
            }
        }

    }

    private static void restartTopicAndConsumers(Map<String, TopicListener> topicListeners,
            MultiMap<DataConsumer> consumers, DataConsumer consumer) {
        InfoType type = consumer.getJob().getType();
        TopicListener topic = topicListeners.get(type.getId());
        topic.start();
        restartConsumersOfType(consumers, topic, type);
    }

    private static void restartConsumersOfType(MultiMap<DataConsumer> consumers, TopicListener topic, InfoType type) {
        consumers.get(type.getId()).forEach(consumer -> consumer.start(topic.getOutput().asFlux()));
    }
}
