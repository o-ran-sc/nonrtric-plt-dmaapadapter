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
import org.springframework.stereotype.Component;

@SuppressWarnings("squid:S2629") // Invoke method(s) only conditionally
@Component
@EnableScheduling
public class TopicListeners {
    private static final Logger logger = LoggerFactory.getLogger(TopicListeners.class);

    private final Map<String, TopicListener> kafkaTopicListeners = new HashMap<>(); // Key is typeId
    private final Map<String, TopicListener> dmaapTopicListeners = new HashMap<>(); // Key is typeId

    @Getter
    private final MultiMap<JobDataDistributor> dataDistributors = new MultiMap<>(); // Key is typeId, jobId

    private final ApplicationConfig appConfig;

    public TopicListeners(@Autowired ApplicationConfig appConfig, @Autowired InfoTypes types, @Autowired Jobs jobs,
            @Autowired SecurityContext securityContext) {
        this.appConfig = appConfig;

        for (InfoType type : types.getAll()) {
            if (type.isKafkaTopicDefined()) {
                KafkaTopicListener topicConsumer = new KafkaTopicListener(appConfig, type,
                        type.getId() + "_" + appConfig.getSelfUrl());
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
            addConsumer(job, dataDistributors, kafkaTopicListeners);
        }

        if (job.getType().isDmaapTopicDefined()) {
            addConsumer(job, dataDistributors, dmaapTopicListeners);
        }
    }

    private JobDataDistributor createConsumer(Job job) {
        return !Strings.isEmpty(job.getParameters().getKafkaOutputTopic()) ? new KafkaJobDataDistributor(job, appConfig)
                : new HttpJobDataDistributor(job);
    }

    private void addConsumer(Job job, MultiMap<JobDataDistributor> distributors,
            Map<String, TopicListener> topicListeners) {
        TopicListener topicListener = topicListeners.get(job.getType().getId());
        JobDataDistributor distributor = createConsumer(job);
        distributor.start(topicListener.getFlux());
        distributors.put(job.getType().getId(), job.getId(), distributor);
    }

    public synchronized void removeJob(Job job) {
        removeJob(job, dataDistributors);
    }

    private static void removeJob(Job job, MultiMap<JobDataDistributor> distributors) {
        JobDataDistributor distributor = distributors.remove(job.getType().getId(), job.getId());
        if (distributor != null) {
            logger.debug("Job removed {}", job.getId());
            distributor.stop();
        }
    }

}
