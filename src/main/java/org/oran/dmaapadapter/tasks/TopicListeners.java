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

import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.repository.InfoType;
import org.oran.dmaapadapter.repository.InfoTypes;
import org.oran.dmaapadapter.repository.Jobs;
import org.oran.dmaapadapter.repository.Jobs.JobGroup;
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
            public void onJobbGroupAdded(JobGroup jobGroup) {
                addJob(jobGroup);
            }

            @Override
            public void onJobGroupRemoved(JobGroup jobGroup) {
                removeDistributor(jobGroup);
            }
        });
    }

    public synchronized void addJob(JobGroup jobGroup) {
        removeDistributor(jobGroup);
        logger.debug("Job added {}", jobGroup.getId());
        if (jobGroup.getType().isKafkaTopicDefined()) {
            addDistributor(jobGroup, dataDistributors, kafkaTopicListeners);
        }

        if (jobGroup.getType().isDmaapTopicDefined()) {
            addDistributor(jobGroup, dataDistributors, dmaapTopicListeners);
        }
    }

    private JobDataDistributor createDistributor(JobGroup jobGroup) {
        return jobGroup.getDeliveryInfo() != null ? new KafkaJobDataDistributor(jobGroup, appConfig)
                : new HttpJobDataDistributor(jobGroup, appConfig);
    }

    private void addDistributor(JobGroup jobGroup, MultiMap<JobDataDistributor> distributors,
            Map<String, TopicListener> topicListeners) {
        TopicListener topicListener = topicListeners.get(jobGroup.getType().getId());
        JobDataDistributor distributor = createDistributor(jobGroup);

        distributor.start(topicListener.getFlux());

        distributors.put(jobGroup.getType().getId(), jobGroup.getId(), distributor);
    }

    public synchronized void removeDistributor(JobGroup jobGroup) {
        removeDistributor(jobGroup, dataDistributors);
    }

    private static void removeDistributor(JobGroup jobGroup, MultiMap<JobDataDistributor> distributors) {
        JobDataDistributor distributor = distributors.remove(jobGroup.getType().getId(), jobGroup.getId());
        if (distributor != null) {
            logger.debug("Job removed {}", jobGroup.getId());
            distributor.stop();
        }
    }

}
