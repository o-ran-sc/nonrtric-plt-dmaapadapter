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

package org.oran.dmaapadapter.repository;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.exceptions.ServiceException;
import org.oran.dmaapadapter.repository.Job.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class Jobs {
    public interface Observer {
        void onJobbAdded(Job job);

        void onJobRemoved(Job job);
    }

    private static final Logger logger = LoggerFactory.getLogger(Jobs.class);

    private Map<String, Job> allJobs = new HashMap<>();
    private MultiMap<Job> jobsByType = new MultiMap<>();
    private final AsyncRestClientFactory restclientFactory;
    private final List<Observer> observers = new ArrayList<>();

    public Jobs(@Autowired ApplicationConfig applicationConfig, @Autowired SecurityContext securityContext) {
        restclientFactory = new AsyncRestClientFactory(applicationConfig.getWebClientConfig(), securityContext);
    }

    public synchronized Job getJob(String id) throws ServiceException {
        Job job = allJobs.get(id);
        if (job == null) {
            throw new ServiceException("Could not find job: " + id, HttpStatus.NOT_FOUND);
        }
        return job;
    }

    public synchronized Job get(String id) {
        return allJobs.get(id);
    }

    public void addJob(String id, String callbackUrl, InfoType type, String owner, String lastUpdated,
            Parameters parameters) throws ServiceException {

        if (!Strings.isNullOrEmpty(parameters.getKafkaOutputTopic()) && !Strings.isNullOrEmpty(callbackUrl)) {
            throw new ServiceException("Cannot deliver to both Kafka and HTTP in the same job", HttpStatus.BAD_REQUEST);
        }
        AsyncRestClient consumerRestClient = type.isUseHttpProxy() //
                ? restclientFactory.createRestClientUseHttpProxy(callbackUrl) //
                : restclientFactory.createRestClientNoHttpProxy(callbackUrl);
        Job job = new Job(id, callbackUrl, type, owner, lastUpdated, parameters, consumerRestClient);
        this.put(job);
        synchronized (observers) {
            this.observers.forEach(obs -> obs.onJobbAdded(job));
        }
    }

    public void addObserver(Observer obs) {
        synchronized (observers) {
            this.observers.add(obs);
        }
    }

    private synchronized void put(Job job) {
        logger.debug("Put job: {}", job.getId());
        allJobs.put(job.getId(), job);
        jobsByType.put(job.getType().getId(), job.getId(), job);
    }

    public synchronized Iterable<Job> getAll() {
        return new Vector<>(allJobs.values());
    }

    public synchronized Job remove(String id) {
        Job job = allJobs.get(id);
        if (job != null) {
            remove(job);
        }
        return job;
    }

    public void remove(Job job) {
        synchronized (this) {
            this.allJobs.remove(job.getId());
            jobsByType.remove(job.getType().getId(), job.getId());
        }
        notifyJobRemoved(job);
    }

    private synchronized void notifyJobRemoved(Job job) {
        this.observers.forEach(obs -> obs.onJobRemoved(job));
    }

    public synchronized int size() {
        return allJobs.size();
    }

    public synchronized Collection<Job> getJobsForType(InfoType type) {
        return jobsByType.get(type.getId());
    }

    public void clear() {

        this.allJobs.forEach((id, job) -> notifyJobRemoved(job));

        synchronized (this) {
            allJobs.clear();
            jobsByType.clear();
        }
    }
}
