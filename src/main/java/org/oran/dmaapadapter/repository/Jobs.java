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

import lombok.Getter;

import org.oran.dmaapadapter.clients.AsyncRestClient;
import org.oran.dmaapadapter.clients.AsyncRestClientFactory;
import org.oran.dmaapadapter.clients.SecurityContext;
import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.oran.dmaapadapter.exceptions.ServiceException;
import org.oran.dmaapadapter.filter.Filter;
import org.oran.dmaapadapter.filter.FilterFactory;
import org.oran.dmaapadapter.filter.PmReportFilter;
import org.oran.dmaapadapter.repository.Job.Parameters;
import org.oran.dmaapadapter.tasks.TopicListener.DataFromTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class Jobs {
    public interface Observer {
        void onJobbGroupAdded(JobGroup jobGroup);

        void onJobGroupRemoved(JobGroup jobGroup);
    }

    public interface JobGroup {
        public String getId();

        public InfoType getType();

        public void remove(Job job);

        public boolean isEmpty();

        public Filter.FilteredData filter(DataFromTopic data);

        public Iterable<Job> getJobs();

        public String getTopic();
    }

    public static class JobGroupSingle implements JobGroup {
        @Getter
        private final Job job;
        private boolean isJobRemoved = false;

        public JobGroupSingle(Job job) {
            this.job = job;
        }

        @Override
        public Filter.FilteredData filter(DataFromTopic data) {
            return job.filter(data);
        }

        @Override
        public void remove(Job job) {
            this.isJobRemoved = true;
        }

        @Override
        public boolean isEmpty() {
            return isJobRemoved;
        }

        @Override
        public String getId() {
            return job.getId();
        }

        @Override
        public InfoType getType() {
            return job.getType();
        }

        @Override
        public Iterable<Job> getJobs() {
            Collection<Job> c = new ArrayList<>();
            c.add(job);
            return c;
        }

        @Override
        public String getTopic() {
            return this.job.getParameters().getKafkaOutputTopic();
        }
    }

    public static class JobGroupPm implements JobGroup {
        @Getter
        private final String topic;

        private Map<String, Job> jobs = new HashMap<>();

        @Getter
        private PmReportFilter filter;

        @Getter
        private final InfoType type;

        public JobGroupPm(InfoType type, String topic) {
            this.topic = topic;
            this.type = type;
        }

        public synchronized void add(Job job) {
            this.jobs.put(job.getId(), job);
            this.filter = createFilter();
        }

        public synchronized void remove(Job job) {
            this.jobs.remove(job.getId());
            if (!this.jobs.isEmpty()) {
                this.filter = createFilter();
            }
        }

        public boolean isEmpty() {
            return jobs.isEmpty();
        }

        @Override
        public Filter.FilteredData filter(DataFromTopic data) {
            return filter.filter(data);
        }

        public Job getAJob() {
            if (this.jobs.isEmpty()) {
                return null;
            }
            return this.jobs.values().iterator().next();
        }

        private PmReportFilter createFilter() {
            Collection<PmReportFilter> filterData = new ArrayList<>();
            this.jobs.forEach((key, value) -> filterData.add((PmReportFilter) value.getFilter()));
            return FilterFactory.createAggregateFilter(filterData);
        }

        @Override
        public String getId() {
            return topic;
        }

        @Override
        public Iterable<Job> getJobs() {
            return this.jobs.values();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Jobs.class);

    private Map<String, Job> allJobs = new HashMap<>();
    private MultiMap<Job> jobsByType = new MultiMap<>();
    private Map<String, JobGroup> jobGroups = new HashMap<>();
    private final AsyncRestClientFactory restclientFactory;
    private final List<Observer> observers = new ArrayList<>();
    private final ApplicationConfig appConfig;

    public Jobs(@Autowired ApplicationConfig applicationConfig, @Autowired SecurityContext securityContext,
            @Autowired ApplicationConfig appConfig) {
        restclientFactory = new AsyncRestClientFactory(applicationConfig.getWebClientConfig(), securityContext);
        this.appConfig = appConfig;
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
        Job job = new Job(id, callbackUrl, type, owner, lastUpdated, parameters, consumerRestClient, this.appConfig);
        this.put(job);
    }

    public void addObserver(Observer obs) {
        synchronized (observers) {
            this.observers.add(obs);
        }
    }

    private String jobGroupId(Job job) {
        if (Strings.isNullOrEmpty(job.getParameters().getKafkaOutputTopic())) {
            return job.getId();
        } else if (job.getParameters().getFilterType() == Filter.Type.PM_DATA) {
            return job.getParameters().getKafkaOutputTopic();
        } else {
            return job.getId();
        }
    }

    private synchronized void put(Job job) {
        logger.debug("Put job: {}", job.getId());
        remove(job.getId());

        allJobs.put(job.getId(), job);
        jobsByType.put(job.getType().getId(), job.getId(), job);

        if (job.getParameters().getFilterType() == Filter.Type.PM_DATA
                && job.getParameters().getKafkaOutputTopic() != null) {
            String topic = job.getParameters().getKafkaOutputTopic();
            if (!this.jobGroups.containsKey(topic)) {
                final JobGroupPm group = new JobGroupPm(job.getType(), topic);
                this.jobGroups.put(topic, group);
                group.add(job);
                this.observers.forEach(obs -> obs.onJobbGroupAdded(group));
            } else {
                JobGroupPm group = (JobGroupPm) this.jobGroups.get(topic);
                group.add(job);
            }
        } else {
            JobGroupSingle group = new JobGroupSingle(job);
            this.jobGroups.put(job.getId(), group);
            this.observers.forEach(obs -> obs.onJobbGroupAdded(group));
        }
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
        String groupId = this.jobGroupId(job);
        JobGroup group = this.jobGroups.get(groupId);
        synchronized (this) {
            this.allJobs.remove(job.getId());
            jobsByType.remove(job.getType().getId(), job.getId());
            group.remove(job);
            if (group.isEmpty()) {
                this.jobGroups.remove(groupId);
            }
        }

        if (group.isEmpty()) {
            this.observers.forEach(obs -> obs.onJobGroupRemoved(group));
        }
    }

    public synchronized int size() {
        return allJobs.size();
    }

    public synchronized Collection<Job> getJobsForType(InfoType type) {
        return jobsByType.get(type.getId());
    }

    public void clear() {

        this.jobGroups.forEach((id, group) -> this.observers.forEach(obs -> obs.onJobGroupRemoved(group)));

        synchronized (this) {
            allJobs.clear();
            jobsByType.clear();
        }
    }
}
