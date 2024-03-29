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

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import org.oran.dmaapadapter.configuration.ApplicationConfig;
import org.springframework.util.StringUtils;

@ToString
@Builder
public class InfoType {

    @Getter
    private String id;

    @Getter
    private String dmaapTopicUrl;

    @Getter
    @Builder.Default
    private boolean useHttpProxy = false;

    @Getter
    private String kafkaInputTopic;

    @Getter
    private String inputJobType;

    @Getter
    private Object inputJobDefinition;

    private String dataType;

    @Builder.Default
    private boolean isJson = false;

    public boolean isJson() {
        return this.isJson || getDataType() == DataType.PM_DATA;
    }

    public boolean isKafkaTopicDefined() {
        return StringUtils.hasLength(kafkaInputTopic);
    }

    public boolean isDmaapTopicDefined() {
        return StringUtils.hasLength(dmaapTopicUrl);
    }

    public enum DataType {
        PM_DATA, OTHER
    }

    public DataType getDataType() {
        if (dataType == null) {
            return DataType.OTHER;
        }

        if (dataType.equalsIgnoreCase("pmData")) {
            return DataType.PM_DATA;
        }
        return DataType.OTHER;
    }

    public String getKafkaGroupId() {
        return this.kafkaInputTopic == null ? null : "osc-dmaap-adapter-" + getId();
    }

    public String getKafkaClientId(ApplicationConfig appConfig) {
        return this.kafkaInputTopic == null ? null : getId() + "_" + appConfig.getSelfUrl();

    }
}
