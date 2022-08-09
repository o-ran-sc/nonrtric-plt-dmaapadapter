/*-
 * ========================LICENSE_START=================================
 * O-RAN-SC
 * %%
 * Copyright (C) 2022 Nordix Foundation
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

package org.oran.dmaapadapter.filter;

import lombok.ToString;

import org.oran.dmaapadapter.tasks.TopicListener.DataFromTopic;

public interface Filter {

    public enum Type {
        REGEXP, JSLT, JSON_PATH, PM_DATA, NONE
    }

    @ToString
    public static class FilteredData {
        public final String key;
        public final String value;

        public FilteredData(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public static FilteredData empty() {
            return new FilteredData("", "");
        }
    }

    public FilteredData filter(DataFromTopic data);

}
