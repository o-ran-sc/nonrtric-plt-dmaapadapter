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

package org.oran.dmaapadapter.tasks;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.oran.dmaapadapter.filter.PmReport;
import reactor.core.publisher.Flux;

public interface TopicListener {

    @ToString
    public static class DataFromTopic {
        public final byte[] key;
        public final byte[] value;

        private static byte[] noBytes = new byte[0];

        @Getter
        @Setter
        @ToString.Exclude
        private PmReport cachedPmReport;

        public DataFromTopic(byte[] key, byte[] value) {
            this.key = key == null ? noBytes : key;
            this.value = value == null ? noBytes : value;
        }

        public String valueAsString() {
            return new String(this.value);
        }

    }

    public Flux<DataFromTopic> getFlux();
}
