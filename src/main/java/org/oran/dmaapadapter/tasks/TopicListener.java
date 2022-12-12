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

import org.apache.kafka.common.header.Header;
import org.oran.dmaapadapter.filter.PmReport;
import reactor.core.publisher.Flux;

public interface TopicListener {

    @ToString
    public static class DataFromTopic {
        public final byte[] key;
        public final byte[] value;

        public final String infoTypeId;

        public final Iterable<Header> headers;

        private static byte[] noBytes = new byte[0];

        @Getter
        @Setter
        @ToString.Exclude
        private PmReport cachedPmReport;

        public DataFromTopic(String typeId, Iterable<Header> headers, byte[] key, byte[] value) {
            this.key = key == null ? noBytes : key;
            this.value = value == null ? noBytes : value;
            this.infoTypeId = typeId;
            this.headers = headers;
        }

        public String valueAsString() {
            return new String(this.value);
        }

        public static final String ZIPPED_PROPERTY = "gzip";
        public static final String TYPE_ID_PROPERTY = "type-id";

        public boolean isZipped() {
            if (headers == null) {
                return false;
            }
            for (Header h : headers) {
                if (h.key().equals(ZIPPED_PROPERTY)) {
                    return true;
                }
            }
            return false;
        }

        public String getTypeIdFromHeaders() {
            if (headers == null) {
                return "";
            }
            for (Header h : headers) {
                if (h.key().equals(TYPE_ID_PROPERTY)) {
                    return new String(h.value());
                }
            }
            return "";
        }
    }

    public Flux<DataFromTopic> getFlux();
}
