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

package org.oran.dmaapadapter.r1;

import com.google.gson.annotations.SerializedName;


public class FileReadyJobData {

    public static class Filter {
        @SerializedName("inputCompression")
        public String inputCompression = "xml.gz";

        @SerializedName("outputCompression")
        public String outputCompression = "none";
    }

    @SerializedName("kafkaOutputTopic")
    public String kafkaOutputTopic = "";

    @SerializedName("filterType")
    public String filterType = "pmdata";

    @SerializedName("filter")
    public Filter filter = new Filter();

    public FileReadyJobData(String kafkaOutputTopic) {
        this.kafkaOutputTopic = kafkaOutputTopic;
    }

}
