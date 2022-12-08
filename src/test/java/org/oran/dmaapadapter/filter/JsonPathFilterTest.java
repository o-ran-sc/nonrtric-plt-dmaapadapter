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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.oran.dmaapadapter.filter.Filter.FilteredData;
import org.oran.dmaapadapter.tasks.TopicListener.DataFromTopic;

class JsonPathFilterTest {

    @Test
    void testJsonPath() throws Exception {
        String exp = ("$.event.perf3gppFields.measDataCollection.measInfoList[0].measTypes.sMeasTypesList[0]");
        JsonPathFilter filter = new JsonPathFilter(exp);
        DataFromTopic data = new DataFromTopic("typeId", null, null, loadReport().getBytes());
        FilteredData filtered = filter.filter(data);
        String res = filtered.getValueAString();
        assertThat(res).isEqualTo("\"attTCHSeizures\"");
    }

    private String loadReport() throws Exception {
        String path = "./src/test/resources/pm_report.json";
        return Files.readString(Path.of(path), Charset.defaultCharset());
    }

}
