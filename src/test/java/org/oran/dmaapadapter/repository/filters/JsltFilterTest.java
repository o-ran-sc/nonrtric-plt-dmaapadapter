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

package org.oran.dmaapadapter.repository.filters;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class JsltFilterTest {

    @Test
    void testPickOneValue() throws Exception {
        String expresssion = "if(.event.commonEventHeader.sourceName == 'O-DU-1122')" //
                + ".event.perf3gppFields.measDataCollection.measInfoList[0].measValuesList[0].measResults[0].sValue";

        JsltFilter filter = new JsltFilter(reQuote(expresssion));
        String res = filter.filter(loadReport());
        assertThat(res).isEqualTo(reQuote("'813'"));
    }

    @Test
    void testPickWholeReport() throws Exception {
        String expresssion = "if(.event.commonEventHeader.sourceName == 'O-DU-1122')" //
                + ".";

        JsltFilter filter = new JsltFilter(reQuote(expresssion));
        String res = filter.filter(loadReport());
        assertThat(res).contains("event");
    }

    @Test
    void testNoMatch() throws Exception {
        String expresssion = "if(.event.commonEventHeader.sourceName == 'JUNK')" //
                + ".";
        JsltFilter filter = new JsltFilter(reQuote(expresssion));
        String res = filter.filter(loadReport());
        assertThat(res).isEmpty();
    }

    @Test
    void testMoreAdvanced() throws Exception {

        String expresssion = //
                "if(.event.commonEventHeader.sourceName == 'O-DU-1122')" + //
                        "{ " + //
                        "'array' : [for (.event.perf3gppFields.measDataCollection.measInfoList[0].measValuesList) string(.measObjInstId)], "
                        + //
                        "'size'  : size(.event.perf3gppFields.measDataCollection.measInfoList[0].measValuesList)" + //
                        "}"; //

        JsltFilter filter = new JsltFilter(reQuote(expresssion));
        String res = filter.filter(loadReport());
        String expected =
                "{'array':['RncFunction=RF-1,UtranCell=Gbg-997','RncFunction=RF-1,UtranCell=Gbg-998','RncFunction=RF-1,UtranCell=Gbg-999'],'size':3}";

        assertThat(res).isEqualTo(reQuote(expected));

    }

    private String loadReport() throws Exception {
        String path = "./src/test/resources/pm_report.json";
        return Files.readString(Path.of(path), Charset.defaultCharset());
    }

    private String reQuote(String str) {
        return str.replaceAll("'", "\\\"");
    }

}
