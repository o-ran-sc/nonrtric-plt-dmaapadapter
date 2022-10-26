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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.oran.dmaapadapter.filter.Filter.FilteredData;
import org.oran.dmaapadapter.tasks.TopicListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PmReportFilterTest {
    private final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String filterReport(PmReportFilter filter) throws Exception {
        TopicListener.DataFromTopic data = new TopicListener.DataFromTopic(null, loadReport().getBytes());
        FilteredData filtered = filter.filter(data);
        return filtered.getValueAString();
    }

    @Test
    void testPmFilterMeasTypes() throws Exception {

        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measTypes.add("succImmediateAssignProcs");

        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filterReport(filter);

        assertThat(filtered).contains("succImmediateAssignProcs").doesNotContain("\"p\":2").contains("\"p\":1")
                .contains("Gbg-997");

        // Test that no report is returned if not meas types were found
        filterData = new PmReportFilter.FilterData();
        filterData.measTypes.add("junk");
        filter = new PmReportFilter(filterData);
        filtered = filterReport(filter);
        assertThat(filtered).isEmpty();
    }

    @Test
    void testMeasObjInstIds() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measObjInstIds.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filterReport(filter);
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.measObjInstIds.add("UtranCell=Gbg-997");
        filter = new PmReportFilter(filterData);
        filtered = filterReport(filter);
        assertThat(filtered).contains("Gbg-997").doesNotContain("Gbg-998");
    }

    @Test
    void testMeasObjClass() throws Exception {
        {
            PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
            filterData.measObjClass.add("junk");
            PmReportFilter filter = new PmReportFilter(filterData);
            String filtered = filterReport(filter);
            assertThat(filtered).isEmpty();
        }

        {
            TopicListener.DataFromTopic data = new TopicListener.DataFromTopic(null, loadReport().getBytes());

            PmReportFilter.FilterData utranCellFilter = new PmReportFilter.FilterData();
            utranCellFilter.measObjClass.add("UtranCell");
            FilteredData filtered = new PmReportFilter(utranCellFilter).filter(data);
            assertThat(filtered.getValueAString()).contains("UtranCell").doesNotContain("ENodeBFunction");

            PmReportFilter.FilterData eNodeBFilter = new PmReportFilter.FilterData();
            eNodeBFilter.measObjClass.add("ENodeBFunction");
            filtered = new PmReportFilter(eNodeBFilter).filter(data);
            assertThat(filtered.getValueAString()).contains("ENodeBFunction").doesNotContain("UtranCell");
        }
    }

    @Test
    void testSourceNames() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.sourceNames.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filterReport(filter);
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.sourceNames.add("O-DU-1122");
        filter = new PmReportFilter(filterData);
        filtered = filterReport(filter);
        assertThat(filtered).contains("O-DU-1122");
    }

    void testCharacteristics() throws Exception {
        Gson gson = new GsonBuilder() //
                .disableHtmlEscaping() //
                .create(); //

        String path = "./src/test/resources/A20000626.2315+0200-2330+0200_HTTPS-6-73.json";
        String report = Files.readString(Path.of(path), Charset.defaultCharset());

        TopicListener.DataFromTopic data = new TopicListener.DataFromTopic(null, report.getBytes());

        Instant startTime = Instant.now();

        int CNT = 100000;
        for (int i = 0; i < CNT; ++i) {
            gson.fromJson(data.valueAsString(), PmReport.class);
        }
        printDuration("Parse", startTime, CNT);

        startTime = Instant.now();

        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measTypes.add("pmCounterNumber0");
        PmReportFilter filter = new PmReportFilter(filterData);
        for (int i = 0; i < CNT; ++i) {
            filter.filter(data);
        }

        printDuration("Filter", startTime, CNT);
    }

    void printDuration(String str, Instant startTime, int noOfIterations) {
        final long durationSeconds = Instant.now().getEpochSecond() - startTime.getEpochSecond();
        logger.info("*** Duration " + str + " :" + durationSeconds + ", objects/second: "
                + noOfIterations / durationSeconds);
    }

    @Test
    void testMeasuredEntityDns() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measuredEntityDns.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filterReport(filter);
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.measuredEntityDns.add("ManagedElement=RNC-Gbg-1");
        filter = new PmReportFilter(filterData);
        filtered = filterReport(filter);
        assertThat(filtered).contains("ManagedElement=RNC-Gbg-1");
    }

    @Test
    void testCrapInput() {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        PmReportFilter filter = new PmReportFilter(filterData);

        FilteredData filtered = filter.filter(new TopicListener.DataFromTopic(null, "junk".getBytes()));
        assertThat(filtered.isEmpty()).isTrue();

        filtered = filter.filter(new TopicListener.DataFromTopic(null, reQuote("{'msg': 'test'}").getBytes()));
        assertThat(filtered.isEmpty()).isTrue();

    }

    private String reQuote(String str) {
        return str.replaceAll("'", "\\\"");
    }

    @Test
    void testParse() throws Exception {
        com.google.gson.Gson gson = new com.google.gson.GsonBuilder().disableHtmlEscaping().create();
        PmReport report = gson.fromJson(loadReport(), PmReport.class);

        String dn = report.event.perf3gppFields.measDataCollection.measuredEntityDn;
        String json = gson.toJson(report);
        report = gson.fromJson(json, PmReport.class);

        // '=' is escaped to unicode by gson. but converted back
        assertThat(report.event.perf3gppFields.measDataCollection.measuredEntityDn).isEqualTo(dn);
    }

    private String loadReport() throws Exception {
        String path = "./src/test/resources/pm_report.json";
        return Files.readString(Path.of(path), Charset.defaultCharset());
    }

}
