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

class PmReportFilterTest {

    @Test
    void testPmFilterMeasTypes() throws Exception {

        String reportJson = loadReport();

        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measTypes.add("succImmediateAssignProcs");

        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filter.filter(reportJson);

        assertThat(filtered).contains("succImmediateAssignProcs").doesNotContain("\"p\":2").contains("\"p\":1")
                .contains("Gbg-997");

        // Test that no report is returned if not meas types were found
        filterData = new PmReportFilter.FilterData();
        filterData.measTypes.add("junk");
        filter = new PmReportFilter(filterData);
        filtered = filter.filter(reportJson);
        assertThat(filtered).isEmpty();
    }

    @Test
    void testMeasObjInstIds() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measObjInstIds.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filter.filter(loadReport());
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.measObjInstIds.add("UtranCell=Gbg-997");
        filter = new PmReportFilter(filterData);
        filtered = filter.filter(loadReport());
        assertThat(filtered).contains("Gbg-997").doesNotContain("Gbg-998");
    }

    @Test
    void testMeasObjClass() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measObjClass.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filter.filter(loadReport());
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.measObjClass.add("ENodeBFunction");
        filter = new PmReportFilter(filterData);
        filtered = filter.filter(loadReport());
        assertThat(filtered).contains("ENodeBFunction").doesNotContain("UtranCell");
    }

    @Test
    void testSourceNames() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.sourceNames.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filter.filter(loadReport());
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.sourceNames.add("O-DU-1122");
        filter = new PmReportFilter(filterData);
        filtered = filter.filter(loadReport());
        assertThat(filtered).contains("O-DU-1122");
    }

    @Test
    void testMeasuredEntityDns() throws Exception {
        PmReportFilter.FilterData filterData = new PmReportFilter.FilterData();
        filterData.measuredEntityDns.add("junk");
        PmReportFilter filter = new PmReportFilter(filterData);
        String filtered = filter.filter(loadReport());
        assertThat(filtered).isEmpty();

        filterData = new PmReportFilter.FilterData();
        filterData.measuredEntityDns.add("ManagedElement=RNC-Gbg-1");
        filter = new PmReportFilter(filterData);
        filtered = filter.filter(loadReport());
        assertThat(filtered).contains("RNC-Gbg-1"); // '=' is escaped to unicode by gson. OK
    }

    @Test
    void testParse() throws Exception {
        com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();
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
