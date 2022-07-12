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

package org.oran.dmaapadapter.repository.filters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lombok.Getter;

import org.thymeleaf.util.StringUtils;

public class PmReportFilter implements Filter {

    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();
    private final FilterData filterData;

    @Getter
    public static class FilterData {
        public final Collection<String> sourceNames = new HashSet<>();
        public final Collection<String> measObjInstIds = new ArrayList<>();
        public final Collection<String> measTypes = new HashSet<>();
        public final Collection<String> measuredEntityDns = new ArrayList<>();
    }

    private static class MeasTypesIndexed extends PmReport.MeasTypes {
        private Map<String, Integer> map = new HashMap<>();

        public int addP(String measTypeName) {
            Integer p = map.get(measTypeName);
            if (p != null) {
                return p;
            } else {
                this.sMeasTypesList.add(measTypeName);
                this.map.put(measTypeName, this.sMeasTypesList.size());
                return this.sMeasTypesList.size();
            }
        }
    }

    public PmReportFilter(FilterData filterData) {
        this.filterData = filterData;
    }

    @Override
    public String filter(String data) {
        PmReport report = gson.fromJson(data, PmReport.class);
        if (!filter(report, this.filterData)) {
            return "";
        }
        return gson.toJson(report);

    }

    /**
     * Updates the report based on the filter data.
     *
     * @param report
     * @param filterData
     * @return true if there is anything left in the report
     */
    private boolean filter(PmReport report, FilterData filterData) {
        if (!matchSourceNames(report, filterData.sourceNames)) {
            return false;
        }
        Collection<PmReport.MeasInfoList> filtered = createMeasObjInstIds(report, filterData);
        report.event.perf3gppFields.measDataCollection.measInfoList = filtered;
        return !filtered.isEmpty();
    }

    private boolean isContainedInAny(String aString, Collection<String> collection) {
        for (String s : collection) {
            if (StringUtils.contains(aString, s) == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    private boolean isMeasResultMatch(PmReport.MeasResult measResult, PmReport.MeasTypes measTypes, FilterData filter) {
        String measType = measTypes.getMeasType(measResult.p);
        return filter.measTypes.isEmpty() || filter.measTypes.contains(measType);
    }

    private Collection<PmReport.MeasResult> createMeasResults(Collection<PmReport.MeasResult> oldMeasResults,
            PmReport.MeasTypes measTypes, FilterData filter) {
        Collection<PmReport.MeasResult> newMeasResults = new ArrayList<>();

        for (PmReport.MeasResult measResult : oldMeasResults) {
            if (isMeasResultMatch(measResult, measTypes, filter)) {
                newMeasResults.add(measResult);
            }
        }
        return newMeasResults;
    }

    private PmReport.MeasValuesList createMeasValuesList(PmReport.MeasValuesList oldMeasValues,
            PmReport.MeasTypes measTypes, FilterData filter) {

        PmReport.MeasValuesList newMeasValuesList = oldMeasValues.shallowClone();

        if (filter.measObjInstIds.isEmpty() || isContainedInAny(oldMeasValues.measObjInstId, filter.measObjInstIds)) {
            newMeasValuesList.measResults = createMeasResults(oldMeasValues.measResults, measTypes, filter);
        }
        return newMeasValuesList;
    }

    private PmReport.MeasTypes createMeasTypes(Collection<PmReport.MeasValuesList> measValues,
            PmReport.MeasTypes oldMMeasTypes) {
        MeasTypesIndexed newMeasTypes = new MeasTypesIndexed();
        for (PmReport.MeasValuesList l : measValues) {
            for (PmReport.MeasResult r : l.measResults) {
                String measTypeName = oldMMeasTypes.getMeasType(r.p);
                r.p = newMeasTypes.addP(measTypeName);
            }
        }
        return newMeasTypes;
    }

    private PmReport.MeasInfoList createMeasInfoList(PmReport.MeasInfoList oldMeasInfoList, FilterData filter) {
        PmReport.MeasInfoList newMeasInfoList = oldMeasInfoList.shallowClone();

        for (PmReport.MeasValuesList oldValues : oldMeasInfoList.measValuesList) {
            PmReport.MeasValuesList newMeasValues = createMeasValuesList(oldValues, oldMeasInfoList.measTypes, filter);
            if (!newMeasValues.measResults.isEmpty()) {
                newMeasInfoList.measValuesList.add(newMeasValues);
            }
        }
        newMeasInfoList.measTypes = createMeasTypes(newMeasInfoList.measValuesList, oldMeasInfoList.measTypes);
        return newMeasInfoList;
    }

    private boolean matchMeasuredEntityDns(PmReport report, FilterData filter) {
        return filter.measuredEntityDns.isEmpty() || this.isContainedInAny(
                report.event.perf3gppFields.measDataCollection.measuredEntityDn, filter.measuredEntityDns);
    }

    private Collection<PmReport.MeasInfoList> createMeasObjInstIds(PmReport report, FilterData filter) {
        Collection<PmReport.MeasInfoList> newList = new ArrayList<>();
        if (!matchMeasuredEntityDns(report, filter)) {
            return newList;
        }
        for (PmReport.MeasInfoList oldMeasInfoList : report.event.perf3gppFields.measDataCollection.measInfoList) {
            PmReport.MeasInfoList l = createMeasInfoList(oldMeasInfoList, filter);
            if (!l.measValuesList.isEmpty()) {
                newList.add(l);
            }
        }
        return newList;
    }

    private boolean matchSourceNames(PmReport report, Collection<String> sourceNames) {
        return sourceNames.isEmpty() || sourceNames.contains(report.event.commonEventHeader.sourceName);
    }

}
