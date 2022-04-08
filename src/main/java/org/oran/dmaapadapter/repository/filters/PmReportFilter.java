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
import java.util.Map;

import lombok.Getter;

import org.immutables.gson.Gson;
import org.thymeleaf.util.StringUtils;

public class PmReportFilter implements Filter {

    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();
    private final FilterData filterData;

    @Gson.TypeAdapters
    @Getter
    public static class FilterData {
        Collection<String> sourceNames = new ArrayList<>();
        Collection<String> measObjInstIds = new ArrayList<>();
        Collection<String> measTypes = new ArrayList<>();
        Collection<String> measuredEntityDns = new ArrayList<>();
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

    private boolean filter(PmReport report, FilterData filterData) {
        return (matchSourceNames(report, filterData.sourceNames) && matchMeasObjInstIds(report, filterData));
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

        if (isContainedInAny(oldMeasValues.measObjInstId, filter.measObjInstIds) || filter.measObjInstIds.isEmpty()) {
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

    private boolean matchMeasObjInstIds(PmReport report, FilterData filter) {
        if (!matchMeasuredEntityDns(report, filter)) {
            return false;
        }

        Collection<PmReport.MeasInfoList> oldList = report.event.perf3gppFields.measDataCollection.measInfoList;
        Collection<PmReport.MeasInfoList> newList = new ArrayList<>();
        report.event.perf3gppFields.measDataCollection.measInfoList = newList;
        for (PmReport.MeasInfoList oldMeasInfoList : oldList) {
            PmReport.MeasInfoList l = createMeasInfoList(oldMeasInfoList, filter);
            if (!l.measValuesList.isEmpty()) {
                newList.add(l);
            }
        }

        return !newList.isEmpty();
    }

    private boolean matchSourceNames(PmReport report, Collection<String> sourceNames) {
        return sourceNames.isEmpty() || sourceNames.contains(report.event.commonEventHeader.sourceName);
    }

}
