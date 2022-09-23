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

package org.oran.dmaapadapter.filter;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

import org.oran.dmaapadapter.tasks.TopicListener.DataFromTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thymeleaf.util.StringUtils;

public class PmReportFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static com.google.gson.Gson gson = new com.google.gson.GsonBuilder() //
            .disableHtmlEscaping() //
            .excludeFieldsWithoutExposeAnnotation() //
            .create();

    // excludeFieldsWithoutExposeAnnotation is not needed when parsing and this is a
    // bit quicker
    private static com.google.gson.Gson gsonParse = new com.google.gson.GsonBuilder() //
            .disableHtmlEscaping() //
            .create();

    @Getter
    private final FilterData filterData;

    @Getter
    public static class FilterData {
        final Collection<String> sourceNames = new HashSet<>();
        final Collection<String> measObjInstIds = new ArrayList<>();
        final Collection<String> measTypes = new HashSet<>();
        final Collection<String> measuredEntityDns = new ArrayList<>();
        final Collection<String> measObjClass = new HashSet<>();

        @Setter
        String pmRopStartTime;
    }

    private static class MeasTypesIndexed extends PmReport.MeasTypes {

        private Map<String, Integer> map = new HashMap<>();

        public int addP(String measTypeName) {
            Integer p = map.get(measTypeName);
            if (p != null) {
                return p;
            } else {
                sMeasTypesList.add(measTypeName);
                this.map.put(measTypeName, sMeasTypesList.size());
                return sMeasTypesList.size();
            }
        }
    }

    public PmReportFilter(FilterData filterData) {
        this.filterData = filterData;
    }

    @Override
    public FilteredData filter(DataFromTopic data) {
        try {
            PmReport report = createPmReport(data);
            if (report.event.perf3gppFields == null) {
                logger.warn("Received PM report with no perf3gppFields, ignored. {}", data);
                return FilteredData.empty();
            }

            if (!filter(report, this.filterData)) {
                return FilteredData.empty();
            }
            return new FilteredData(data.key, gson.toJson(report));
        } catch (Exception e) {
            logger.warn("Could not parse PM data. {}, reason: {}", data, e.getMessage());
            return FilteredData.empty();
        }
    }

    @SuppressWarnings("java:S2445") // "data" is a method parameter, and should not be used for synchronization.
    private PmReport createPmReport(DataFromTopic data) {
        synchronized (data) {
            if (data.getCachedPmReport() == null) {
                data.setCachedPmReport(gsonParse.fromJson(data.value, PmReport.class));
            }
            return data.getCachedPmReport();
        }
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

    private boolean isMeasInstIdMatch(String measObjInstId, FilterData filter) {
        return filter.measObjInstIds.isEmpty() || isContainedInAny(measObjInstId, filter.measObjInstIds);
    }

    private String managedObjectClass(String distinguishedName) {
        int lastRdn = distinguishedName.lastIndexOf(",");
        if (lastRdn == -1) {
            return "";
        }
        int lastEqualChar = distinguishedName.indexOf("=", lastRdn);
        if (lastEqualChar == -1) {
            return "";
        }
        return distinguishedName.substring(lastRdn + 1, lastEqualChar);
    }

    private boolean isMeasInstClassMatch(String measObjInstId, FilterData filter) {
        if (filter.measObjClass.isEmpty()) {
            return true;
        }

        String measObjClass = managedObjectClass(measObjInstId);
        return filter.measObjClass.contains(measObjClass);
    }

    private PmReport.MeasValuesList createMeasValuesList(PmReport.MeasValuesList oldMeasValues,
            PmReport.MeasTypes measTypes, FilterData filter) {

        PmReport.MeasValuesList newMeasValuesList = oldMeasValues.shallowClone();

        if (isMeasInstIdMatch(oldMeasValues.measObjInstId, filter)
                && isMeasInstClassMatch(oldMeasValues.measObjInstId, filter)) {
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
