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
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import org.oran.dmaapadapter.filter.PmReport.Event;
import org.oran.dmaapadapter.filter.PmReport.MeasDataCollection;
import org.oran.dmaapadapter.filter.PmReport.MeasInfoList;
import org.oran.dmaapadapter.filter.PmReport.MeasResult;
import org.oran.dmaapadapter.filter.PmReport.MeasTypes;
import org.oran.dmaapadapter.filter.PmReport.MeasValuesList;
import org.oran.dmaapadapter.filter.PmReport.Perf3gppFields;
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
        final Set<String> sourceNames = new HashSet<>();
        final Set<String> measObjInstIds = new HashSet<>();
        final Set<String> measTypes = new HashSet<>();
        final Set<String> measuredEntityDns = new HashSet<>();
        final Set<String> measObjClass = new HashSet<>();

        @Setter
        String pmRopStartTime;

        @Setter
        String pmRopEndTime;

        public void addAll(FilterData other) {
            addAll(other.sourceNames, sourceNames);
            addAll(other.measObjInstIds, measObjInstIds);
            addAll(other.measTypes, measTypes);
            addAll(other.measuredEntityDns, measuredEntityDns);
            addAll(other.measObjClass, measObjClass);
        }

        private void addAll(Set<String> source, Set<String> dst) {
            if (source.isEmpty()) {
                dst.clear();
            } else if (dst.isEmpty()) {
                // Nothing, this means 'match all'
            } else {
                dst.addAll(source);
            }
        }
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
            PmReport report = getPmReport(data);

            if (report.event == null || report.event.getPerf3gppFields() == null) {
                logger.warn("Received PM report with no perf3gppFields, ignored. {}", data);
                return FilteredData.empty();
            }

            PmReport reportFiltered = filter(report, this.filterData);
            if (reportFiltered == null) {
                return FilteredData.empty();
            }
            return new FilteredData(data.infoTypeId, data.key, gson.toJson(reportFiltered).getBytes());
        } catch (Exception e) {
            logger.warn("Could not parse PM data. {}, reason: {}", data, e.getMessage());
            return FilteredData.empty();
        }
    }

    @SuppressWarnings("java:S2445") // "data" is a method parameter, and should not be used for synchronization.
    private PmReport getPmReport(DataFromTopic data) {
        synchronized (data) {
            if (data.getCachedPmReport() == null) {
                data.setCachedPmReport(gsonParse.fromJson(data.valueAsString(), PmReport.class));
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
    private PmReport filter(PmReport report, FilterData filterData) {
        if (!matchSourceNames(report, filterData.sourceNames)) {
            return null;
        }

        Collection<MeasInfoList> filteredMeasObjs = createMeasObjInstIds(report, filterData);
        if (filteredMeasObjs.isEmpty()) {
            return null;
        }
        MeasDataCollection measDataCollection = report.event.getPerf3gppFields().getMeasDataCollection().toBuilder() //
                .measInfoList(filteredMeasObjs) //
                .build();

        Perf3gppFields perf3gppFields =
                report.event.getPerf3gppFields().toBuilder().measDataCollection(measDataCollection) //
                        .build();
        Event event = report.event.toBuilder() //
                .perf3gppFields(perf3gppFields) //
                .build();

        return report.toBuilder() //
                .event(event) //
                .build();
    }

    private boolean isContainedInAny(String aString, Collection<String> collection) {
        for (String s : collection) {
            if (StringUtils.contains(aString, s) == Boolean.TRUE) {
                return true;
            }
        }
        return false;
    }

    private boolean isMeasResultMatch(MeasResult measResult, MeasTypes measTypes, FilterData filter) {
        String measType = measTypes.getMeasType(measResult.getP());
        return filter.measTypes.isEmpty() || filter.measTypes.contains(measType);
    }

    private Collection<MeasResult> createMeasResults(Collection<MeasResult> oldMeasResults, MeasTypes measTypes,
            FilterData filter) {
        Collection<MeasResult> newMeasResults = new ArrayList<>();

        for (MeasResult measResult : oldMeasResults) {
            if (isMeasResultMatch(measResult, measTypes, filter)) {
                newMeasResults.add(measResult.toBuilder().build());
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

    private MeasValuesList createMeasValuesList(MeasValuesList oldMeasValues, MeasTypes measTypes, FilterData filter) {

        if (isMeasInstIdMatch(oldMeasValues.getMeasObjInstId(), filter)
                && isMeasInstClassMatch(oldMeasValues.getMeasObjInstId(), filter)) {

            Collection<MeasResult> newResults = createMeasResults(oldMeasValues.getMeasResults(), measTypes, filter);
            return oldMeasValues.toBuilder() //
                    .measResults(newResults) //
                    .build();
        } else {
            return MeasValuesList.empty();
        }
    }

    private MeasTypes createMeasTypes(Collection<MeasValuesList> newMeasValues, MeasTypes oldMMeasTypes) {
        MeasTypesIndexed newMeasTypes = new MeasTypesIndexed();
        for (MeasValuesList l : newMeasValues) {
            for (MeasResult r : l.getMeasResults()) {
                String measTypeName = oldMMeasTypes.getMeasType(r.getP());
                int newP = newMeasTypes.addP(measTypeName);
                r.setP(newP);
            }
        }
        return newMeasTypes;
    }

    private MeasInfoList createMeasInfoList(MeasInfoList oldMeasInfoList, FilterData filter) {

        Collection<MeasValuesList> measValueLists = new ArrayList<>();
        for (MeasValuesList oldValues : oldMeasInfoList.getMeasValuesList()) {
            MeasValuesList newMeasValues = createMeasValuesList(oldValues, oldMeasInfoList.getMeasTypes(), filter);
            if (!newMeasValues.isEmpty()) {
                measValueLists.add(newMeasValues);
            }
        }

        MeasTypes newMeasTypes = createMeasTypes(measValueLists, oldMeasInfoList.getMeasTypes());

        return oldMeasInfoList.toBuilder() //
                .measTypes(newMeasTypes).measValuesList(measValueLists) //
                .build();

    }

    private boolean matchMeasuredEntityDns(PmReport report, FilterData filter) {
        return filter.measuredEntityDns.isEmpty()
                || this.isContainedInAny(report.event.getPerf3gppFields().getMeasDataCollection().getMeasuredEntityDn(),
                        filter.measuredEntityDns);
    }

    private Collection<MeasInfoList> createMeasObjInstIds(PmReport report, FilterData filter) {
        Collection<MeasInfoList> newList = new ArrayList<>();
        if (!matchMeasuredEntityDns(report, filter)) {
            return newList;
        }
        for (MeasInfoList oldMeasInfoList : report.event.getPerf3gppFields().getMeasDataCollection()
                .getMeasInfoList()) {
            MeasInfoList l = createMeasInfoList(oldMeasInfoList, filter);
            if (!l.getMeasValuesList().isEmpty()) {
                newList.add(l);
            }
        }
        return newList;
    }

    private boolean matchSourceNames(PmReport report, Collection<String> sourceNames) {
        return sourceNames.isEmpty() || sourceNames.contains(report.event.getCommonEventHeader().getSourceName());
    }

}
