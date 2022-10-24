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

import com.google.gson.annotations.Expose;

import java.util.ArrayList;
import java.util.Collection;

import lombok.Builder;

public class PmReport {

    @Expose
    Event event = new Event();

    public static class CommonEventHeader {
        @Expose
        String domain;

        @Expose
        String eventId;

        @Expose
        int sequence;

        @Expose
        String eventName;

        @Expose
        String sourceName;

        @Expose
        String reportingEntityName;

        @Expose
        String priority;

        @Expose
        long startEpochMicrosec;

        @Expose
        long lastEpochMicrosec;

        @Expose
        String version;

        @Expose
        String vesEventListenerVersion;

        @Expose
        String timeZoneOffset;
    }

    public static class MeasInfoId {
        @Expose
        String sMeasInfoId = "";
    }

    public static class MeasTypes {
        public String getMeasType(int pValue) {
            if (pValue > sMeasTypesList.size()) {
                return "MeasTypeIndexOutOfBounds:" + pValue;
            }
            return sMeasTypesList.get(pValue - 1);
        }

        @Expose
        protected ArrayList<String> sMeasTypesList = new ArrayList<>();
    }

    public static class MeasResult {
        @Expose
        int p;

        @Expose
        String sValue = "";
    }

    public static class MeasValuesList {
        @Expose
        String measObjInstId;

        @Expose
        String suspectFlag;

        @Expose
        Collection<MeasResult> measResults = new ArrayList<>();

        public MeasValuesList shallowClone() {
            MeasValuesList n = new MeasValuesList();
            n.measObjInstId = this.measObjInstId;
            n.suspectFlag = this.suspectFlag;
            return n;
        }
    }

    public static class MeasInfoList {
        @Expose
        MeasInfoId measInfoId;

        @Expose
        MeasTypes measTypes;

        @Expose
        Collection<MeasValuesList> measValuesList = new ArrayList<>();

        public MeasInfoList shallowClone() {
            MeasInfoList n = new MeasInfoList();
            n.measInfoId = this.measInfoId;
            n.measTypes = new MeasTypes();
            return n;
        }
    }

    @Builder(toBuilder = true)
    public static class MeasDataCollection {
        @Expose
        int granularityPeriod;

        @Expose
        String measuredEntityUserName;

        @Expose
        String measuredEntityDn;

        @Expose
        String measuredEntitySoftwareVersion;

        @Expose
        Collection<MeasInfoList> measInfoList;
    }

    @Builder(toBuilder = true)
    public static class Perf3gppFields {
        @Expose
        String perf3gppFieldsVersion;

        @Expose
        MeasDataCollection measDataCollection;
    }

    public static class Event {
        @Expose
        CommonEventHeader commonEventHeader;

        @Expose
        Perf3gppFields perf3gppFields;
    }

}
