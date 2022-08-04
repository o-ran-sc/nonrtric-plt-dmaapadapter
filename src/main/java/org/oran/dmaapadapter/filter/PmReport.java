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

import java.util.ArrayList;
import java.util.Collection;

public class PmReport {

    Event event = new Event();

    public static class CommonEventHeader {
        String domain;
        String eventId;
        int sequence;
        String eventName;
        String sourceName;
        String reportingEntityName;
        String priority;
        long startEpochMicrosec;
        long lastEpochMicrosec;
        String version;
        String vesEventListenerVersion;
        String timeZoneOffset;
    }

    public static class MeasInfoId {
        String sMeasInfoId;
    }

    public static class MeasTypes {
        public String getMeasType(int pValue) {
            if (pValue > sMeasTypesList.size()) {
                return "MeasTypeIndexOutOfBounds:" + pValue;
            }
            return sMeasTypesList.get(pValue - 1);
        }

        protected ArrayList<String> sMeasTypesList = new ArrayList<>();
    }

    public static class MeasResult {
        int p;
        String sValue;
    }

    public static class MeasValuesList {
        String measObjInstId;
        String suspectFlag;
        Collection<MeasResult> measResults = new ArrayList<>();

        public MeasValuesList shallowClone() {
            MeasValuesList n = new MeasValuesList();
            n.measObjInstId = this.measObjInstId;
            n.suspectFlag = this.suspectFlag;
            return n;
        }
    }

    public static class MeasInfoList {
        MeasInfoId measInfoId;
        MeasTypes measTypes;
        Collection<MeasValuesList> measValuesList = new ArrayList<>();

        public MeasInfoList shallowClone() {
            MeasInfoList n = new MeasInfoList();
            n.measInfoId = this.measInfoId;
            n.measTypes = new MeasTypes();
            return n;
        }
    }

    public static class MeasDataCollection {
        int granularityPeriod;
        String measuredEntityUserName;
        String measuredEntityDn;
        String measuredEntitySoftwareVersion;
        Collection<MeasInfoList> measInfoList = new ArrayList<>();
    }

    public static class Perf3gppFields {
        String perf3gppFieldsVersion;
        MeasDataCollection measDataCollection;
    }

    public static class Event {
        CommonEventHeader commonEventHeader;
        Perf3gppFields perf3gppFields;
    }

}
