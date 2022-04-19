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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegexpFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(RegexpFilter.class);
    private Pattern regexp;

    public RegexpFilter(String exp) {
        try {
            regexp = Pattern.compile(exp);
        } catch (Exception e) {
            logger.warn("Could not parse REGEXP expression: {}, reason: {}", exp, e.getMessage());
        }
    }

    @Override
    public String filter(String data) {
        if (regexp == null) {
            return data;
        }
        Matcher matcher = regexp.matcher(data);
        boolean match = matcher.find();
        if (match) {
            return data;
        } else {
            return "";
        }
    }

}
