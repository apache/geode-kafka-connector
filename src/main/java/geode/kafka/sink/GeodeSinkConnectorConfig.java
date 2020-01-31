/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package geode.kafka;

import java.util.List;
import java.util.Map;

public class GeodeSinkConnectorConfig extends GeodeConnectorConfig {
    //Used by sink
    public static final String TOPIC_TO_REGION_BINDINGS = "topicToRegions";
    public static final String NULL_VALUES_MEAN_REMOVE = "nullValuesMeanRemove";
    public static final String DEFAULT_NULL_VALUES_MEAN_REMOVE = "true";

    private Map<String, List<String>> topicToRegions;
    private final boolean nullValuesMeanRemove;

    //just for tests
    GeodeSinkConnectorConfig() {
        super();
        nullValuesMeanRemove = Boolean.parseBoolean(DEFAULT_NULL_VALUES_MEAN_REMOVE);
    }

    public GeodeSinkConnectorConfig(Map<String, String> connectorProperties) {
        super(connectorProperties);
       topicToRegions = parseTopicToRegions(connectorProperties.get(TOPIC_TO_REGION_BINDINGS));
       nullValuesMeanRemove = Boolean.parseBoolean(connectorProperties.get(NULL_VALUES_MEAN_REMOVE));
    }

    public Map<String, List<String>> getTopicToRegions() {
        return topicToRegions;
    }

    public boolean getNullValuesMeanRemove() {
        return nullValuesMeanRemove;
    }

}
