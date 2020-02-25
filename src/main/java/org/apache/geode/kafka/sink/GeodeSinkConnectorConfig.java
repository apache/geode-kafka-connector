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
package org.apache.geode.kafka.sink;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import org.apache.geode.kafka.GeodeConnectorConfig;

public class GeodeSinkConnectorConfig extends GeodeConnectorConfig {
  public static final ConfigDef SINK_CONFIG_DEF = configurables();

  // Used by sink
  public static final String TOPIC_TO_REGION_BINDINGS = "topic-to-regions";
  public static final String DEFAULT_TOPIC_TO_REGION_BINDING = "[gkcTopic:gkcRegion]";
  public static final String NULL_VALUES_MEAN_REMOVE = "null-values-mean-remove";
  public static final String DEFAULT_NULL_VALUES_MEAN_REMOVE = "true";

  private final Map<String, List<String>> topicToRegions;
  private final boolean nullValuesMeanRemove;

  public GeodeSinkConnectorConfig(Map<String, String> connectorProperties) {
    super(SINK_CONFIG_DEF, connectorProperties);
    topicToRegions = parseTopicToRegions(getString(TOPIC_TO_REGION_BINDINGS));
    nullValuesMeanRemove = getBoolean(NULL_VALUES_MEAN_REMOVE);
  }

  protected static ConfigDef configurables() {
    ConfigDef configDef = GeodeConnectorConfig.configurables();
    configDef.define(TOPIC_TO_REGION_BINDINGS, ConfigDef.Type.STRING,
        DEFAULT_TOPIC_TO_REGION_BINDING, ConfigDef.Importance.HIGH,
        "A comma separated list of \"one topic to many regions\" bindings.  Each binding is surrounded by brackets. For example \"[topicName:regionName], [anotherTopic: regionName, anotherRegion]");
    configDef.define(NULL_VALUES_MEAN_REMOVE, ConfigDef.Type.BOOLEAN,
        DEFAULT_NULL_VALUES_MEAN_REMOVE, ConfigDef.Importance.MEDIUM,
        "If set to true, when topics send a SinkRecord with a null value, we will convert to an operation similar to region.remove instead of putting a null value into the region");
    return configDef;
  }

  public Map<String, List<String>> getTopicToRegions() {
    return topicToRegions;
  }

  public boolean getNullValuesMeanRemove() {
    return nullValuesMeanRemove;
  }

}
