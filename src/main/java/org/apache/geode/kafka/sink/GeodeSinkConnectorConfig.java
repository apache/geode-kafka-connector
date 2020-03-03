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

import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.DEFAULT_NULL_VALUES_BEHAVIOR;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.DEFAULT_TOPIC_TO_REGION_BINDING;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.NULL_VALUES_BEHAVIOR;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.NULL_VALUES_BEHAVIOR_DISPLAY_NAME;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.NULL_VALUES_BEHAVIOR_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.SINK_GROUP;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.TOPIC_TO_REGION_BINDINGS;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.TOPIC_TO_REGION_BINDINGS_DISPLAY_NAME;
import static org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants.TOPIC_TO_REGION_BINDINGS_DOCUMENTATION;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import org.apache.geode.kafka.GeodeConnectorConfig;
import org.apache.geode.kafka.utils.EnumValidator;
import org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants;

public class GeodeSinkConnectorConfig extends GeodeConnectorConfig {
  public static final ConfigDef SINK_CONFIG_DEF = configurables();
  private final Map<String, List<String>> topicToRegions;
  private final GeodeSinkConfigurationConstants.NullValueBehavior nullValueBehavior;

  public GeodeSinkConnectorConfig(Map<String, String> connectorProperties) {
    super(SINK_CONFIG_DEF, connectorProperties);
    topicToRegions = parseTopicToRegions(getString(TOPIC_TO_REGION_BINDINGS));
    nullValueBehavior = GeodeSinkConfigurationConstants.NullValueBehavior
        .valueOf(getString(NULL_VALUES_BEHAVIOR).toUpperCase());
  }

  protected static ConfigDef configurables() {
    ConfigDef configDef = GeodeConnectorConfig.configurables();
    configDef.define(
        TOPIC_TO_REGION_BINDINGS,
        ConfigDef.Type.STRING,
        DEFAULT_TOPIC_TO_REGION_BINDING,
        ConfigDef.Importance.HIGH,
        TOPIC_TO_REGION_BINDINGS_DOCUMENTATION,
        SINK_GROUP,
        1,
        ConfigDef.Width.MEDIUM,
        TOPIC_TO_REGION_BINDINGS_DISPLAY_NAME);

    configDef.define(
        NULL_VALUES_BEHAVIOR,
        ConfigDef.Type.STRING,
        DEFAULT_NULL_VALUES_BEHAVIOR,
        EnumValidator.in(GeodeSinkConfigurationConstants.NullValueBehavior.values()),
        ConfigDef.Importance.MEDIUM,
        NULL_VALUES_BEHAVIOR_DOCUMENTATION,
        SINK_GROUP,
        2,
        ConfigDef.Width.MEDIUM,
        NULL_VALUES_BEHAVIOR_DISPLAY_NAME);
    return configDef;
  }

  public Map<String, List<String>> getTopicToRegions() {
    return topicToRegions;
  }

  public boolean getNullValueBehavior() {
    if (nullValueBehavior.equals(GeodeSinkConfigurationConstants.NullValueBehavior.REMOVE)) {
      return true;
    } else {
      return false;
    }
  }

}
