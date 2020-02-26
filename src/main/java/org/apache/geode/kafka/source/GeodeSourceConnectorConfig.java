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
package org.apache.geode.kafka.source;

import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.BATCH_SIZE;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.BATCH_SIZE_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.CQS_TO_REGISTER;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.CQS_TO_REGISTER_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.CQ_PREFIX;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.CQ_PREFIX_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_CQ_PREFIX;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_DURABLE_CLIENT_ID;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_LOAD_ENTIRE_REGION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_QUEUE_SIZE;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DEFAULT_REGION_TO_TOPIC_BINDING;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DURABLE_CLIENT_ID_PREFIX;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DURABLE_CLIENT_ID_PREFIX_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DURABLE_CLIENT_TIME_OUT;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.DURABLE_CLIENT_TIME_OUT_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.LOAD_ENTIRE_REGION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.LOAD_ENTIRE_REGION_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.QUEUE_SIZE;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.QUEUE_SIZE_DOCUMENTATION;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.REGION_TO_TOPIC_BINDINGS;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.REGION_TO_TOPIC_BINDINGS_DOCUMENTATION;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import org.apache.geode.kafka.GeodeConnectorConfig;

public class GeodeSourceConnectorConfig extends GeodeConnectorConfig {

  public static final ConfigDef SOURCE_CONFIG_DEF = configurables();


  private final String durableClientId;
  private final String durableClientTimeout;
  private final String cqPrefix;
  private final boolean loadEntireRegion;
  private final int batchSize;
  private final int queueSize;

  private final Map<String, List<String>> regionToTopics;
  private final Collection<String> cqsToRegister;

  public GeodeSourceConnectorConfig(Map<String, String> connectorProperties) {
    super(SOURCE_CONFIG_DEF, connectorProperties);
    cqsToRegister = parseRegionToTopics(getString(CQS_TO_REGISTER)).keySet();
    regionToTopics = parseRegionToTopics(getString(REGION_TO_TOPIC_BINDINGS));
    String durableClientIdPrefix = getString(DURABLE_CLIENT_ID_PREFIX);
    if (isDurable(durableClientIdPrefix)) {
      durableClientId = durableClientIdPrefix + taskId;
    } else {
      durableClientId = "";
    }
    durableClientTimeout = getString(DURABLE_CLIENT_TIME_OUT);
    cqPrefix = getString(CQ_PREFIX);
    loadEntireRegion = getBoolean(LOAD_ENTIRE_REGION);
    batchSize = getInt(BATCH_SIZE);
    queueSize = getInt(QUEUE_SIZE);
  }

  protected static ConfigDef configurables() {
    ConfigDef configDef = GeodeConnectorConfig.configurables();

    configDef.define(
        CQS_TO_REGISTER,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.HIGH,
        CQS_TO_REGISTER_DOCUMENTATION);

    configDef.define(
        REGION_TO_TOPIC_BINDINGS,
        ConfigDef.Type.STRING,
        DEFAULT_REGION_TO_TOPIC_BINDING,
        ConfigDef.Importance.HIGH,
        REGION_TO_TOPIC_BINDINGS_DOCUMENTATION);

    configDef.define(
        DURABLE_CLIENT_ID_PREFIX,
        ConfigDef.Type.STRING,
        DEFAULT_DURABLE_CLIENT_ID,
        ConfigDef.Importance.LOW,
        DURABLE_CLIENT_ID_PREFIX_DOCUMENTATION);

    configDef.define(DURABLE_CLIENT_TIME_OUT,
        ConfigDef.Type.STRING,
        DEFAULT_DURABLE_CLIENT_TIMEOUT,
        ConfigDef.Importance.LOW,
        DURABLE_CLIENT_TIME_OUT_DOCUMENTATION);

    configDef.define(CQ_PREFIX,
        ConfigDef.Type.STRING,
        DEFAULT_CQ_PREFIX,
        ConfigDef.Importance.LOW,
        CQ_PREFIX_DOCUMENTATION);

    configDef.define(
        BATCH_SIZE,
        ConfigDef.Type.INT,
        DEFAULT_BATCH_SIZE,
        ConfigDef.Importance.MEDIUM,
        BATCH_SIZE_DOCUMENTATION);

    configDef.define(
        QUEUE_SIZE,
        ConfigDef.Type.INT,
        DEFAULT_QUEUE_SIZE,
        ConfigDef.Importance.MEDIUM,
        QUEUE_SIZE_DOCUMENTATION);

    configDef.define(LOAD_ENTIRE_REGION,
        ConfigDef.Type.BOOLEAN,
        DEFAULT_LOAD_ENTIRE_REGION,
        ConfigDef.Importance.MEDIUM,
        LOAD_ENTIRE_REGION_DOCUMENTATION);

    return configDef;
  }

  public boolean isDurable() {
    return isDurable(durableClientId);
  }

  /**
   * @param durableClientId or prefix can be passed in. Either both will be "" or both will have a
   *        value
   */
  boolean isDurable(String durableClientId) {
    return !durableClientId.equals("");
  }

  public int getTaskId() {
    return taskId;
  }

  public String getDurableClientId() {
    return durableClientId;
  }

  public String getDurableClientTimeout() {
    return durableClientTimeout;
  }

  public String getCqPrefix() {
    return cqPrefix;
  }

  public boolean getLoadEntireRegion() {
    return loadEntireRegion;
  }

  public Map<String, List<String>> getRegionToTopics() {
    return regionToTopics;
  }

  public Collection<String> getCqsToRegister() {
    return cqsToRegister;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getQueueSize() {
    return queueSize;
  }
}
