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
package geode.kafka.source;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import geode.kafka.GeodeConnectorConfig;

public class GeodeSourceConnectorConfig extends GeodeConnectorConfig {

  // Geode Configuration
  public static final String DURABLE_CLIENT_ID_PREFIX = "durableClientIdPrefix";
  public static final String DEFAULT_DURABLE_CLIENT_ID = "";
  public static final String DURABLE_CLIENT_TIME_OUT = "durableClientTimeout";
  public static final String DEFAULT_DURABLE_CLIENT_TIMEOUT = "60000";

  public static final String CQ_PREFIX = "cqPrefix";
  public static final String DEFAULT_CQ_PREFIX = "cqForGeodeKafka";

  /**
   * Used as a key for source partitions
   */
  public static final String REGION_PARTITION = "regionPartition";
  public static final String REGION_TO_TOPIC_BINDINGS = "regionToTopics";
  public static final String CQS_TO_REGISTER = "cqsToRegister";

  public static final String BATCH_SIZE = "geodeConnectorBatchSize";
  public static final String DEFAULT_BATCH_SIZE = "100";

  public static final String QUEUE_SIZE = "geodeConnectorQueueSize";
  public static final String DEFAULT_QUEUE_SIZE = "10000";

  public static final String LOAD_ENTIRE_REGION = "loadEntireRegion";
  public static final String DEFAULT_LOAD_ENTIRE_REGION = "false";

  private final String durableClientId;
  private final String durableClientIdPrefix;
  private final String durableClientTimeout;
  private final String cqPrefix;
  private final boolean loadEntireRegion;

  private Map<String, List<String>> regionToTopics;
  private Collection<String> cqsToRegister;

  public GeodeSourceConnectorConfig(Map<String, String> connectorProperties) {
    super(connectorProperties);
    cqsToRegister = parseRegionToTopics(connectorProperties.get(CQS_TO_REGISTER)).keySet();
    regionToTopics = parseRegionToTopics(connectorProperties.get(REGION_TO_TOPIC_BINDINGS));
    durableClientIdPrefix = connectorProperties.get(DURABLE_CLIENT_ID_PREFIX);
    if (isDurable(durableClientIdPrefix)) {
      durableClientId = durableClientIdPrefix + taskId;
    } else {
      durableClientId = "";
    }
    durableClientTimeout = connectorProperties.get(DURABLE_CLIENT_TIME_OUT);
    cqPrefix = connectorProperties.get(CQ_PREFIX);
    loadEntireRegion = Boolean.parseBoolean(connectorProperties.get(LOAD_ENTIRE_REGION));
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

}
