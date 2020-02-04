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

import geode.kafka.GeodeConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static geode.kafka.GeodeConnectorConfig.DEFAULT_LOCATOR;
import static geode.kafka.GeodeConnectorConfig.LOCATORS;
import static geode.kafka.source.GeodeSourceConnectorConfig.BATCH_SIZE;
import static geode.kafka.source.GeodeSourceConnectorConfig.CQS_TO_REGISTER;
import static geode.kafka.source.GeodeSourceConnectorConfig.CQ_PREFIX;
import static geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_BATCH_SIZE;
import static geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_CQ_PREFIX;
import static geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_DURABLE_CLIENT_ID;
import static geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT;
import static geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_LOAD_ENTIRE_REGION;
import static geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_QUEUE_SIZE;
import static geode.kafka.source.GeodeSourceConnectorConfig.DURABLE_CLIENT_ID_PREFIX;
import static geode.kafka.source.GeodeSourceConnectorConfig.DURABLE_CLIENT_TIME_OUT;
import static geode.kafka.source.GeodeSourceConnectorConfig.LOAD_ENTIRE_REGION;
import static geode.kafka.source.GeodeSourceConnectorConfig.QUEUE_SIZE;
import static geode.kafka.source.GeodeSourceConnectorConfig.REGION_TO_TOPIC_BINDINGS;


public class GeodeKafkaSource extends SourceConnector {

  private Map<String, String> sharedProps;
  //TODO maybe club this into GeodeConnnectorConfig
  private static final ConfigDef CONFIG_DEF = new ConfigDef();


  @Override
  public Class<? extends Task> taskClass() {
    return GeodeKafkaSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    List<String> bindings = GeodeConnectorConfig.parseStringByComma(sharedProps.get(REGION_TO_TOPIC_BINDINGS));
    List<List<String>> bindingsPerTask = ConnectorUtils.groupPartitions(bindings, maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> taskProps = new HashMap<>();
      taskProps.putAll(sharedProps);
      taskProps.put(GeodeConnectorConfig.TASK_ID, "" + i);
      taskProps.put(CQS_TO_REGISTER, GeodeConnectorConfig.reconstructString(bindingsPerTask.get(i)));
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }


  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void start(Map<String, String> props) {
    sharedProps = computeMissingConfigurations(props);
  }

  private Map<String, String> computeMissingConfigurations(Map<String, String> props) {
    props.computeIfAbsent(LOCATORS, (key)-> DEFAULT_LOCATOR);
    props.computeIfAbsent(DURABLE_CLIENT_TIME_OUT, (key) -> DEFAULT_DURABLE_CLIENT_TIMEOUT);
    props.computeIfAbsent(DURABLE_CLIENT_ID_PREFIX, (key) -> DEFAULT_DURABLE_CLIENT_ID);
    props.computeIfAbsent(BATCH_SIZE, (key) -> DEFAULT_BATCH_SIZE);
    props.computeIfAbsent(QUEUE_SIZE, (key) -> DEFAULT_QUEUE_SIZE);
    props.computeIfAbsent(CQ_PREFIX, (key) -> DEFAULT_CQ_PREFIX);
    props.computeIfAbsent(LOAD_ENTIRE_REGION, (key) -> DEFAULT_LOAD_ENTIRE_REGION);
    return props;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    //TODO
    return AppInfoParser.getVersion();
  }

  public Map<String, String> getSharedProps() {
    return sharedProps;
  }
}
