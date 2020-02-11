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
package org.geode.kafka.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geode.kafka.GeodeConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import static org.geode.kafka.source.GeodeSourceConnectorConfig.SOURCE_CONFIG_DEF;


public class GeodeKafkaSource extends SourceConnector {

  private Map<String, String> sharedProps;

  @Override
  public Class<? extends Task> taskClass() {
    return GeodeKafkaSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    List<String> bindings =
        GeodeConnectorConfig
            .parseStringByComma(sharedProps.get(GeodeSourceConnectorConfig.REGION_TO_TOPIC_BINDINGS));
    List<List<String>> bindingsPerTask = ConnectorUtils.groupPartitions(bindings, maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> taskProps = new HashMap<>();
      taskProps.putAll(sharedProps);
      taskProps.put(GeodeConnectorConfig.TASK_ID, "" + i);
      taskProps.put(GeodeSourceConnectorConfig.CQS_TO_REGISTER,
          GeodeConnectorConfig.reconstructString(bindingsPerTask.get(i)));
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }


  @Override
  public ConfigDef config() {
    return SOURCE_CONFIG_DEF;
  }

  @Override
  public void start(Map<String, String> props) {
    sharedProps = props;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    // TODO
    return AppInfoParser.getVersion();
  }

  public Map<String, String> getSharedProps() {
    return sharedProps;
  }
}
