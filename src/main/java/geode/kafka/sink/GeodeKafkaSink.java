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
package geode.kafka.sink;

import geode.kafka.GeodeConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static geode.kafka.GeodeConnectorConfig.DEFAULT_LOCATOR;
import static geode.kafka.GeodeConnectorConfig.LOCATORS;
import static geode.kafka.GeodeSinkConnectorConfig.DEFAULT_NULL_VALUES_MEAN_REMOVE;
import static geode.kafka.GeodeSinkConnectorConfig.NULL_VALUES_MEAN_REMOVE;
import static geode.kafka.GeodeSinkConnectorConfig.TOPIC_TO_REGION_BINDINGS;

public class GeodeKafkaSink extends SinkConnector  {
    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private Map<String, String> sharedProps;

    @Override
    public void start(Map<String, String> props) {
        sharedProps = computeMissingConfigurations(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GeodeKafkaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(sharedProps);
        List<String> bindings = GeodeConnectorConfig.parseStringByComma(taskProps.get(TOPIC_TO_REGION_BINDINGS));
        List<List<String>> bindingsPerTask = ConnectorUtils.groupPartitions(bindings, maxTasks);

        for (int i = 0; i < maxTasks; i++) {
            taskProps.put(GeodeConnectorConfig.TASK_ID, "" + i);
            taskProps.put(TOPIC_TO_REGION_BINDINGS, GeodeConnectorConfig.reconstructString(bindingsPerTask.get(i)));
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        //TODO
        return "unknown";
    }


    private Map<String, String> computeMissingConfigurations(Map<String, String> props) {
        props.computeIfAbsent(LOCATORS, (key)-> DEFAULT_LOCATOR);
        props.computeIfAbsent(NULL_VALUES_MEAN_REMOVE, (key) -> DEFAULT_NULL_VALUES_MEAN_REMOVE);
        return props;
    }
}
