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
package org.apache.geode.kafka.utilities;

import static org.apache.geode.kafka.utils.GeodeConfigurationConstants.LOCATORS;
import static org.apache.geode.kafka.utils.GeodeSourceConfigurationConstants.REGION_TO_TOPIC_BINDINGS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;

import org.apache.geode.kafka.sink.GeodeKafkaSink;
import org.apache.geode.kafka.source.GeodeKafkaSource;
import org.apache.geode.kafka.utils.GeodeConfigurationConstants;
import org.apache.geode.kafka.utils.GeodeSinkConfigurationConstants;

public class WorkerAndHerderWrapper {

  public static void main(String[] args) {
    if (args.length != 11) {
      throw new RuntimeException("Insufficient arguments to start workers and herders");
    }
    String maxTasks = args[0];
    String sourceRegion = args[1];
    String sinkRegion = args[2];
    String sourceTopic = args[3];
    String sinkTopic = args[4];
    String offsetPath = args[5];
    String regionToTopicBinding = "[" + sourceRegion + ":" + sourceTopic + "]";
    String topicToRegionBinding = "[" + sinkTopic + ":" + sinkRegion + "]";
    String locatorString = args[6];
    String keyConverter = args[7];
    String keyConverterArgs = args[8];
    Map<String, String> keyConverterProps = new HashMap<>();
    if (keyConverterArgs != null && !keyConverterArgs.isEmpty()) {
      keyConverterProps = parseArguments(keyConverterArgs, true);
    }
    String valueConverter = args[9];
    String valueConverterArgs = args[10];
    Map<String, String> valueConverterProps = new HashMap<>();
    if (valueConverterArgs != null && !valueConverterArgs.isEmpty()) {
      valueConverterProps = parseArguments(valueConverterArgs, false);
    }

    HashMap<String, String> props = new HashMap<>();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put("offset.storage.file.filename", offsetPath);
    // fast flushing for testing.
    props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "10");

    props.put("internal.key.converter.schemas.enable", "false");
    props.put("internal.value.converter.schemas.enable", "false");
    props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, keyConverter);
    props.putAll(keyConverterProps);
    props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, valueConverter);
    props.putAll(valueConverterProps);
    props.put("key.converter.schemas.enable", "false");
    props.put("value.converter.schemas.enable", "false");
    props.put(LOCATORS, locatorString);
    WorkerConfig workerCfg = new StandaloneConfig(props);

    MemoryOffsetBackingStore offBackingStore = new MemoryOffsetBackingStore();
    offBackingStore.configure(workerCfg);

    Worker worker = new Worker("WORKER_ID", new SystemTime(), new Plugins(props), workerCfg,
        offBackingStore, new AllConnectorClientConfigOverridePolicy());
    worker.start();

    Herder herder = new StandaloneHerder(worker, ConnectUtils.lookupKafkaClusterId(workerCfg),
        new AllConnectorClientConfigOverridePolicy());
    herder.start();

    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GeodeKafkaSource.class.getName());
    sourceProps.put(ConnectorConfig.NAME_CONFIG, "geode-kafka-source-connector");
    sourceProps.put(ConnectorConfig.TASKS_MAX_CONFIG, maxTasks);
    sourceProps.put(GeodeConfigurationConstants.LOCATORS, locatorString);
    sourceProps.put(REGION_TO_TOPIC_BINDINGS, regionToTopicBinding);

    herder.putConnectorConfig(
        sourceProps.get(ConnectorConfig.NAME_CONFIG),
        sourceProps, true, (error, result) -> {
        });

    Map<String, String> sinkProps = new HashMap<>();
    sinkProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GeodeKafkaSink.class.getName());
    sinkProps.put(ConnectorConfig.NAME_CONFIG, "geode-kafka-sink-connector");
    sinkProps.put(ConnectorConfig.TASKS_MAX_CONFIG, maxTasks);
    sinkProps.put(GeodeSinkConfigurationConstants.TOPIC_TO_REGION_BINDINGS, topicToRegionBinding);
    sinkProps.put(GeodeConfigurationConstants.LOCATORS, locatorString);
    sinkProps.put("topics", sinkTopic);

    herder.putConnectorConfig(
        sinkProps.get(ConnectorConfig.NAME_CONFIG),
        sinkProps, true, (error, result) -> {
        });


  }

  // We expect that converter properties will be supplied as a comma separated list of the form
  // "first.property.name=first.property.value,second.property.name=second.property.value"
  public static Map<String, String> parseArguments(String args, boolean isKeyConverter) {
    String propertyNamePrefix;
    if (isKeyConverter) {
      propertyNamePrefix = "key.converter.";
    } else {
      propertyNamePrefix = "value.converter.";
    }
    return Arrays.stream(args.split(","))
        .collect(Collectors.toMap(
            string -> propertyNamePrefix + string.substring(0, string.indexOf("=")).trim(),
            string -> string.substring(string.indexOf("=") + 1).trim()));
  }
}
