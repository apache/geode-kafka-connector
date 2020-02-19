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

import static org.apache.geode.kafka.source.GeodeSourceConnectorConfig.REGION_TO_TOPIC_BINDINGS;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import org.apache.geode.kafka.GeodeConnectorConfig;
import org.apache.geode.kafka.sink.GeodeKafkaSink;
import org.apache.geode.kafka.sink.GeodeSinkConnectorConfig;
import org.apache.geode.kafka.source.GeodeKafkaSource;

public class WorkerAndHerderWrapper {

  public static void main(String[] args) throws IOException {
    if (args.length != 7) {
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

    Map props = new HashMap();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put("offset.storage.file.filename", offsetPath);
    // fast flushing for testing.
    props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "10");

    props.put("internal.key.converter.schemas.enable", "false");
    props.put("internal.value.converter.schemas.enable", "false");
    props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
        "org.apache.kafka.connect.storage.StringConverter");
    props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
        "org.apache.kafka.connect.storage.StringConverter");
    props.put("key.converter.schemas.enable", "false");
    props.put("value.converter.schemas.enable", "false");
    props.put(GeodeConnectorConfig.LOCATORS, locatorString);
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
    sourceProps.put(GeodeConnectorConfig.LOCATORS, locatorString);
    sourceProps.put(REGION_TO_TOPIC_BINDINGS, regionToTopicBinding);

    herder.putConnectorConfig(
        sourceProps.get(ConnectorConfig.NAME_CONFIG),
        sourceProps, true, (error, result) -> {
        });

    Map<String, String> sinkProps = new HashMap<>();
    sinkProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GeodeKafkaSink.class.getName());
    sinkProps.put(ConnectorConfig.NAME_CONFIG, "geode-kafka-sink-connector");
    sinkProps.put(ConnectorConfig.TASKS_MAX_CONFIG, maxTasks);
    sinkProps.put(GeodeSinkConnectorConfig.TOPIC_TO_REGION_BINDINGS, topicToRegionBinding);
    sinkProps.put(GeodeConnectorConfig.LOCATORS, locatorString);
    sinkProps.put("topics", sinkTopic);

    herder.putConnectorConfig(
        sinkProps.get(ConnectorConfig.NAME_CONFIG),
        sinkProps, true, (error, result) -> {
        });


  }
}
