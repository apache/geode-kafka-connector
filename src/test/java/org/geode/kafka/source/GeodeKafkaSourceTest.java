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

import static org.geode.kafka.source.GeodeSourceConnectorConfig.REGION_TO_TOPIC_BINDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.geode.kafka.GeodeConnectorConfig;
import org.junit.Test;

public class GeodeKafkaSourceTest {

  @Test
  public void taskConfigsCreatesMaxNumberOfTasks() {
    GeodeKafkaSource source = new GeodeKafkaSource();
    Map<String, String> props = new HashMap();
    props.put(REGION_TO_TOPIC_BINDINGS, "[someRegion:someTopic]");
    source.start(props);
    Collection<Map<String, String>> tasks = source.taskConfigs(5);
    assertEquals(5, tasks.size());
  }

  @Test
  public void sourceTaskConfigsAllAssignedEntireRegionToTopicBinding() {
    GeodeKafkaSource source = new GeodeKafkaSource();
    Map<String, String> props = new HashMap();
    props.put(REGION_TO_TOPIC_BINDINGS, "[someRegion:someTopic]");
    source.start(props);
    Collection<Map<String, String>> tasks = source.taskConfigs(5);
    for (Map<String, String> prop : tasks) {
      assertEquals("[someRegion:someTopic]", prop.get(REGION_TO_TOPIC_BINDINGS));
    }
  }

  @Test
  public void eachTaskHasUniqueTaskIds() {
    GeodeKafkaSource sink = new GeodeKafkaSource();
    Map<String, String> props = new HashMap();
    props.put(REGION_TO_TOPIC_BINDINGS, "[someRegion:someTopic]");
    sink.start(props);
    Collection<Map<String, String>> tasks = sink.taskConfigs(5);
    HashSet<String> seenIds = new HashSet();
    for (Map<String, String> taskProp : tasks) {
      assertTrue(seenIds.add(taskProp.get(GeodeConnectorConfig.TASK_ID)));
    }
  }

}
