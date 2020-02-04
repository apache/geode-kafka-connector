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

import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static geode.kafka.GeodeConnectorConfig.TASK_ID;
import static geode.kafka.GeodeSinkConnectorConfig.TOPIC_TO_REGION_BINDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeodeKafkaSinkTest {

    @Test
    public void taskConfigsCreatesMaxNumberOfTasks() {
        GeodeKafkaSink sink = new GeodeKafkaSink();
        Map<String, String> props = new HashMap();
        props.put(TOPIC_TO_REGION_BINDINGS, "[someTopic:someRegion]");
        sink.start(props);
        Collection<Map<String,String>> tasks = sink.taskConfigs(5);
        assertEquals(5, tasks.size());
    }

    @Test
    public void sinkTaskConfigsAllAssignedEntireTopicToRegionBinding() {
        GeodeKafkaSink sink = new GeodeKafkaSink();
        Map<String, String> props = new HashMap();
        props.put(TOPIC_TO_REGION_BINDINGS, "[someTopic:someRegion]");
        sink.start(props);
        Collection<Map<String,String>> tasks = sink.taskConfigs(5);
        for(Map<String, String> prop : tasks) {
            assertEquals("[someTopic:someRegion]", prop.get(TOPIC_TO_REGION_BINDINGS));
        }
    }

    @Test
    public void eachTaskHasUniqueTaskIds() {
        GeodeKafkaSink sink = new GeodeKafkaSink();
        Map<String, String> props = new HashMap();
        props.put(TOPIC_TO_REGION_BINDINGS, "[someTopic:someRegion]");
        sink.start(props);
        Collection<Map<String,String>> tasks = sink.taskConfigs(5);
        HashSet<String> seenIds = new HashSet();
        for(Map<String, String> taskProp : tasks) {
            assertTrue(seenIds.add(taskProp.get(TASK_ID)));
        }
    }
}
