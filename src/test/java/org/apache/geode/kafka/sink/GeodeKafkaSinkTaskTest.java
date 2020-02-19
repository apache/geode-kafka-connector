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

import static org.apache.geode.kafka.sink.GeodeSinkConnectorConfig.NULL_VALUES_MEAN_REMOVE;
import static org.apache.geode.kafka.sink.GeodeSinkConnectorConfig.TOPIC_TO_REGION_BINDINGS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.kafka.GeodeConnectorConfig;

public class GeodeKafkaSinkTaskTest {

  private HashMap<String, String> createTestSinkProps(boolean nullMeansRemove) {
    HashMap<String, String> props = new HashMap<>();
    props.put(TOPIC_TO_REGION_BINDINGS, "[topic:region]");
    props.put(GeodeConnectorConfig.TASK_ID, "0");
    props.put(NULL_VALUES_MEAN_REMOVE, String.valueOf(nullMeansRemove));
    props.put(GeodeConnectorConfig.LOCATORS, "localhost[10334]");
    return props;
  }

  @Test
  public void putRecordsAddsToRegionBatchRecords() {
    boolean nullMeansRemove = true;
    GeodeKafkaSinkTask task = new GeodeKafkaSinkTask();
    HashMap<String, String> props = createTestSinkProps(nullMeansRemove);

    SinkRecord topicRecord = mock(SinkRecord.class);
    when(topicRecord.topic()).thenReturn("topic");
    when(topicRecord.value()).thenReturn("value");
    when(topicRecord.key()).thenReturn("key");

    List<SinkRecord> records = new ArrayList();
    records.add(topicRecord);

    HashMap<String, Region> regionNameToRegion = new HashMap<>();
    GeodeSinkConnectorConfig geodeSinkConnectorConfig = new GeodeSinkConnectorConfig(props);
    HashMap<String, BatchRecords> batchRecordsMap = new HashMap();
    BatchRecords batchRecords = mock(BatchRecords.class);
    batchRecordsMap.put("region", batchRecords);
    task.configure(geodeSinkConnectorConfig);
    task.setRegionNameToRegion(regionNameToRegion);

    task.put(records, batchRecordsMap);
    assertTrue(batchRecordsMap.containsKey("region"));
    verify(batchRecords, times(1)).addUpdateOperation(topicRecord, nullMeansRemove);
  }

  @Test
  public void newBatchRecordsAreCreatedIfOneDoesntExist() {
    boolean nullMeansRemove = true;
    GeodeKafkaSinkTask task = new GeodeKafkaSinkTask();
    HashMap<String, String> props = createTestSinkProps(nullMeansRemove);

    SinkRecord topicRecord = mock(SinkRecord.class);
    when(topicRecord.topic()).thenReturn("topic");
    when(topicRecord.value()).thenReturn("value");
    when(topicRecord.key()).thenReturn("key");

    List<SinkRecord> records = new ArrayList();
    records.add(topicRecord);

    HashMap<String, Region> regionNameToRegion = new HashMap<>();
    GeodeSinkConnectorConfig geodeSinkConnectorConfig = new GeodeSinkConnectorConfig(props);
    HashMap<String, BatchRecords> batchRecordsMap = new HashMap();
    task.configure(geodeSinkConnectorConfig);
    task.setRegionNameToRegion(regionNameToRegion);

    task.put(records, batchRecordsMap);
    assertNotNull(batchRecordsMap.get("region"));
  }
}
