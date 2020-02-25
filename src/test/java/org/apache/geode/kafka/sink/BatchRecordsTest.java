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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import org.apache.geode.cache.Region;

@SuppressWarnings("unchecked")
public class BatchRecordsTest {
  @Test
  public void updatingARecordShouldRemoveFromTheRemoveListIfNullValuesIsRemoveBooleanIsSet() {
    HashMap<Object, Object> updates = mock(HashMap.class);
    ArrayList<Object> removes = mock(ArrayList.class);
    when(removes.contains(any())).thenReturn(true);
    BatchRecords records = new BatchRecords(updates, removes);
    SinkRecord sinkRecord = mock(SinkRecord.class);
    records.addUpdateOperation(sinkRecord, true);
    verify(removes, times(1)).remove(any());
  }

  @Test
  public void updatingARecordShouldAddToTheUpdateMap() {
    HashMap<Object, Object> updates = mock(HashMap.class);
    ArrayList<Object> removes = mock(ArrayList.class);
    when(removes.contains(any())).thenReturn(false);
    BatchRecords records = new BatchRecords(updates, removes);
    SinkRecord sinkRecord = mock(SinkRecord.class);
    records.addUpdateOperation(sinkRecord, true);
    verify(updates, times(1)).put(any(), any());
  }

  @Test
  public void updatingARecordShouldNotRemoveFromTheRemoveListIfNullValuesIsNotSet() {
    HashMap<Object, Object> updates = mock(HashMap.class);
    ArrayList<Object> removes = mock(ArrayList.class);
    when(removes.contains(any())).thenReturn(true);
    BatchRecords records = new BatchRecords(updates, removes);
    SinkRecord sinkRecord = mock(SinkRecord.class);
    records.addUpdateOperation(sinkRecord, false);
    verify(removes, times(0)).remove(any());
  }


  @Test
  public void removingARecordShouldRemoveFromTheUpdateMapIfKeyIsPresent() {
    HashMap<Object, Object> updates = mock(HashMap.class);
    ArrayList<Object> removes = mock(ArrayList.class);
    when(updates.containsKey(any())).thenReturn(true);
    BatchRecords records = new BatchRecords(updates, removes);
    SinkRecord sinkRecord = mock(SinkRecord.class);
    records.addRemoveOperation(sinkRecord);
    verify(updates, times(1)).remove(any());
  }

  @Test
  public void removingARecordAddToTheRemoveCollection() {
    HashMap<Object, Object> updates = mock(HashMap.class);
    ArrayList<Object> removes = mock(ArrayList.class);
    BatchRecords records = new BatchRecords(updates, removes);
    SinkRecord sinkRecord = mock(SinkRecord.class);
    records.addRemoveOperation(sinkRecord);
    verify(removes, times(1)).add(any());
  }

  @Test
  public void executeOperationsShouldInvokePutAllAndRemoveAll() {
    Region<Object, Object> region = mock(Region.class);
    BatchRecords records = new BatchRecords();
    records.executeOperations(region);
    verify(region, times(1)).putAll(any());
    verify(region, times(1)).removeAll(any());
  }



}
