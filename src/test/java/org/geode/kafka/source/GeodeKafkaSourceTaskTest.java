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

import static org.geode.kafka.source.GeodeSourceConnectorConfig.DEFAULT_CQ_PREFIX;
import static org.geode.kafka.source.GeodeSourceConnectorConfig.REGION_PARTITION;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.LinkedStructSet;
import org.apache.geode.cache.query.internal.ResultsBag;
import org.apache.geode.cache.query.internal.ResultsBag;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.geode.kafka.GeodeContext;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.CqEvent;


public class GeodeKafkaSourceTaskTest {


  @Test
  public void whenLoadingEntireRegionAbleToPutInitialResultsIntoEventBuffer() {
    GeodeContext geodeContext = mock(GeodeContext.class);
    BlockingQueue<GeodeEvent> eventBuffer = new LinkedBlockingQueue(100);
    boolean loadEntireRegion = true;
    boolean isDurable = false;
    CqResults fakeInitialResults = new ResultsBag();
    for (int i = 0; i < 10; i++) {
      fakeInitialResults.add(mock(Struct.class));
    }

    when(geodeContext.newCqWithInitialResults(anyString(), anyString(), any(), anyBoolean()))
        .thenReturn(fakeInitialResults);
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    task.installListenersToRegion(geodeContext, 1, createEventBufferSupplier(eventBuffer),
        "testRegion", DEFAULT_CQ_PREFIX, loadEntireRegion, isDurable);
    assertEquals(10, eventBuffer.size());
  }

  @Test
  public void whenNotLoadingEntireRegionShouldNotPutInitialResultsIntoEventBuffer() {
    GeodeContext geodeContext = mock(GeodeContext.class);
    BlockingQueue<GeodeEvent> eventBuffer = new LinkedBlockingQueue(100);
    boolean loadEntireRegion = false;
    boolean isDurable = false;
    CqResults fakeInitialResults = new ResultsBag();
    for (int i = 0; i < 10; i++) {
      fakeInitialResults.add(mock(CqEvent.class));
    }

    when(geodeContext.newCqWithInitialResults(anyString(), anyString(), any(), anyBoolean()))
        .thenReturn(fakeInitialResults);
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    task.installListenersToRegion(geodeContext, 1, createEventBufferSupplier(eventBuffer),
        "testRegion", DEFAULT_CQ_PREFIX, loadEntireRegion, isDurable);
    assertEquals(0, eventBuffer.size());
  }

  @Test
  public void cqListenerOnEventPopulatesEventsBuffer() {
    GeodeContext geodeContext = mock(GeodeContext.class);
    BlockingQueue<GeodeEvent> eventBuffer = new LinkedBlockingQueue(100);
    boolean loadEntireRegion = false;
    boolean isDurable = false;

    when(geodeContext.newCqWithInitialResults(anyString(), anyString(), any(), anyBoolean()))
        .thenReturn(mock(CqResults.class));
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    GeodeKafkaSourceListener listener =
        task.installListenersToRegion(geodeContext, 1, createEventBufferSupplier(eventBuffer),
            "testRegion", DEFAULT_CQ_PREFIX, loadEntireRegion, isDurable);

    listener.onEvent(mock(CqEvent.class));
    assertEquals(1, eventBuffer.size());
  }

  @Test
  public void readyForEventsIsCalledIfDurable() {
    ClientCache clientCache = mock(ClientCache.class);

    GeodeContext geodeContext = mock(GeodeContext.class);
    when(geodeContext.getClientCache()).thenReturn(clientCache);

    GeodeSourceConnectorConfig config = mock(GeodeSourceConnectorConfig.class);
    when(config.isDurable()).thenReturn(true);
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    task.installOnGeode(config, geodeContext, null, "", false);
    verify(clientCache, times(1)).readyForEvents();
  }

  @Test
  public void cqIsInvokedForEveryRegionWithATopic() {
    ClientCache clientCache = mock(ClientCache.class);

    GeodeContext geodeContext = mock(GeodeContext.class);
    when(geodeContext.getClientCache()).thenReturn(clientCache);

    Map<String, List<String>> regionToTopicsMap = new HashMap<>();
    regionToTopicsMap.put("region1", new ArrayList());

    GeodeSourceConnectorConfig config = mock(GeodeSourceConnectorConfig.class);
    when(config.getCqsToRegister()).thenReturn(regionToTopicsMap.keySet());

    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    task.installOnGeode(config, geodeContext, null, "someCqPrefix", false);
    verify(geodeContext, times(1)).newCq(anyString(), anyString(), any(), anyBoolean());
  }

  @Test
  public void cqWithInitialResultsIsInvokedForEveryRegionWithATopicIfLoadEntireIsSet() {
    ClientCache clientCache = mock(ClientCache.class);

    GeodeContext geodeContext = mock(GeodeContext.class);
    when(geodeContext.getClientCache()).thenReturn(clientCache);
    when(geodeContext.newCqWithInitialResults(anyString(), anyString(), any(CqAttributes.class), anyBoolean())).thenReturn(new ResultsBag());
    Map<String, List<String>> regionToTopicsMap = new HashMap<>();
    regionToTopicsMap.put("region1", new ArrayList());

    GeodeSourceConnectorConfig config = mock(GeodeSourceConnectorConfig.class);
    when(config.getCqsToRegister()).thenReturn(regionToTopicsMap.keySet());

    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    task.installOnGeode(config, geodeContext,
        createEventBufferSupplier(new LinkedBlockingQueue<>()), "someCqPrefix", true);
    verify(geodeContext, times(1)).newCqWithInitialResults(anyString(), anyString(), any(),
        anyBoolean());
  }

  @Test
  public void readyForEventsIsNotCalledIfNotDurable() {
    ClientCache clientCache = mock(ClientCache.class);

    GeodeContext geodeContext = mock(GeodeContext.class);
    when(geodeContext.getClientCache()).thenReturn(clientCache);

    GeodeSourceConnectorConfig config = mock(GeodeSourceConnectorConfig.class);
    when(config.isDurable()).thenReturn(false);
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    task.installOnGeode(config, geodeContext, null, "", false);
    verify(clientCache, times(0)).readyForEvents();
  }

  @Test
  public void pollReturnsEventsWhenEventBufferHasValues() throws Exception {
    // BlockingQueue<GeodeEvent> eventBuffer = new LinkedBlockingQueue(100);
    // CqEvent cqEvent = mock(CqEvent.class);
    // when(cqEvent.getNewValue()).thenReturn("New Value");
    // GeodeEvent event = mock(GeodeEvent.class);
    // when(event.getEvent()).thenReturn(cqEvent);
    // eventBuffer.add(event);
    //
    // List<String> topics = new ArrayList<>();
    // topics.add("myTopic");
    //
    // GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    // task.startForTesting(eventBuffer, topics, 1);
    // List<SourceRecord> records = task.poll();
    // assertEquals(1, records.size());
  }

  @Test
  public void installOnGeodeShouldCallCq() {
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
  }



  @Test
  public void createSourcePartitionsShouldReturnAMapOfSourcePartitions() {
    GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    List<String> regionNames = Arrays.asList(new String[] {"region1", "region2", "region3"});
    Map<String, Map<String, String>> sourcePartitions = task.createSourcePartitionsMap(regionNames);
    assertThat(3, is(sourcePartitions.size()));
    assertThat(true, is(sourcePartitions.get("region1").get(REGION_PARTITION).equals("region1")));
    assertThat(true, is(sourcePartitions.get("region2").get(REGION_PARTITION).equals("region2")));
    assertThat(true, is(sourcePartitions.get("region3").get(REGION_PARTITION).equals("region3")));
  }

  @Test
  public void listOfLocatorsShouldBeConfiguredIntoClientCache() {

  }

  @Test
  public void shouldNotBeDurableIfDurableClientIdIsNull() {

  }

  @Test
  public void shouldNotCallReadyForEventsIfDurableClientPrefixIsEmpty() {

  }


  @Test
  public void cqPrefixShouldBeProperlyCalculatedFromProps() {
    // GeodeContext geodeContext = mock(GeodeContext.class);
    // GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
  }


  private EventBufferSupplier createEventBufferSupplier(BlockingQueue<GeodeEvent> eventBuffer) {
    return new EventBufferSupplier() {
      @Override
      public BlockingQueue<GeodeEvent> get() {
        return eventBuffer;
      }
    };
  }
}
