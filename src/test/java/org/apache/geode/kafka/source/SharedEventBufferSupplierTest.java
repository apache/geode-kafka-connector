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
package org.apache.geode.kafka.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.concurrent.BlockingQueue;

import org.junit.Test;

public class SharedEventBufferSupplierTest {

  @Test
  public void creatingNewSharedEventSupplierShouldCreateInstance() {
    SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
    assertNotNull(supplier.get());
  }

  @Test
  public void alreadySharedEventSupplierShouldReturnSameInstanceOfEventBuffer() {
    SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
    BlockingQueue<GeodeEvent> queue = supplier.get();
    supplier = new SharedEventBufferSupplier(1);
    assertEquals(queue, supplier.get());
  }

  @Test
  public void newEventBufferShouldBeReflectedInAllSharedSuppliers() {
    SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
    SharedEventBufferSupplier newSupplier = new SharedEventBufferSupplier(2);
    assertEquals(supplier.get(), newSupplier.get());
  }

  @Test
  public void newEventBufferSuppliedShouldNotBeTheOldQueue() {
    SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
    BlockingQueue<GeodeEvent> queue = supplier.get();
    SharedEventBufferSupplier newSupplier = new SharedEventBufferSupplier(2);
    assertNotEquals(queue, newSupplier.get());
  }

  @Test
  public void newEventBufferShouldContainAllEventsFromTheOldSupplier() {
    SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
    GeodeEvent geodeEvent = mock(GeodeEvent.class);
    supplier.get().add(geodeEvent);
    SharedEventBufferSupplier newSupplier = new SharedEventBufferSupplier(2);
    assertEquals(geodeEvent, newSupplier.get().poll());
  }
}
