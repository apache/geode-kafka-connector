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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SharedEventBufferSupplier implements EventBufferSupplier {

  private static BlockingQueue<GeodeEvent> eventBuffer;

  public SharedEventBufferSupplier(int size) {
    recreateEventBufferIfNeeded(size);
  }

  BlockingQueue recreateEventBufferIfNeeded(int size) {
    if (eventBuffer == null || (eventBuffer.size() + eventBuffer.remainingCapacity()) != size) {
      synchronized (GeodeKafkaSource.class) {
        if (eventBuffer == null || (eventBuffer.size() + eventBuffer.remainingCapacity()) != size) {
          BlockingQueue<GeodeEvent> oldEventBuffer = eventBuffer;
          eventBuffer = new LinkedBlockingQueue<>(size);
          if (oldEventBuffer != null) {
            eventBuffer.addAll(oldEventBuffer);
          }
        }
      }
    }
    return eventBuffer;
  }

  /**
   * Callers should not store a reference to this and instead always call get to make sure we always
   * use the latest buffer
   * Buffers themselves shouldn't change often but in cases where we want to modify the size
   */
  public BlockingQueue<GeodeEvent> get() {
    return eventBuffer;
  }
}
