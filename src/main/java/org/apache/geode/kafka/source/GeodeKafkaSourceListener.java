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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqStatusListener;

class GeodeKafkaSourceListener implements CqStatusListener {

  private static final Logger logger = LoggerFactory.getLogger(GeodeKafkaSourceListener.class);

  public final String regionName;
  private final EventBufferSupplier eventBufferSupplier;
  private boolean initialResultsLoaded;

  public GeodeKafkaSourceListener(EventBufferSupplier eventBufferSupplier, String regionName) {
    this.regionName = regionName;
    this.eventBufferSupplier = eventBufferSupplier;
    initialResultsLoaded = false;
  }

  @Override
  public void onEvent(CqEvent aCqEvent) {
    while (!initialResultsLoaded) {
      Thread.yield();
    }
    try {
      eventBufferSupplier.get().offer(new GeodeEvent(regionName, aCqEvent), 2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {

      while (true) {
        try {
          if (!eventBufferSupplier.get().offer(new GeodeEvent(regionName, aCqEvent), 2,
              TimeUnit.SECONDS))
            break;
        } catch (InterruptedException ex) {
          logger.info("Thread interrupted while updating buffer", ex);
        }
        logger.info("GeodeKafkaSource Queue is full");
      }
    }
  }

  @Override
  public void onError(CqEvent aCqEvent) {

  }

  @Override
  public void onCqDisconnected() {
    // we should probably redistribute or reconnect
    logger.info("cq has been disconnected");
  }

  @Override
  public void onCqConnected() {

  }

  public void signalInitialResultsLoaded() {
    initialResultsLoaded = true;
  }
}
