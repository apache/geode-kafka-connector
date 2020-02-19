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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.geode.cache.Region;

/**
 * A collection of records to put/remove from a region
 */
public class BatchRecords {
  private static final Logger logger = LoggerFactory.getLogger(BatchRecords.class);

  private Map updateMap;
  private Collection removeList;

  public BatchRecords() {
    this(new HashMap(), new ArrayList());
  }

  /** Used for tests **/
  public BatchRecords(Map updateMap, Collection removeList) {
    this.updateMap = updateMap;
    this.removeList = removeList;
  }

  public void addRemoveOperation(SinkRecord record) {
    // if a previous operation added to the update map
    // let's just remove it so we don't do a put and then a remove
    // depending on the order of operations (putAll then removeAll or removeAll or putAll)...
    // ...we could remove one of the if statements.
    if (updateMap.containsKey(record.key())) {
      updateMap.remove(record.key());
    } else {
      removeList.add(record.key());
    }
  }

  public void addUpdateOperation(SinkRecord record, boolean nullValuesMeansRemove) {
    // it's assumed the records in are order
    // if so if a previous value was in the remove list
    // let's not remove it at the end of this operation
    if (nullValuesMeansRemove) {
      if (removeList.contains(record.key())) {
        removeList.remove(record.key());
      }
    }
    updateMap.put(record.key(), record.value());
  }


  public void executeOperations(Region region) {
    if (region != null) {
      region.putAll(updateMap);
      region.removeAll(removeList);
    } else {
      logger.info("Unable to locate proxy region: " + region);
    }
  }
}
