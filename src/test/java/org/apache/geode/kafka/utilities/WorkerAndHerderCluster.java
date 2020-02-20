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

import java.io.IOException;

public class WorkerAndHerderCluster {

  private JavaProcess workerAndHerder;

  public WorkerAndHerderCluster() {
    workerAndHerder = new JavaProcess(WorkerAndHerderWrapper.class);
  }

  public void start(String maxTasks) throws IOException, InterruptedException {
    workerAndHerder.exec(maxTasks);
  }

  public void start(String maxTasks, String sourceRegion, String sinkRegion, String sourceTopic,
      String sinkTopic, String offsetPath, String locatorString, String keyConverter,
      String keyConverterArgs, String valueConverter, String valueConverterArgs)
      throws IOException, InterruptedException {
    String[] args = new String[] {maxTasks, sourceRegion, sinkRegion, sourceTopic, sinkTopic,
        offsetPath, locatorString, keyConverter, keyConverterArgs, valueConverter,
        valueConverterArgs};
    workerAndHerder.exec(args);
  }

  public void stop() {
    workerAndHerder.destroy();
  }
}
