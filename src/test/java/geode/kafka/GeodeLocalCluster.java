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
package geode.kafka;

import java.io.IOException;

public class GeodeLocalCluster {

    private JavaProcess locatorProcess;
    private JavaProcess serverProcess;

    public GeodeLocalCluster() {
        locatorProcess = new JavaProcess(LocatorLauncherWrapper.class);
        serverProcess = new JavaProcess(ServerLauncherWrapper.class);
    }

    public void start() throws IOException, InterruptedException {
        System.out.println("starting locator");
        locatorProcess.exec("10334");
        Thread.sleep(15000);
        serverProcess.exec("40404");
        Thread.sleep(30000);
    }

    public void stop() {
        serverProcess.destroy();
        locatorProcess.destroy();
    }
}

