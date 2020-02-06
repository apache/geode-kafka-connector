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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;

public class LocatorLauncherWrapper {

  public static void main(String[] args) throws IOException {
    Properties properties = new Properties();
    // String statsFile = new File(context.getOutputDir(), "stats.gfs").getAbsolutePath();
    // properties.setProperty(ConfigurationPropert/**/ies.STATISTIC_ARCHIVE_FILE, statsFile);
    properties.setProperty(ConfigurationProperties.NAME, "locator1");

    Locator.startLocatorAndDS(10334,
        new File("/Users/jhuynh/Pivotal/geode-kafka-connector/locator.log"), properties);
    while (true) {

    }
    //
    // LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
    // .setMemberName("locator1")
    //// .setPort(Integer.valueOf(args[0]))
    //// .setBindAddress("localhost")
    // .build();
    //
    // locatorLauncher.start();
    // while (!locatorLauncher.isRunning()) {
    //
    // }
    // System.out.println(locatorLauncher.getBindAddress() + ":" + locatorLauncher.getPort());

  }
}
