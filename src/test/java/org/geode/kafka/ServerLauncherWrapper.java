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
package org.geode.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;

public class ServerLauncherWrapper {

  public static void main(String... args) throws IOException {
    // ServerLauncher serverLauncher = new ServerLauncher.Builder()
    // .setMemberName("server1")
    //// .setServerPort(Integer.valueOf(args[0]))
    //// .setServerBindAddress("localhost")
    // // .set("locators", "localhost[10334]")
    //// .set("jmx-manager", "true")
    //// .set("jmx-manager-start", "true")
    // .build();
    //
    // serverLauncher.start();
    // System.out.println("Geode Server Launcher complete");



    Properties properties = new Properties();
    String locatorString = "localhost[10334]";
    // String statsFile = new File(context.getOutputDir(), "stats.gfs").getAbsolutePath();
    Cache cache = new CacheFactory(properties)
        // .setPdxSerializer(new ReflectionBasedAutoSerializer("benchmark.geode.data.*"))
        .set(ConfigurationProperties.LOCATORS, locatorString)
        .set(ConfigurationProperties.NAME,
            "server-1")
        .set(ConfigurationProperties.LOG_FILE,
            "/Users/jhuynh/Pivotal/geode-kafka-connector/server.log")
        .set(ConfigurationProperties.LOG_LEVEL, "info")
        // .set(ConfigurationProperties.STATISTIC_ARCHIVE_FILE, statsFile)
        .create();
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();

    // create the region
    cache.createRegionFactory(RegionShortcut.PARTITION).create(
        GeodeKafkaTestCluster.TEST_REGION_FOR_SINK);
    cache.createRegionFactory(RegionShortcut.PARTITION).create(
        GeodeKafkaTestCluster.TEST_REGION_FOR_SOURCE);
    System.out.println("starting cacheserver");
    while (true) {

    }
  }
}
