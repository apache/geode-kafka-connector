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

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public class ZooKeeperLocalCluster {

  ZooKeeperServerMain zooKeeperServer;
  private Properties zooKeeperProperties;
  Thread zooKeeperThread;

  public ZooKeeperLocalCluster(Properties zooKeeperProperties) {
    this.zooKeeperProperties = zooKeeperProperties;
  }

  public void start() throws IOException, QuorumPeerConfig.ConfigException {
    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    quorumConfiguration.parseProperties(zooKeeperProperties);

    zooKeeperServer = new ZooKeeperServerMain();
    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);

    zooKeeperThread = new Thread() {
      public void run() {
        try {
          zooKeeperServer.runFromConfig(configuration);
        } catch (IOException | AdminServer.AdminServerException e) {
          System.out.println("ZooKeeper Failed");
          e.printStackTrace(System.err);
        }
      }
    };
    zooKeeperThread.start();
    System.out.println("ZooKeeper thread started");
  }
}
