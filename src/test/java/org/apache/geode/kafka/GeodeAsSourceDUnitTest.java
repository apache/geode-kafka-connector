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
package org.apache.geode.kafka;

import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.createConsumer;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.createTopic;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.deleteTopic;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.getKafkaConfig;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.getZooKeeperProperties;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.startKafka;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.startWorkerAndHerderCluster;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.startZooKeeper;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.verifyEventsAreConsumed;

import java.util.Arrays;

import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.utils.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.kafka.utilities.KafkaLocalCluster;
import org.apache.geode.kafka.utilities.WorkerAndHerderCluster;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(Parameterized.class)
public class GeodeAsSourceDUnitTest {
  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(3);

  @Rule
  public final TestName testName = new TestName();

  @ClassRule
  public static final TemporaryFolder temporaryFolderForZooKeeper = new TemporaryFolder();

  @Rule
  public final TemporaryFolder temporaryFolderForOffset = new TemporaryFolder();

  @BeforeClass
  public static void setup()
      throws Exception {
    startZooKeeper(getZooKeeperProperties(temporaryFolderForZooKeeper));
  }

  @AfterClass
  public static void cleanUp() {
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181",
        false,
        200000,
        15000,
        10,
        Time.SYSTEM,
        "myGroup",
        "myMetricType",
        null);

    zkClient.close();
  }

  @Parameters(name = "tasks_{0}_partitions_{1}")
  public static Iterable<Object[]> getParams() {
    return Arrays.asList(new Object[][] {{1, 1}, {1, 2}, {5, 2}});
  }

  private final int numTask;
  private final int numPartition;

  public GeodeAsSourceDUnitTest(int numTask, int numPartition) {
    this.numTask = numTask;
    this.numPartition = numPartition;
  }

  @Test
  public void whenDataIsInsertedInGeodeSourceThenKafkaConsumerMustReceiveEvents() throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0, 10334);
    int locatorPort = locator.getPort();
    MemberVM server = clusterStartupRule.startServerVM(1, locatorPort);
    ClientVM client1 = clusterStartupRule
        .startClientVM(2, client -> client.withLocatorConnection(locatorPort));
    int NUM_EVENT = 10;

    // Set unique names for all the different components
    String testIdentifier = testName.getMethodName().replaceAll("[\\[\\]]", "");
    String sourceRegion = "SOURCE_REGION_" + testIdentifier;
    String sinkRegion = "SINK_REGION_" + testIdentifier;
    String sinkTopic = "SINK_TOPIC_" + testIdentifier;
    String sourceTopic = "SOURCE_TOPIC_" + testIdentifier;

    /*
     * Start the Apache Geode cluster and create the source and sink regions.
     * Create a Apache Geode client which inserts data into the source
     */
    server.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .create(sourceRegion);
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .create(sinkRegion);
    });
    client1.invoke(() -> {
      ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(sourceRegion);
    });

    /*
     * Start the Kafka Cluster, workers and the topic to which the Apache Geode will connect as
     * a source
     */
    WorkerAndHerderCluster workerAndHerderCluster = null;
    KafkaLocalCluster kafkaLocalCluster = null;
    try {
      kafkaLocalCluster = startKafka(
          getKafkaConfig(temporaryFolderForOffset.newFolder("kafkaLogs").getAbsolutePath()));
      createTopic(sourceTopic, numPartition, 1);
      // Create workers and herder cluster
      workerAndHerderCluster = startWorkerAndHerderCluster(numTask, sourceRegion, sinkRegion,
          sourceTopic, sinkTopic, temporaryFolderForOffset.getRoot().getAbsolutePath(),
          "localhost[" + locatorPort + "]");

      // Create the consumer to consume from the source topic
      Consumer<String, String> consumer = createConsumer(sourceTopic);

      // Insert data into the Apache Geode source from the client
      client1.invoke(() -> {
        Region<Object, Object> region = ClusterStartupRule.getClientCache().getRegion(sourceRegion);
        for (int i = 0; i < NUM_EVENT; i++) {
          region.put("KEY" + i, "VALUE" + i);
        }
      });

      // Assert that all the data inserted in Apache Geode source is received by the consumer
      verifyEventsAreConsumed(consumer, NUM_EVENT);
    } finally {
      // Clean up by deleting the source topic
      deleteTopic(sourceTopic);
      if (workerAndHerderCluster != null) {
        workerAndHerderCluster.stop();
      }
      if (kafkaLocalCluster != null) {
        kafkaLocalCluster.stop();
      }
    }
  }
}
