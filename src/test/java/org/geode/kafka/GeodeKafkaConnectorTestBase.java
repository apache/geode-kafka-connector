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

import static org.awaitility.Awaitility.await;
import static org.geode.kafka.GeodeKafkaTestUtils.createConsumer;
import static org.geode.kafka.GeodeKafkaTestUtils.createTopic;
import static org.geode.kafka.GeodeKafkaTestUtils.deleteTopic;
import static org.geode.kafka.GeodeKafkaTestUtils.putDataIntoGeodeCluster;
import static org.geode.kafka.GeodeKafkaTestUtils.startGeodeClientAndRegion;
import static org.geode.kafka.GeodeKafkaTestUtils.startGeodeLocator;
import static org.geode.kafka.GeodeKafkaTestUtils.startGeodeServerAndCreateSourceRegion;
import static org.geode.kafka.GeodeKafkaTestUtils.startKafka;
import static org.geode.kafka.GeodeKafkaTestUtils.startWorkerAndHerderCluster;
import static org.geode.kafka.GeodeKafkaTestUtils.startZooKeeper;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.version.VersionManager;

public class GeodeKafkaConnectorTestBase {
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public TestName testName = new TestName();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static boolean debug = true;

  public static String TEST_REGION_TO_TOPIC_BINDINGS = "[someRegionForSource:someTopicForSource]";
  public static String TEST_TOPIC_TO_REGION_BINDINGS = "[someTopicForSink:someRegionForSink]";

  public static String TEST_TOPIC_FOR_SOURCE = "someTopicForSource";
  public static String TEST_REGION_FOR_SOURCE = "someRegionForSource";
  public static String TEST_TOPIC_FOR_SINK = "someTopicForSink";
  public static String TEST_REGION_FOR_SINK = "someRegionForSink";

  private static ZooKeeperLocalCluster zooKeeperLocalCluster;
  private static KafkaLocalCluster kafkaLocalCluster;
  private static GeodeLocalCluster geodeLocalCluster;
  private static WorkerAndHerderCluster workerAndHerderCluster;
  private static Consumer<String, String> consumer;

  private static Properties getZooKeeperProperties() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("dataDir",
        (debug) ? "/tmp/zookeeper" : temporaryFolder.newFolder("zookeeper").getAbsolutePath());
    properties.setProperty("clientPort", "2181");
    properties.setProperty("tickTime", "2000");
    return properties;
  }

  private static Properties getKafkaConfig() throws IOException {
    int BROKER_PORT = 9092;
    Properties props = new Properties();

    props.put("broker.id", "0");
    props.put("zookeeper.connect", "localhost:2181");
    props.put("host.name", "localHost");
    props.put("port", BROKER_PORT);
    props.put("offsets.topic.replication.factor", "1");

    // Specifically GeodeKafka connector configs
    return props;
  }


  // @Before
  // public void setup()
  // throws IOException, QuorumPeerConfig.ConfigException, InterruptedException {
  //
  // System.out.println("NABA : kafka started");
  // }

  @After
  public void cleanUp() {
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
    kafkaLocalCluster.stop();
  }

  @Test
  public void doNothing() {

  }

  @Test
  public void endToEndSourceTest2() throws Exception {
    startZooKeeper(getZooKeeperProperties());
    startKafka(getKafkaConfig());
    Host host = Host.getHost(0);
    VM server = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM locator = host.getVM(VersionManager.CURRENT_VERSION, 1);
    VM client = host.getVM(VersionManager.CURRENT_VERSION, 2);
    WorkerAndHerderCluster workerAndHerderCluster = null;
    // Start the Apache Geode locator and server and create the source region
    startGeodeLocator(locator);
    // Topic Name and all region names must be the same
    String topicName = testName.getMethodName();
    startGeodeServerAndCreateSourceRegion(server, topicName);
    startGeodeClientAndRegion(client, topicName);

    try {
      createTopic(topicName, 1, 1);
      // Create workers and herder cluster
      workerAndHerderCluster = startWorkerAndHerderCluster(1);

      consumer = createConsumer(topicName);

      // Insert data into the Apache Geode source
      putDataIntoGeodeCluster(client, topicName, 10);
      // Assert that all the data inserted in Apache Geode source is received by the consumer
      AtomicInteger valueReceived = new AtomicInteger(0);
      await().atMost(10, TimeUnit.SECONDS).until(() -> {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        for (ConsumerRecord<String, String> record : records) {
          valueReceived.incrementAndGet();
        }
        return valueReceived.get() == 10;
      });
    } finally {
      deleteTopic(topicName);
      if (workerAndHerderCluster != null) {
        workerAndHerderCluster.stop();
      }
    }
  }

}
