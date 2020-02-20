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
package org.apache.geode.kafka.converter;

import static org.apache.geode.kafka.GeodeConnectorConfig.DEFAULT_KEY_CONVERTER;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.createTopic;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.deleteTopic;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.getKafkaConfig;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.getZooKeeperProperties;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.startKafka;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.startWorkerAndHerderCluster;
import static org.apache.geode.kafka.utilities.GeodeKafkaTestUtils.startZooKeeper;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;
import org.assertj.core.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.kafka.utilities.KafkaLocalCluster;
import org.apache.geode.kafka.utilities.TestObject;
import org.apache.geode.kafka.utilities.WorkerAndHerderCluster;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class JsonPdxConverterDUnitTest {
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(3);

  @Rule
  public TestName testName = new TestName();

  @ClassRule
  public static TemporaryFolder temporaryFolderForZooKeeper = new TemporaryFolder();

  @Rule
  public TemporaryFolder temporaryFolderForOffset = new TemporaryFolder();

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

  @Test
  public void jsonPdxConverterCanConvertPdxInstanceToJsonAndBackWhenDataMovesFromRegionToTopicToRegion()
      throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    // .withPDXReadSerialized()
    MemberVM server1 = clusterStartupRule.startServerVM(1, server -> server
        .withConnectionToLocator(locatorPort)
    // .withPDXReadSerialized()
    );
    ClientVM client1 = clusterStartupRule.startClientVM(2, client -> client
        .withLocatorConnection(locatorPort)
        .withCacheSetup(
            cf -> cf.setPdxSerializer(new ReflectionBasedAutoSerializer("org.apache.geode.kafka.*"))
                .setPdxReadSerialized(true)));

    // Set unique names for all the different components
    String sourceRegionName = "SOURCE_REGION";
    String sinkRegionName = "SINK_REGION";
    // We only need one topic for this test, which we will both source to and sink from
    String topicName = "TEST_TOPIC";

    /*
     * Start the Apache Geode cluster and create the source and sink regions.
     * Create a Apache Geode client which can insert data into the source and get data from the sink
     */
    server1.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .create(sourceRegionName);
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .create(sinkRegionName);
    });
    client1.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(sourceRegionName);
      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(sinkRegionName);
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
      createTopic(topicName, 1, 1);
      // Create workers and herder cluster
      workerAndHerderCluster = startWorkerAndHerderCluster(1, sourceRegionName, sinkRegionName,
          topicName, topicName, temporaryFolderForOffset.getRoot().getAbsolutePath(),
          "localhost[" + locatorPort + "]", DEFAULT_KEY_CONVERTER, "",
          JsonPdxConverter.class.getCanonicalName(),
          JsonPdxConverter.ADD_TYPE_ANNOTATION_TO_JSON + "=true");

      // Insert data into the Apache Geode source and retrieve the data from the Apache Geode sink
      // from the client
      client1.invoke(() -> {
        // Create an object that will be serialized into a PdxInstance
        String name = "testName";
        int age = 42;
        double number = 3.141;
        List<String> words = new ArrayList<>();
        words.add("words1");
        words.add("words2");
        words.add("words3");
        TestObject originalObject = new TestObject(name, age, number, words);

        ClientCache clientCache = ClusterStartupRule.getClientCache();

        // Create a PdxInstance from the test object
        PdxInstanceFactory instanceFactory =
            clientCache.createPdxInstanceFactory(originalObject.getClass().getName());
        Arrays.asList(originalObject.getClass().getFields())
            .stream()
            .map(field -> (Field) field)
            .forEach(field -> {
              try {
                Object value = field.get(originalObject);
                Class type = field.getType();
                instanceFactory.writeField(field.getName(), value, type);
              } catch (IllegalAccessException ignore) {
              }
            });
        PdxInstance putInstance = instanceFactory.create();

        // Put the PdxInstance into the source region
        String key = "key1";
        clientCache.getRegion(sourceRegionName).put(key, putInstance);

        // Assert that the data that arrives in the sink region is the same as the data that was put
        // into the source region
        Region<Object, Object> sinkRegion = clientCache.getRegion(sinkRegionName);
        await().atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(1, sinkRegion.sizeOnServer()));
        PdxInstance getInstance = (PdxInstance) sinkRegion.get(key);

        assertEquals(originalObject, getInstance.getObject());
      });

    } finally {
      // Clean up by deleting the topic
      deleteTopic(topicName);
      if (workerAndHerderCluster != null) {
        workerAndHerderCluster.stop();
      }
      if (kafkaLocalCluster != null) {
        kafkaLocalCluster.stop();
      }
    }
  }
}
