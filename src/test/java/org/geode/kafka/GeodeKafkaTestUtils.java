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
import java.util.Collections;
import java.util.Properties;

import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.dunit.VM;

public class GeodeKafkaTestUtils {
  protected static ZooKeeperLocalCluster startZooKeeper(Properties zookeeperProperties)
      throws IOException, QuorumPeerConfig.ConfigException {
    ZooKeeperLocalCluster zooKeeperLocalCluster = new ZooKeeperLocalCluster(zookeeperProperties);
    zooKeeperLocalCluster.start();
    return zooKeeperLocalCluster;
  }

  protected static KafkaLocalCluster startKafka(Properties kafkaProperties)
      throws IOException, InterruptedException, QuorumPeerConfig.ConfigException {
    KafkaLocalCluster kafkaLocalCluster = new KafkaLocalCluster(kafkaProperties);
    kafkaLocalCluster.start();
    return kafkaLocalCluster;
  }

  protected static void createTopic(String topicName, int numPartitions, int replicationFactor) {
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181", false, 200000,
        15000, 10, Time.SYSTEM, "myGroup", "myMetricType", null);

    Properties topicProperties = new Properties();
    topicProperties.put("flush.messages", "1");
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.createTopic(topicName, numPartitions, replicationFactor, topicProperties,
        RackAwareMode.Disabled$.MODULE$);
  }

  protected static void deleteTopic(String topicName) {
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181", false, 200000,
        15000, 10, Time.SYSTEM, "myGroup", "myMetricType", null);
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.deleteTopic(topicName);
  }

  // consumer props, less important, just for testing?
  public static Consumer<String, String> createConsumer(String testTopicForSource) {
    final Properties props = new Properties();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
        "myGroup");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Create the consumer using props.
    final Consumer<String, String> consumer =
        new KafkaConsumer<>(props);
    // Subscribe to the topic.
    consumer.subscribe(Collections.singletonList(testTopicForSource));
    return consumer;
  }

  // consumer props, less important, just for testing?
  public static Producer<String, String> createProducer() {
    final Properties props = new Properties();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // Create the producer using props.
    final Producer<String, String> producer =
        new KafkaProducer<>(props);
    return producer;

  }

  public static void startGeodeLocator(VM locatorVM) {
    locatorVM.invoke(() -> {
      Properties properties = new Properties();
      properties.setProperty(ConfigurationProperties.NAME, "locator1");
      Locator.startLocatorAndDS(10334,
          null, properties);
    });
  }

  public static void startGeodeServerAndCreateSourceRegion(VM serverVM, String regionName) {
    serverVM.invoke(() -> {
      Properties properties = new Properties();
      Cache cache = new CacheFactory(properties)
          .set(ConfigurationProperties.LOCATORS, "localhost[10334]")
          .set(ConfigurationProperties.NAME, "server-1")
          .create();
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(0);
      cacheServer.start();

      cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    });
  }

  protected static WorkerAndHerderCluster startWorkerAndHerderCluster(int maxTasks) {
    WorkerAndHerderCluster workerAndHerderCluster = new WorkerAndHerderCluster();
    try {
      workerAndHerderCluster.start(String.valueOf(maxTasks));
      Thread.sleep(20000);
    } catch (Exception e) {
      throw new RuntimeException("Could not start the worker and herder cluster" + e);
    }
    return workerAndHerderCluster;
  }

  protected static void startGeodeClientAndRegion(VM client, String regionName) {
    client.invoke(() -> {
      ClientCache clientCache = new ClientCacheFactory()
          .addPoolLocator("localhost", 10334)
          .create();

      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
    });
  }

  protected static void putDataIntoGeodeCluster(VM client, String regionName, int num) {
    client.invoke(() -> {
      ClientCache clientCache = new ClientCacheFactory()
          .addPoolLocator("localhost", 10334)
          .create();
      Region region = clientCache.getRegion(regionName);
      for (int i = 0; i < num; i++) {
        region.put("KEY" + i, "VALUE" + i);
      }
    });

  }


}
