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

import static org.apache.geode.kafka.GeodeConnectorConfig.DEFAULT_KEY_CONVERTER;
import static org.apache.geode.kafka.GeodeConnectorConfig.DEFAULT_VALUE_CONVERTER;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.rules.TemporaryFolder;

public class GeodeKafkaTestUtils {
  public static void startZooKeeper(Properties zookeeperProperties)
      throws IOException, QuorumPeerConfig.ConfigException {
    ZooKeeperLocalCluster zooKeeperLocalCluster = new ZooKeeperLocalCluster(zookeeperProperties);
    zooKeeperLocalCluster.start();
  }

  public static KafkaLocalCluster startKafka(Properties kafkaProperties) {
    KafkaLocalCluster kafkaLocalCluster = new KafkaLocalCluster(kafkaProperties);
    kafkaLocalCluster.start();
    return kafkaLocalCluster;
  }

  public static void createTopic(String topicName, int numPartitions, int replicationFactor) {
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181", false, 200000,
        15000, 10, Time.SYSTEM, "myGroup", "myMetricType", null);

    Properties topicProperties = new Properties();
    topicProperties.put("flush.messages", "1");
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.createTopic(topicName, numPartitions, replicationFactor, topicProperties,
        RackAwareMode.Disabled$.MODULE$);
  }

  public static void deleteTopic(String topicName) {
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181", false, 200000,
        15000, 10, Time.SYSTEM, "myGroup", "myMetricType", null);
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.deleteTopic(topicName);
  }

  public static Producer<String, String> createProducer() {
    final Properties props = new Properties();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // Create the producer using props.
    return new KafkaProducer<>(props);
  }

  public static Properties getZooKeeperProperties(TemporaryFolder temporaryFolder)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("dataDir", temporaryFolder.newFolder("zookeeper").getAbsolutePath());
    properties.setProperty("clientPort", "2181");
    properties.setProperty("tickTime", "2000");
    return properties;
  }

  public static Properties getKafkaConfig(String logPath) {
    int BROKER_PORT = 9092;
    Properties props = new Properties();
    props.put("broker.id", "0");
    props.put("zookeeper.connect", "localhost:2181");
    props.put("host.name", "localHost");
    props.put("port", BROKER_PORT);
    props.put("offsets.topic.replication.factor", "1");
    props.put("log.dirs", logPath);
    return props;
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

  public static WorkerAndHerderCluster startWorkerAndHerderCluster(int maxTasks,
      String sourceRegion,
      String sinkRegion,
      String sourceTopic,
      String sinkTopic,
      String offsetPath,
      String locatorString) {
    return startWorkerAndHerderCluster(maxTasks, sourceRegion, sinkRegion, sourceTopic, sinkTopic,
        offsetPath, locatorString, DEFAULT_KEY_CONVERTER, "", DEFAULT_VALUE_CONVERTER, "");
  }

  public static WorkerAndHerderCluster startWorkerAndHerderCluster(int maxTasks,
      String sourceRegion,
      String sinkRegion,
      String sourceTopic,
      String sinkTopic,
      String offsetPath,
      String locatorString,
      String keyConverter,
      String keyConverterArgs,
      String valueConverter,
      String valueConverterArgs) {
    WorkerAndHerderCluster workerAndHerderCluster = new WorkerAndHerderCluster();
    try {
      workerAndHerderCluster.start(String.valueOf(maxTasks), sourceRegion, sinkRegion, sourceTopic,
          sinkTopic, offsetPath, locatorString, keyConverter, keyConverterArgs, valueConverter,
          valueConverterArgs);
      Thread.sleep(20000);
    } catch (Exception e) {
      throw new RuntimeException("Could not start the worker and herder cluster" + e);
    }
    return workerAndHerderCluster;
  }

  public static void verifyEventsAreConsumed(Consumer<String, String> consumer, int numEvents) {
    AtomicInteger valueReceived = new AtomicInteger(0);
    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
      for (ConsumerRecord<String, String> record : records) {
        valueReceived.incrementAndGet();
      }
      return valueReceived.get() == numEvents;
    });

  }
}
