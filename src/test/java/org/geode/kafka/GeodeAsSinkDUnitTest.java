package org.geode.kafka;

import static org.awaitility.Awaitility.await;
import static org.geode.kafka.GeodeKafkaTestUtils.createProducer;
import static org.geode.kafka.GeodeKafkaTestUtils.createTopic;
import static org.geode.kafka.GeodeKafkaTestUtils.deleteTopic;
import static org.geode.kafka.GeodeKafkaTestUtils.getKafkaConfig;
import static org.geode.kafka.GeodeKafkaTestUtils.getZooKeeperProperties;
import static org.geode.kafka.GeodeKafkaTestUtils.startKafka;
import static org.geode.kafka.GeodeKafkaTestUtils.startWorkerAndHerderCluster;
import static org.geode.kafka.GeodeKafkaTestUtils.startZooKeeper;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(Parameterized.class)
public class GeodeAsSinkDUnitTest {
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(3);

  private static MemberVM locator, server;
  private static ClientVM client;

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

  @Parameterized.Parameters(name = "tasks_{0}_partitions_{1}")
  public static Iterable<Object[]> getParams() {
    return Arrays.asList(new Object[][] {{1, 1}, {5, 10}, {15, 10}});
  }

  private final int numTask;
  private final int numPartition;

  public GeodeAsSinkDUnitTest(int numTask, int numPartition) {
    this.numTask = numTask;
    this.numPartition = numPartition;
  }

  @Test
  public void whenKafkaProducerProducesEventsThenGeodeMustReceiveTheseEvents() throws Exception {

    locator = clusterStartupRule.startLocatorVM(0, 10334);
    int locatorPort = locator.getPort();
    server = clusterStartupRule.startServerVM(1, locatorPort);
    client =
        clusterStartupRule
            .startClientVM(2, client -> client.withLocatorConnection(locatorPort));
    int NUM_EVENT = 10;

    // Set unique names for all the different components
    String testIdentifier = testName.getMethodName().replaceAll("\\[|\\]", "");
    String sourceRegion = "SOURCE_REGION_" + testIdentifier;
    String sinkRegion = "SINK_REGION_" + testIdentifier;
    String sinkTopic = "SINK_TOPIC_" + testIdentifier;
    String sourceTopic = "SOURCE_TOPIC_" + testIdentifier;

    /**
     * Start the Apache Geode cluster and create the source and sink regions.
     * Create a Apache Geode client which inserts data into the source
     */
    server.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .create(sourceRegion);
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .create(sinkRegion);
    });


    /**
     * Start the Kafka Cluster, workers and the topic to which the Apache Geode will connect as
     * a source
     */
    WorkerAndHerderCluster workerAndHerderCluster = null;
    KafkaLocalCluster kafkaLocalCluster = null;
    try {
      kafkaLocalCluster = startKafka(
          getKafkaConfig(temporaryFolderForOffset.newFolder("kafkaLogs").getAbsolutePath()));
      createTopic(sinkTopic, numPartition, 1);
      // Create workers and herder cluster
      workerAndHerderCluster = startWorkerAndHerderCluster(numTask, sourceRegion, sinkRegion,
          sourceTopic, sinkTopic, temporaryFolderForOffset.getRoot().getAbsolutePath(),
          "localhost[" + locatorPort + "]");
      client.invoke(() -> {
        ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(sinkRegion);
      });
      // Create the producer
      Producer<String, String> producer = createProducer();

      for (int i = 0; i < NUM_EVENT; i++) {
        producer.send(new ProducerRecord(sinkTopic, "KEY" + i, "VALUE" + i));
      }

      client.invoke(() -> {
        Region region = ClusterStartupRule.getClientCache().getRegion(sinkRegion);
        await().atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(10, region.sizeOnServer()));
      });


    } finally {
      // Clean up by deleting the sink topic
      deleteTopic(sinkTopic);
      if (workerAndHerderCluster != null) {
        workerAndHerderCluster.stop();
      }
      kafkaLocalCluster.stop();
    }

  }
}
