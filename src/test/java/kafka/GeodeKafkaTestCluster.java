package kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.I0Itec.zkclient.ZkClient;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class GeodeKafkaTestCluster {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static boolean debug = true;

  private static ZooKeeperLocalCluster zooKeeperLocalCluster;
  private static KafkaLocalCluster kafkaLocalCluster;
  private static GeodeLocalCluster geodeLocalCluster;
  private static WorkerAndHerderCluster workerAndHerderCluster;
  private static Consumer<String, String> consumer;

  @BeforeClass
  public static void setup() throws IOException, QuorumPeerConfig.ConfigException, InterruptedException {
    startZooKeeper();
    startKafka();
    startGeode();
    createTopic();
    Thread.sleep(5000);
    startWorker();
    consumer = createConsumer();
    Thread.sleep(5000);
  }

  @AfterClass
  public static void shutdown() {
    workerAndHerderCluster.stop();
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181",false,200000,
            15000,10, Time.SYSTEM, "myGroup","myMetricType", null);

    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.deleteTopic("someTopic");

    kafkaLocalCluster.stop();
    geodeLocalCluster.stop();
  }


  private static void startWorker() throws IOException, InterruptedException {
    workerAndHerderCluster = new WorkerAndHerderCluster();
    workerAndHerderCluster.start();
    Thread.sleep(20000);
  }

  private static void createTopic() {
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181",false,200000,
            15000,10, Time.SYSTEM, "myGroup","myMetricType", null);
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.createTopic("someTopic",3,1, new Properties(), RackAwareMode.Disabled$.MODULE$);
  }

  private ClientCache createGeodeClient() {
    return new ClientCacheFactory().addPoolLocator("localhost", 10334).create();
  }

  private static void startZooKeeper() throws IOException, QuorumPeerConfig.ConfigException {
    zooKeeperLocalCluster = new ZooKeeperLocalCluster(getZooKeeperProperties());
    zooKeeperLocalCluster.start();
  }

  private static void startKafka() throws IOException, InterruptedException, QuorumPeerConfig.ConfigException {
    kafkaLocalCluster = new KafkaLocalCluster(getKafkaConfig());
    kafkaLocalCluster.start();
  }

  private static void startGeode() throws IOException, InterruptedException {
    geodeLocalCluster = new GeodeLocalCluster();
    geodeLocalCluster.start();
  }

  private static  Properties getZooKeeperProperties() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("dataDir", (debug)? "/tmp/zookeeper" :temporaryFolder.newFolder("zookeeper").getAbsolutePath());
    properties.setProperty("clientPort", "2181");
    properties.setProperty("tickTime", "2000");
    return properties;
  }


  private static Properties getKafkaConfig() throws IOException {
    int BROKER_PORT = 8888;
    Properties props = new Properties();

    props.put("broker.id", "0");
    props.put("zookeeper.connect", "localhost:2181");
    props.put("host.name", "localHost");
    props.put("port", BROKER_PORT);
    props.put("offsets.topic.replication.factor", "1");
    props.put("log.dir", (debug)? "/tmp/kafka" : temporaryFolder.newFolder("kafka").getAbsolutePath());
    props.put("log.flush.interval.messages", "1");
    props.put("log.flush.interval.ms", "10");


    //Connector configs
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GeodeKafkaSource.class.getName());
    props.put(ConnectorConfig.NAME_CONFIG, "geode-kafka-source-connector");
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");

    //Specifically GeodeKafka connector configs



/*
name=file-source
# The class implementing the connector
connector.class=FileStreamSource
# Maximum number of tasks to run for this connector instance
tasks.max=1
# The input file (path relative to worker's working directory)
# This is the only setting specific to the FileStreamSource
file=test.txt
# The output topic in Kafka
topic=connect-test
 */

    return props;
  }


  //consumer props, less important, just for testing?
  public static Consumer<String,String> createConsumer() {
      final Properties props = new Properties();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8888");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "myGroup");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class.getName());
      // Create the consumer using props.
      final Consumer<String, String> consumer =
              new KafkaConsumer<>(props);
      // Subscribe to the topic.
      consumer.subscribe(Collections.singletonList("someTopic"));
      return consumer;
  }

  @Test
  public void testX() throws InterruptedException {
    ClientCache client = createGeodeClient();
    Region region = client.createClientRegionFactory(ClientRegionShortcut.PROXY).create("someRegion");
    region.put("JASON KEY", "JASON VALUE");
    System.out.println("PUT COMPLETE!");
    region.get("JASON KEY");
//    client.close();


    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
    for (ConsumerRecord<String, String> record: records) {
      System.out.println("JASON we consumed a record:" + record);
    }
    System.out.println("TEST COMPLETE!");

  }

}
