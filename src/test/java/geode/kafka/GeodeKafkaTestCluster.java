package geode.kafka;

import kafka.admin.RackAwareMode;
import geode.kafka.source.GeodeKafkaSource;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

public class GeodeKafkaTestCluster {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static boolean debug = true;

  public static String TEST_TOPICS = "someTopic";
  public static String TEST_REGIONS = "someRegion";

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
    startWorker();
    consumer = createConsumer();
  }

  @Before
  public void beforeTests() {
  }

  @After
  public void afterTests() {

  }

  @AfterClass
  public static void shutdown() {
    workerAndHerderCluster.stop();
    KafkaZkClient zkClient = KafkaZkClient.apply("localhost:2181",false,200000,
            15000,10, Time.SYSTEM, "myGroup","myMetricType", null);
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.deleteTopic(TEST_TOPICS);

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

    Properties topicProperties = new Properties();
    topicProperties.put("flush.messages", "1");
    AdminZkClient adminZkClient = new AdminZkClient(zkClient);
    adminZkClient.createTopic(TEST_TOPICS,1
            ,1, topicProperties, RackAwareMode.Disabled$.MODULE$);
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
    int BROKER_PORT = 9092;
    Properties props = new Properties();

    props.put("broker.id", "0");
    props.put("log4j.configuration", "/Users/jhuynh/Pivotal/kafka/config/connect-log4j.properties");
    props.put("zookeeper.connect", "localhost:2181");
    props.put("host.name", "localHost");
    props.put("port", BROKER_PORT);
    props.put("offsets.topic.replication.factor", "1");
    props.put("log.flush.interval.messages", "1");
    props.put("log.flush.interval.ms", "10");


    //Connector configs
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GeodeKafkaSource.class.getName());
    props.put(ConnectorConfig.NAME_CONFIG, "geode-kafka-source-connector");
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");

    //Specifically GeodeKafka connector configs
    return props;
  }


  //consumer props, less important, just for testing?
  public static Consumer<String,String> createConsumer() {
      final Properties props = new Properties();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
      consumer.subscribe(Collections.singletonList(TEST_TOPICS));
      return consumer;
  }

  //consumer props, less important, just for testing?
  public static Producer<String,String> createProducer() {
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

  @Test
  public void endToEndSourceTest() {
    ClientCache client = createGeodeClient();
    Region region = client.createClientRegionFactory(ClientRegionShortcut.PROXY).create(TEST_REGIONS);

    //right now just verify something makes it end to end
    AtomicInteger valueReceived = new AtomicInteger(0);
    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      region.put("KEY", "VALUE" + System.currentTimeMillis());
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(4));
      for (ConsumerRecord<String, String> record: records) {
//        System.out.println("WE consumed a record:" + record);
        valueReceived.incrementAndGet();
      }
      return valueReceived.get() > 0;
    });
  }

  @Test
  public void endToEndSinkTest() {
    ClientCache client = createGeodeClient();
    Region region = client.createClientRegionFactory(ClientRegionShortcut.PROXY).create(TEST_REGIONS);

    Producer<String, String> producer = createProducer();
    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord(TEST_TOPICS, "KEY" + i, "VALUE" + i));
    }

    int i = 0;
    await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertNotNull(region.get("KEY" + i)));
  }

}
