package kafka;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;

public class GeodeKafkaTestCluster {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static boolean debug = true;

  private static ZooKeeperLocalCluster zooKeeperLocalCluster;
  private static KafkaLocalCluster kafkaLocalCluster;
  private static GeodeLocalCluster geodeLocalCluster;

  @BeforeClass
  public static void setup() throws IOException, QuorumPeerConfig.ConfigException, InterruptedException {
    startZooKeeper();
    startKafka();
    startGeode();
  }

  @AfterClass
  public static void shutdown() {
    kafkaLocalCluster.stop();
    geodeLocalCluster.stop();
  }

  private ClientCache createGeodeClient() {
    return new ClientCacheFactory().addPoolLocator("127.0.0.1", 10334).create();
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
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "2");
        props.put(ConnectorConfig.NAME_CONFIG, "test-src-connector");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, IgniteSourceConnectorMock.class.getName());
        props.put(IgniteSourceConstants.CACHE_NAME, "testCache");
        props.put(IgniteSourceConstants.CACHE_CFG_PATH, "example-ignite.xml");
        props.put(IgniteSourceConstants.TOPIC_NAMES, topics);
        props.put(IgniteSourceConstants.CACHE_EVENTS, "put");
        props.put(IgniteSourceConstants.CACHE_FILTER_CLASS, TestCacheEventFilter.class.getName());
        props.put(IgniteSourceConstants.INTL_BUF_SIZE, "1000000");
     */

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



  @Test
  public void testX() throws InterruptedException {
    ClientCache client = createGeodeClient();
    Region region = client.createClientRegionFactory(ClientRegionShortcut.PROXY).create("someRegion");
    region.put("JASON KEY", "JASON VALUE");
    System.out.println("TEST COMPLETE!");
  }

}
