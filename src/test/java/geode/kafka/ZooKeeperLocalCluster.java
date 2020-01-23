package geode.kafka;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.util.Properties;

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
