package kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.IOException;
import java.util.Properties;

public class KafkaLocalCluster {

  KafkaServerStartable kafka;

  public KafkaLocalCluster(Properties kafkaProperties) throws IOException, InterruptedException {
    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
    kafka = new KafkaServerStartable(kafkaConfig);
  }

  public void start() {
    try {
      kafka.startup();
      System.out.println("Kafka started up");
    }
    catch (Throwable t) {
      System.out.println(t);
    }
  }


  public void stop() {
    kafka.shutdown();
  }
}
