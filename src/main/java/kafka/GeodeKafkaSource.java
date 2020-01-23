package kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kafka.GeodeConnectorConfig.BATCH_SIZE;
import static kafka.GeodeConnectorConfig.CQ_PREFIX;
import static kafka.GeodeConnectorConfig.DEFAULT_BATCH_SIZE;
import static kafka.GeodeConnectorConfig.DEFAULT_CQ_PREFIX;
import static kafka.GeodeConnectorConfig.DEFAULT_DURABLE_CLIENT_ID;
import static kafka.GeodeConnectorConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT;
import static kafka.GeodeConnectorConfig.DEFAULT_LOCATOR;
import static kafka.GeodeConnectorConfig.DEFAULT_QUEUE_SIZE;
import static kafka.GeodeConnectorConfig.DURABLE_CLIENT_ID_PREFIX;
import static kafka.GeodeConnectorConfig.DURABLE_CLIENT_TIME_OUT;
import static kafka.GeodeConnectorConfig.LOCATORS;
import static kafka.GeodeConnectorConfig.QUEUE_SIZE;
import static kafka.GeodeConnectorConfig.REGIONS;
import static kafka.GeodeConnectorConfig.TOPICS;
import static kafka.GeodeKafkaSourceTask.TASK_ID;

public class GeodeKafkaSource extends SourceConnector {

  private Map<String, String> sharedProps;
  private static final ConfigDef CONFIG_DEF = new ConfigDef();


  @Override
  public Class<? extends Task> taskClass() {
    return GeodeKafkaSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    System.out.println("GKSource: taskConfigs");
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();

    taskProps.putAll(sharedProps);

    // use the same props for all tasks at the moment
    for (int i = 0; i < maxTasks; i++) {
    //TODO partition regions and topics
      taskProps.put(TASK_ID, "" + i);
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }


  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void start(Map<String, String> props) {
    sharedProps = computeMissingConfigurations(props);
  }

  private Map<String, String> computeMissingConfigurations(Map<String, String> props) {
    props.computeIfAbsent(LOCATORS, (key)-> DEFAULT_LOCATOR);
    props.computeIfAbsent(DURABLE_CLIENT_TIME_OUT, (key) -> DEFAULT_DURABLE_CLIENT_TIMEOUT);
    props.computeIfAbsent(DURABLE_CLIENT_ID_PREFIX, (key) -> DEFAULT_DURABLE_CLIENT_ID);
    props.computeIfAbsent(BATCH_SIZE, (key) -> DEFAULT_BATCH_SIZE);
    props.computeIfAbsent(QUEUE_SIZE, (key) -> DEFAULT_QUEUE_SIZE);
    props.computeIfAbsent(CQ_PREFIX, (key) -> DEFAULT_CQ_PREFIX);
    return props;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  public Map<String, String> getSharedProps() {
    return sharedProps;
  }
}
