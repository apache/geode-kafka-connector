package geode.kafka.source;

import geode.kafka.GeodeConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static geode.kafka.GeodeConnectorConfig.BATCH_SIZE;
import static geode.kafka.GeodeConnectorConfig.CQ_PREFIX;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_BATCH_SIZE;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_CQ_PREFIX;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_DURABLE_CLIENT_ID;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_LOAD_ENTIRE_REGION;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_LOCATOR;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_QUEUE_SIZE;
import static geode.kafka.GeodeConnectorConfig.DURABLE_CLIENT_ID_PREFIX;
import static geode.kafka.GeodeConnectorConfig.DURABLE_CLIENT_TIME_OUT;
import static geode.kafka.GeodeConnectorConfig.LOAD_ENTIRE_REGION;
import static geode.kafka.GeodeConnectorConfig.LOCATORS;
import static geode.kafka.GeodeConnectorConfig.QUEUE_SIZE;
import static geode.kafka.GeodeConnectorConfig.REGION_TO_TOPIC_BINDINGS;


public class GeodeKafkaSource extends SourceConnector {

  private Map<String, String> sharedProps;
  //TODO maybe club this into GeodeConnnectorConfig
  private static final ConfigDef CONFIG_DEF = new ConfigDef();


  @Override
  public Class<? extends Task> taskClass() {
    return GeodeKafkaSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(sharedProps);

    List<String> bindings = GeodeConnectorConfig.parseNames(taskProps.get(REGION_TO_TOPIC_BINDINGS));
    List<List<String>> bindingsPerTask = ConnectorUtils.groupPartitions(bindings, maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      taskProps.put(GeodeConnectorConfig.TASK_ID, "" + i);
      taskProps.put(REGION_TO_TOPIC_BINDINGS, GeodeConnectorConfig.reconstructString(bindingsPerTask.get(i)));
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
    props.computeIfAbsent(LOAD_ENTIRE_REGION, (key) -> DEFAULT_LOAD_ENTIRE_REGION);
    return props;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    //TODO
    return AppInfoParser.getVersion();
  }

  public Map<String, String> getSharedProps() {
    return sharedProps;
  }
}
