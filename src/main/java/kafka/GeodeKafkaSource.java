package kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeodeKafkaSource extends SourceConnector {

  public static String REGION_NAME = "GEODE_REGION_NAME";
  private String regionName;
  private static String TOPICS = "TOPICS";

  private Map<String, String> sharedProps;
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

    // use the same props for all tasks at the moment
    for (int i = 0; i < maxTasks; i++)
      taskConfigs.add(taskProps);

    return taskConfigs;
  }


  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void start(Map<String, String> props) {
    sharedProps = props;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }
}
