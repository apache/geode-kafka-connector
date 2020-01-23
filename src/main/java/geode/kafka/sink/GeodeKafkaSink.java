package geode.kafka.sink;

import geode.kafka.GeodeConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

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
import static geode.kafka.GeodeConnectorConfig.DEFAULT_LOCATOR;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_QUEUE_SIZE;
import static geode.kafka.GeodeConnectorConfig.DURABLE_CLIENT_ID_PREFIX;
import static geode.kafka.GeodeConnectorConfig.DURABLE_CLIENT_TIME_OUT;
import static geode.kafka.GeodeConnectorConfig.LOCATORS;
import static geode.kafka.GeodeConnectorConfig.QUEUE_SIZE;

public class GeodeKafkaSink extends SinkConnector  {
    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private Map<String, String> sharedProps;

    @Override
    public void start(Map<String, String> props) {
        sharedProps = computeMissingConfigurations(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GeodeKafkaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();

        taskProps.putAll(sharedProps);

        for (int i = 0; i < maxTasks; i++) {
            taskProps.put(GeodeConnectorConfig.TASK_ID, "" + i);
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        //TODO
        return "unknown";
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
}
