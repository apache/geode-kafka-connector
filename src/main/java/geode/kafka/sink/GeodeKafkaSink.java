package geode.kafka.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;

import java.util.Map;

public class GeodeKafkaSink {

//
//    /** Sink properties. */
//    private Map<String, String> configProps;
//
//    /** Expected configurations. */
//    private static final ConfigDef CONFIG_DEF = new ConfigDef();
//
//    /** {@inheritDoc} */
//    @Override public String version() {
//        return AppInfoParser.getVersion();
//    }
//
//    /**
//     * A sink lifecycle method. Validates grid-specific sink properties.
//     *
//     * @param props Sink properties.
//     */
//    @Override public void start(Map<String, String> props) {
//        configProps = props;
//
//        try {
//            A.notNullOrEmpty(configProps.get(SinkConnector.TOPICS_CONFIG), "topics");
//            A.notNullOrEmpty(configProps.get(IgniteSinkConstants.CACHE_NAME), "cache name");
//            A.notNullOrEmpty(configProps.get(IgniteSinkConstants.CACHE_CFG_PATH), "path to cache config file");
//        }
//        catch (IllegalArgumentException e) {
//            throw new ConnectException("Cannot start IgniteSinkConnector due to configuration error", e);
//        }
//    }
//
//    /**
//     * Obtains a sink task class to be instantiated for feeding data into grid.
//     *
//     * @return IgniteSinkTask class.
//     */
//    @Override public Class<? extends Task> taskClass() {
//        return IgniteSinkTask.class;
//    }
//
//    /**
//     * Builds each config for <tt>maxTasks</tt> tasks.
//     *
//     * @param maxTasks Max number of tasks.
//     * @return Task configs.
//     */
//    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
//        List<Map<String, String>> taskConfigs = new ArrayList<>();
//        Map<String, String> taskProps = new HashMap<>();
//
//        taskProps.putAll(configProps);
//
//        for (int i = 0; i < maxTasks; i++)
//            taskConfigs.add(taskProps);
//
//        return taskConfigs;
//    }
//
//    /** {@inheritDoc} */
//    @Override public void stop() {
//        // No-op.
//    }
//
//    /** {@inheritDoc} */
//    @Override public ConfigDef config() {
//        return CONFIG_DEF;
//    }
}
