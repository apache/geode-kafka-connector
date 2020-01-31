package geode.kafka;

import java.util.List;
import java.util.Map;

public class GeodeSinkConnectorConfig extends GeodeConnectorConfig {
    //Used by sink
    public static final String TOPIC_TO_REGION_BINDINGS = "topicToRegions";
    public static final String NULL_VALUES_MEAN_REMOVE = "nullValuesMeanRemove";
    public static final String DEFAULT_NULL_VALUES_MEAN_REMOVE = "true";

    private Map<String, List<String>> topicToRegions;
    private final boolean nullValuesMeanRemove;

    //just for tests
    GeodeSinkConnectorConfig() {
        super();
        nullValuesMeanRemove = Boolean.parseBoolean(DEFAULT_NULL_VALUES_MEAN_REMOVE);
    }

    public GeodeSinkConnectorConfig(Map<String, String> connectorProperties) {
        super(connectorProperties);
       topicToRegions = parseTopicToRegions(connectorProperties.get(TOPIC_TO_REGION_BINDINGS));
       nullValuesMeanRemove = Boolean.parseBoolean(connectorProperties.get(NULL_VALUES_MEAN_REMOVE));
    }

    public Map<String, List<String>> getTopicToRegions() {
        return topicToRegions;
    }

    public boolean getNullValuesMeanRemove() {
        return nullValuesMeanRemove;
    }

}
