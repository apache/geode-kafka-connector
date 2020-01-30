package geode.kafka.source;

import geode.kafka.GeodeConnectorConfig;
import geode.kafka.LocatorHostPort;

import java.util.List;
import java.util.Map;

public class GeodeSourceConnectorConfig extends GeodeConnectorConfig {

    //Geode Configuration
    public static final String DURABLE_CLIENT_ID_PREFIX = "durableClientId";
    public static final String DEFAULT_DURABLE_CLIENT_ID = "";
    public static final String DURABLE_CLIENT_TIME_OUT = "durableClientTimeout";
    public static final String DEFAULT_DURABLE_CLIENT_TIMEOUT = "60000";

    public static final String CQ_PREFIX = "cqPrefix";
    public static final String DEFAULT_CQ_PREFIX = "cqForGeodeKafka";

    /**
     * Used as a key for source partitions
     */
    public static final String REGION = "region";

    public static final String REGION_TO_TOPIC_BINDINGS = "regionToTopic";

    public static final String BATCH_SIZE = "geodeConnectorBatchSize";
    public static final String DEFAULT_BATCH_SIZE = "100";

    public static final String QUEUE_SIZE = "geodeConnectorQueueSize";
    public static final String DEFAULT_QUEUE_SIZE = "100000";

    public static final String LOAD_ENTIRE_REGION = "loadEntireRegion";
    public static final String DEFAULT_LOAD_ENTIRE_REGION = "false";

    private final String durableClientId;
    private final String durableClientIdPrefix;
    private final String durableClientTimeout;
    private final String cqPrefix;
    private final boolean loadEntireRegion;

    private Map<String, List<String>> regionToTopics;

    //just for tests
    protected GeodeSourceConnectorConfig() {
        super();
        durableClientId = "";
        durableClientIdPrefix = "";
        durableClientTimeout = "0";
        cqPrefix = DEFAULT_CQ_PREFIX;
        loadEntireRegion = Boolean.parseBoolean(DEFAULT_LOAD_ENTIRE_REGION);
    }

    public GeodeSourceConnectorConfig(Map<String, String> connectorProperties) {
        super(connectorProperties);
        regionToTopics = parseRegionToTopics(connectorProperties.get(REGION_TO_TOPIC_BINDINGS));
        durableClientIdPrefix = connectorProperties.get(DURABLE_CLIENT_ID_PREFIX);
        if (isDurable(durableClientIdPrefix)) {
            durableClientId = durableClientIdPrefix + taskId;
        } else {
            durableClientId = "";
        }
        durableClientTimeout = connectorProperties.get(DURABLE_CLIENT_TIME_OUT);
        cqPrefix = connectorProperties.get(CQ_PREFIX);
        loadEntireRegion = Boolean.parseBoolean(connectorProperties.get(LOAD_ENTIRE_REGION));
    }

    public boolean isDurable() {
        return isDurable(durableClientId);
    }

    /**
     * @param durableClientId or prefix can be passed in.  Either both will be "" or both will have a value
     * @return
     */
    boolean isDurable(String durableClientId) {
        return !durableClientId.equals("");
    }

    public int getTaskId() {
        return taskId;
    }

    public String getDurableClientId() {
        return durableClientId;
    }

    public String getDurableClientTimeout() {
        return durableClientTimeout;
    }

    public String getCqPrefix() {
        return cqPrefix;
    }

    public boolean getLoadEntireRegion() {
        return loadEntireRegion;
    }

    public Map<String, List<String>> getRegionToTopics() {
        return regionToTopics;
    }

}
