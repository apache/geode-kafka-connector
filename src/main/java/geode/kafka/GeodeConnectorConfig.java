package geode.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GeodeConnectorConfig {

    //Geode Configuration
    public static final String DURABLE_CLIENT_ID_PREFIX = "durableClientId";
    public static final String DEFAULT_DURABLE_CLIENT_ID = "";
    public static final String DURABLE_CLIENT_TIME_OUT = "durableClientTimeout";
    public static final String DEFAULT_DURABLE_CLIENT_TIMEOUT = "60000";

    //GeodeKafka Specific Configuration
    public static final String TASK_ID = "GEODE_TASK_ID"; //One config per task

    public static final String CQ_PREFIX = "cqPrefix";
    public static final String DEFAULT_CQ_PREFIX = "cqForGeodeKafka";
    /**
     * Specifies which Locators to connect to Apache Geode
     */
    public static final String LOCATORS = "locators";
    public static final String DEFAULT_LOCATOR = "localhost[10334]";

    /**
     * Specifies which Regions to connect in Apache Geode
     */
    public static final String REGIONS = "regions";

    /**
     * Specifies which Topics to connect in Kafka
     */
    public static final String TOPICS = "topics";

    /**
     * Property to describe the Source Partition in a record
     */
    public static final String REGION_NAME = "regionName";  //used for Source Partition Events

    public static final String BATCH_SIZE = "geodeConnectorBatchSize";
    public static final String DEFAULT_BATCH_SIZE = "100";

    public static final String QUEUE_SIZE = "geodeConnectorQueueSize";
    public static final String DEFAULT_QUEUE_SIZE = "100000";

    public static final String LOAD_ENTIRE_REGION = "loadEntireRegion";
    public static final String DEFAULT_LOAD_ENTIRE_REGION = "false";


    private final int taskId;
    private final String durableClientId;
    private final String durableClientIdPrefix;
    private final String durableClientTimeout;
    private List<String> regionNames;
    private List<String> topics;
    private List<LocatorHostPort> locatorHostPorts;

    //just for tests
    GeodeConnectorConfig() {
        taskId = 0;
        durableClientId = "";
        durableClientIdPrefix = "";
        durableClientTimeout = "0";
    }

    public GeodeConnectorConfig(Map<String, String> connectorProperties) {
        taskId = Integer.parseInt(connectorProperties.get(TASK_ID));
        durableClientIdPrefix = connectorProperties.get(DURABLE_CLIENT_ID_PREFIX);
        if (isDurable(durableClientIdPrefix)) {
            durableClientId = durableClientIdPrefix + taskId;
        }
        else {
            durableClientId = "";
        }
        durableClientTimeout = connectorProperties.get(DURABLE_CLIENT_TIME_OUT);
        regionNames = parseNames(connectorProperties.get(GeodeConnectorConfig.REGIONS));
        topics = parseNames(connectorProperties.get(GeodeConnectorConfig.TOPICS));
        locatorHostPorts = parseLocators(connectorProperties.get(GeodeConnectorConfig.LOCATORS));
    }

    List<String> parseNames(String names) {
        return Arrays.stream(names.split(",")).map((s) -> s.trim()).collect(Collectors.toList());
    }

    List<LocatorHostPort> parseLocators(String locators) {
        return Arrays.stream(locators.split(",")).map((s) -> {
            String locatorString = s.trim();
            return parseLocator(locatorString);
        }).collect(Collectors.toList());
    }

    private LocatorHostPort parseLocator(String locatorString) {
        String[] splits = locatorString.split("\\[");
        String locator = splits[0];
        int port = Integer.parseInt(splits[1].replace("]", ""));
        return new LocatorHostPort(locator, port);
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

    public List<String> getRegionNames() {
        return regionNames;
    }

    public List<String> getTopics() {
        return topics;
    }

    public List<LocatorHostPort> getLocatorHostPorts() {
        return locatorHostPorts;
    }
}
