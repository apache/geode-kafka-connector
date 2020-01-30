package geode.kafka;

import java.util.Arrays;
import java.util.Collection;
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
     * Specifies which Topics to connect in Kafka, uses the variable name with Kafka Sink Configuration
     * Only used in sink configuration
     */
    public static final String TOPICS = "topics";

    //Used by sink
    public static final String TOPIC_TO_REGION_BINDINGS = "topicToRegion";

    //Used by source
    public static final String REGION_TO_TOPIC_BINDINGS = "regionToTopic";

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

    private Map<String, List<String>> regionToTopics;
    private Map<String, List<String>> topicToRegions;
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
        } else {
            durableClientId = "";
        }
        durableClientTimeout = connectorProperties.get(DURABLE_CLIENT_TIME_OUT);
        regionToTopics = parseRegionToTopics(connectorProperties.get(GeodeConnectorConfig.REGION_TO_TOPIC_BINDINGS));
        topicToRegions = parseTopicToRegions(connectorProperties.get(GeodeConnectorConfig.TOPIC_TO_REGION_BINDINGS));
        locatorHostPorts = parseLocators(connectorProperties.get(GeodeConnectorConfig.LOCATORS));
    }


    public static Map<String, List<String>> parseTopicToRegions(String combinedBindings) {
        //It's the same formatting, so parsing is the same going topic to region or region to topic
        return parseRegionToTopics(combinedBindings);
    }

    /**
     * Given a string of the form [region:topic,...] will produce a map where the key is the
     * regionName and the value is a list of topicNames to push values to
     *
     * @param combinedBindings a string with similar form to "[region:topic,...], [region2:topic2,...]
     * @return mapping of regionName to list of topics to update
     */
    public static Map<String, List<String>> parseRegionToTopics(String combinedBindings) {
        if (combinedBindings == "" || combinedBindings == null){
            return null;
        }
        List<String> bindings = parseBindings(combinedBindings);
        return bindings.stream().map(binding -> {
            String[] regionToTopicsArray = parseBinding(binding);
            return regionToTopicsArray;
        }).collect(Collectors.toMap(regionToTopicsArray -> regionToTopicsArray[0], regionToTopicsArray -> parseNames(regionToTopicsArray[1])));
    }

    public static List<String> parseBindings(String bindings) {
        return Arrays.stream(bindings.split("](\\s)*,")).map((s) -> {
            s = s.replaceAll("\\[", "");
            s = s.replaceAll("\\]", "");
            s = s.trim();
            return s;
        }).collect(Collectors.toList());
    }

    private static String[] parseBinding(String binding) {
        return binding.split(":");
    }

    //Used to parse a string of topics or regions
    public static List<String> parseNames(String names) {
        return Arrays.stream(names.split(",")).map((s) -> s.trim()).collect(Collectors.toList());
    }

    public static String reconstructString(Collection<String> strings) {
        return strings.stream().collect(Collectors.joining("],[")) + "]";
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

    public List<LocatorHostPort> getLocatorHostPorts() {
        return locatorHostPorts;
    }

    public Map<String, List<String>> getRegionToTopics() {
        return regionToTopics;
    }

    public Map<String, List<String>> getTopicToRegions() {
        return topicToRegions;
    }

}
