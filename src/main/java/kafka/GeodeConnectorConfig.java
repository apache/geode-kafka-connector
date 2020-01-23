package kafka;

public class GeodeConnectorConfig {

    //Geode Configuration
    public static final String DURABLE_CLIENT_ID_PREFIX = "durableClientId";
    public static final String DEFAULT_DURABLE_CLIENT_ID = "";
    public static final String DURABLE_CLIENT_TIME_OUT = "durableClientTimeout";
    public static final String DEFAULT_DURABLE_CLIENT_TIMEOUT = "60000";


    //GeodeKafka Specific Configuration
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
}
