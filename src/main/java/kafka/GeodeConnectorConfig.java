package kafka;

public class GeodeConnectorConfig {

    //Geode Configuration
    public static final String DURABLE_CLIENT_ID_PREFIX = "DurableClientId";
    public static final String DEFAULT_DURABLE_CLIENT_ID = "";
    public static final String DURABLE_CLIENT_TIME_OUT = "DurableClientTimeout";
    public static final String DEFAULT_DURABLE_CLIENT_TIMEOUT = "60000";


    //GeodeKafka Specific Configuration
    public static final String CQ_PREFIX = "CqPrefix";
    public static final String DEFAULT_CQ_PREFIX = "CqForGeodeKafka";
    /**
     * Specifies which Locators to connect to Apache Geode
     */
    public static final String LOCATORS = "Locators";
    public static final String DEFAULT_LOCATOR = "localhost[10334]";

    /**
     * Specifies which Regions to connect in Apache Geode
     */
    public static final String REGIONS = "Regions";

    /**
     * Specifies which Topics to connect in Kafka
     */
    public static final String TOPICS = "Topics";

    /**
     * Property to describe the Source Partition in a record
     */
    public static final String REGION_NAME = "RegionName";  //used for Source Partition Events

    public static final String BATCH_SIZE = "GeodeConnectorBatchSize";
    public static final String DEFAULT_BATCH_SIZE = "100";

    public static final String QUEUE_SIZE = "GeodeConnectorQueueSize";
    public static final String DEFAULT_QUEUE_SIZE = "100000";
}
