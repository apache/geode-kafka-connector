package kafka;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static kafka.GeodeConnectorConfig.BATCH_SIZE;
import static kafka.GeodeConnectorConfig.CQ_PREFIX;
import static kafka.GeodeConnectorConfig.DEFAULT_CQ_PREFIX;
import static kafka.GeodeConnectorConfig.DEFAULT_DURABLE_CLIENT_ID;
import static kafka.GeodeConnectorConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT;
import static kafka.GeodeConnectorConfig.DURABLE_CLIENT_ID_PREFIX;
import static kafka.GeodeConnectorConfig.DURABLE_CLIENT_TIME_OUT;
import static kafka.GeodeConnectorConfig.QUEUE_SIZE;
import static kafka.GeodeConnectorConfig.REGION_NAME;

public class GeodeKafkaSourceTask extends SourceTask {

    //property string to pass in to identify task
    public static final String TASK_ID = "GEODE_TASK_ID";
    private static final String TASK_PREFIX = "TASK";
    private static final String DOT = ".";
    private static final Map<String, Long> OFFSET_DEFAULT = createOffset();

    private int taskId;
    private ClientCache clientCache;
    private List<String> regionNames;
    private List<String> topics;
    private Map<String, Map<String, String>> sourcePartitions;
    private static BlockingQueue<GeodeEvent> eventBuffer;
    private int batchSize;


    private static Map<String, Long> createOffset() {
        Map<String, Long> offset = new HashMap<>();
        offset.put("OFFSET", 0L);
        return offset;
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            System.out.println("JASON task start");
            taskId = Integer.parseInt(props.get(TASK_ID));
            batchSize = Integer.parseInt(props.get(BATCH_SIZE));
            int queueSize = Integer.parseInt(props.get(QUEUE_SIZE));
            eventBuffer = new LinkedBlockingQueue<>(queueSize);

            //grouping will be done in the source and not the task
            regionNames = parseNames(props.get(GeodeConnectorConfig.REGIONS));
            topics = parseNames(props.get(GeodeConnectorConfig.TOPICS));
            sourcePartitions = createSourcePartitionsMap(regionNames);

            String durableClientId = props.get(DURABLE_CLIENT_ID_PREFIX);
            if (!durableClientId.equals("")) {
                durableClientId += taskId;
            }
            System.out.println("JASON durable client id is:" + durableClientId);
            String durableClientTimeout = props.get(DURABLE_CLIENT_TIME_OUT);
            String cqPrefix = props.get(CQ_PREFIX);

            List<LocatorHostPort> locators = parseLocators(props.get(GeodeConnectorConfig.LOCATORS));
            installOnGeode(taskId, eventBuffer, locators, regionNames, durableClientId, durableClientTimeout, cqPrefix);
            System.out.println("JASON task start finished");
        }
        catch (Exception e) {
            System.out.println("Exception:" + e);
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<>(batchSize);
        ArrayList<GeodeEvent> events = new ArrayList<>(batchSize);
        if (eventBuffer.drainTo(events, batchSize) > 0) {
            for (GeodeEvent event : events) {
                for (String topic : topics) {
                    records.add(new SourceRecord(sourcePartitions.get(event.getRegionName()), OFFSET_DEFAULT, topic, null, event.getEvent()));
//                    records.add(new SourceRecord(sourcePartitions.get(event.getRegionName()), OFFSET_DEFAULT, topic, null, "STRING"));
                }
            }

            return records;
        }

        return null;
    }

    @Override
    public void stop() {
        clientCache.close(true);
    }

    ClientCache createClientCache(List<LocatorHostPort> locators, String durableClientName, String durableClientTimeOut) {
        ClientCacheFactory ccf = new ClientCacheFactory().set("durable-client-id", durableClientName)
                .set("durable-client-timeout", durableClientTimeOut)
                .setPoolSubscriptionEnabled(true);
        for (LocatorHostPort locator: locators) {
          ccf.addPoolLocator(locator.getHostName(), locator.getPort()).create();
        }
        return ccf.create();
    }

    void installOnGeode(int taskId, BlockingQueue<GeodeEvent> eventBuffer, List<LocatorHostPort> locators, List<String> regionNames, String durableClientId, String durableClientTimeout, String cqPrefix) {
      boolean isDurable = isDurable(durableClientId);

      clientCache = createClientCache(locators, durableClientId, durableClientTimeout);
        for (String region : regionNames) {
            installListenersToRegion(taskId, eventBuffer, region, cqPrefix, isDurable);
        }
        if (isDurable) {
          clientCache.readyForEvents();
        }
    }

    void installListenersToRegion(int taskId, BlockingQueue<GeodeEvent> eventBuffer, String regionName, String cqPrefix, boolean isDurable) {
        CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
        cqAttributesFactory.addCqListener(new GeodeKafkaSourceListener(eventBuffer, regionName));
        System.out.println("JASON installing on Geode");
        CqAttributes cqAttributes = cqAttributesFactory.create();
        try {
            System.out.println("JASON installing new cq");
            clientCache.getQueryService().newCq(generateCqName(taskId, cqPrefix, regionName), "select * from /" + regionName, cqAttributes,
                    isDurable).execute();
            System.out.println("JASON finished installing cq");
        } catch (CqExistsException e) {
            System.out.println("UHH");
            e.printStackTrace();
        } catch (CqException | RegionNotFoundException e) {
            System.out.println("UHH e");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("UHHHHHH " + e);
        }
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

    boolean isDurable(String durableClientId) {
      return !durableClientId.equals("");
    }

    String generateCqName(int taskId, String cqPrefix, String regionName) {
      return cqPrefix + DOT + TASK_PREFIX + taskId + DOT + regionName;
    }

    /**
     * converts a list of regions names into a map of source partitions
     *
     * @param regionNames list of regionNames
     * @return Map<String, Map < String, String>> a map of source partitions, keyed by region name
     */
    Map<String, Map<String, String>> createSourcePartitionsMap(List<String> regionNames) {
        return regionNames.stream().map(regionName -> {
            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put(REGION_NAME, regionName);
            return sourcePartition;
        }).collect(Collectors.toMap(s -> s.get(REGION_NAME), s -> s));
    }
}
