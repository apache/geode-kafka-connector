package geode.kafka.source;

import geode.kafka.GeodeConnectorConfig;
import geode.kafka.GeodeContext;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static geode.kafka.GeodeConnectorConfig.BATCH_SIZE;
import static geode.kafka.GeodeConnectorConfig.CQ_PREFIX;
import static geode.kafka.GeodeConnectorConfig.DEFAULT_BATCH_SIZE;
import static geode.kafka.GeodeConnectorConfig.LOAD_ENTIRE_REGION;
import static geode.kafka.GeodeConnectorConfig.QUEUE_SIZE;
import static geode.kafka.GeodeConnectorConfig.REGION_NAME;

public class GeodeKafkaSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(GeodeKafkaSourceTask.class);

    private static final String TASK_PREFIX = "TASK";
    private static final String DOT = ".";

    //property string to pass in to identify task
    private static final Map<String, Long> OFFSET_DEFAULT = createOffset();

    private GeodeContext geodeContext;
    private GeodeConnectorConfig geodeConnectorConfig;
    private List<String> topics;
    private Map<String, Map<String, String>> sourcePartitions;
    private BlockingQueue<GeodeEvent> eventBuffer;
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

    void startForTesting(BlockingQueue eventBuffer, List<String> topics, int batchSize) {
        this.eventBuffer = eventBuffer;
        this.topics = topics;
        this.batchSize = batchSize;
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            geodeConnectorConfig = new GeodeConnectorConfig(props);
            logger.debug("GeodeKafkaSourceTask id:" + geodeConnectorConfig.getTaskId() + " starting");
            geodeContext = new GeodeContext(geodeConnectorConfig);

            batchSize = Integer.parseInt(props.get(BATCH_SIZE));
            int queueSize = Integer.parseInt(props.get(QUEUE_SIZE));
            eventBuffer = new LinkedBlockingQueue<>(queueSize);

            sourcePartitions = createSourcePartitionsMap(geodeConnectorConfig.getRegionNames());
            topics = geodeConnectorConfig.getTopics();

            String cqPrefix = props.get(CQ_PREFIX);
            boolean loadEntireRegion = Boolean.parseBoolean(props.get(LOAD_ENTIRE_REGION));

            installOnGeode(geodeConnectorConfig, geodeContext, eventBuffer, cqPrefix, loadEntireRegion);
        }
        catch (Exception e) {
            logger.error("Unable to start source task", e);
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
                    records.add(new SourceRecord(sourcePartitions.get(event.getRegionName()), OFFSET_DEFAULT, topic, null, event.getEvent().getNewValue()));
                }
            }
            return records;
        }

        return null;
    }

    @Override
    public void stop() {
        geodeContext.getClientCache().close(true);
    }

    void installOnGeode(GeodeConnectorConfig geodeConnectorConfig, GeodeContext geodeContext, BlockingQueue eventBuffer, String cqPrefix, boolean loadEntireRegion) {
      boolean isDurable = geodeConnectorConfig.isDurable();
      int taskId = geodeConnectorConfig.getTaskId();
        for (String region : geodeConnectorConfig.getRegionNames()) {
            installListenersToRegion(geodeContext, taskId, eventBuffer, region, cqPrefix, loadEntireRegion, isDurable);
        }
        if (isDurable) {
            geodeContext.getClientCache().readyForEvents();
        }
    }

    GeodeKafkaSourceListener installListenersToRegion(GeodeContext geodeContext, int taskId, BlockingQueue<GeodeEvent> eventBuffer, String regionName, String cqPrefix, boolean loadEntireRegion, boolean isDurable) {
        CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
        GeodeKafkaSourceListener listener = new GeodeKafkaSourceListener(eventBuffer, regionName);
        cqAttributesFactory.addCqListener(listener);
        CqAttributes cqAttributes = cqAttributesFactory.create();
        try {
            if (loadEntireRegion) {
                Collection<CqEvent> events = geodeContext.newCqWithInitialResults(generateCqName(taskId, cqPrefix, regionName), "select * from /" + regionName, cqAttributes,
                        isDurable);
                eventBuffer.addAll(events.stream().map(e -> new GeodeEvent(regionName, e)).collect(Collectors.toList()));
            } else {
                geodeContext.newCq(generateCqName(taskId, cqPrefix, regionName), "select * from /" + regionName, cqAttributes,
                        isDurable);
            }
        }
        finally {
            listener.signalInitialResultsLoaded();
        }
        return listener;
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

    String generateCqName(int taskId, String cqPrefix, String regionName) {
        return cqPrefix + DOT + TASK_PREFIX + taskId + DOT + regionName;
    }
}
