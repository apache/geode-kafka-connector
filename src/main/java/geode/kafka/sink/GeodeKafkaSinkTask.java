package geode.kafka.sink;

import geode.kafka.GeodeConnectorConfig;
import geode.kafka.GeodeContext;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TODO javaDoc
 * Currently force 1 region per task
 */
public class GeodeKafkaSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(GeodeKafkaSinkTask.class);

    GeodeContext geodeContext;
    Map<String, List<String>> topicToRegions;
    Map<String, Region> regionNameToRegion;
    boolean nullValuesMeansRemove = true;

    /**
     * {@inheritDoc}
     */
    @Override
    public String version() {
        //TODO
        return "unknown";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            GeodeConnectorConfig geodeConnectorConfig = new GeodeConnectorConfig(props);
            logger.debug("GeodeKafkaSourceTask id:" + geodeConnectorConfig.getTaskId() + " starting");
            geodeContext = new GeodeContext(geodeConnectorConfig);
            topicToRegions = geodeConnectorConfig.getTopicToRegions();
            regionNameToRegion = createProxyRegions(topicToRegions.values());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unable to start sink task", e);
            throw e;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        //spin off a new thread to handle this operation?  Downside is ordering and retries...
        Map<String, BatchRecords> batchRecordsMap = new HashMap<>();
        for (SinkRecord record : records) {
            updateRegionsByTopic(record, batchRecordsMap);
        }
        batchRecordsMap.entrySet().stream().forEach((entry) -> {
            String region = entry.getKey();
            BatchRecords batchRecords = entry.getValue();
            batchRecords.executeOperations(regionNameToRegion.get(region));
        });
    }

    private void updateRegionsByTopic(SinkRecord sinkRecord, Map<String, BatchRecords> batchRecordsMap) {
        Collection<String> regionsToUpdate = topicToRegions.get(sinkRecord.topic());
        for (String region : regionsToUpdate) {
            updateBatchRecordsForRecord(sinkRecord, batchRecordsMap, region);
        }
    }

    private void updateBatchRecordsForRecord(SinkRecord record, Map<String, BatchRecords> batchRecordsMap, String region) {
        BatchRecords batchRecords = batchRecordsMap.get(region);
        if (batchRecords == null) {
            batchRecords = new BatchRecords();
            batchRecordsMap.put(region, batchRecords);
        }
        if (record.key() != null) {
            if (record.value() == null && nullValuesMeansRemove) {
                batchRecords.addRemoveOperation(record);
            } else {
                batchRecords.addUpdateOperation(record, nullValuesMeansRemove);
            }
        } else {
            //Invest in configurable auto key generator?
            logger.warn("Unable to push to Geode, missing key in record : " + record.value());
        }
    }

    private Map<String, Region> createProxyRegions(Collection<List<String>> regionNames) {
        List<String> flat = regionNames.stream().flatMap(List::stream).collect(Collectors.toList());
        return flat.stream().map(regionName -> createProxyRegion(regionName)).collect(Collectors.toMap(region->region.getName(), region -> region));
    }

    private Region createProxyRegion(String regionName) {
        return geodeContext.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
    }

    @Override
    public void stop() {
        geodeContext.getClientCache().close(false);
    }

}