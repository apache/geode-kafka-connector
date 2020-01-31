/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package geode.kafka.sink;

import geode.kafka.GeodeContext;
import geode.kafka.GeodeSinkConnectorConfig;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private GeodeContext geodeContext;
    private Map<String, List<String>> topicToRegions;
    private Map<String, Region> regionNameToRegion;
    private boolean nullValuesMeansRemove = true;

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
            GeodeSinkConnectorConfig geodeConnectorConfig = new GeodeSinkConnectorConfig(props);
            logger.debug("GeodeKafkaSourceTask id:" + geodeConnectorConfig.getTaskId() + " starting");
            geodeContext = new GeodeContext();
            geodeContext.connectClient(geodeConnectorConfig.getLocatorHostPorts());
            topicToRegions = geodeConnectorConfig.getTopicToRegions();
            regionNameToRegion = createProxyRegions(topicToRegions.values());
            nullValuesMeansRemove = geodeConnectorConfig.getNullValuesMeanRemove();
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