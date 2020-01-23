package geode.kafka;

import geode.kafka.source.GeodeKafkaSource;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static geode.kafka.GeodeConnectorConfig.REGIONS;
import static geode.kafka.GeodeConnectorConfig.TOPICS;
import static geode.kafka.GeodeKafkaTestCluster.TEST_REGIONS;
import static geode.kafka.GeodeKafkaTestCluster.TEST_TOPICS;

public class WorkerAndHerderWrapper {

    public static void main(String[] args) throws IOException {
        Map props = new HashMap();
        props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("offset.storage.file.filename", "/tmp/connect.offsets");
        // fast flushing for testing.
        props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "10");


        props.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put("internal.key.converter.schemas.enable", "false");
        props.put("internal.value.converter.schemas.enable", "false");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put("key.converter.schemas.enable", "false");
        props.put("value.converter.schemas.enable", "false");

        WorkerConfig workerCfg = new StandaloneConfig(props);

        MemoryOffsetBackingStore offBackingStore = new MemoryOffsetBackingStore();
        offBackingStore.configure(workerCfg);

        Worker worker = new Worker("WORKER_ID", new SystemTime(), new Plugins(props), workerCfg, offBackingStore, new AllConnectorClientConfigOverridePolicy());
        worker.start();

        Herder herder = new StandaloneHerder(worker, ConnectUtils.lookupKafkaClusterId(workerCfg), new AllConnectorClientConfigOverridePolicy());
        herder.start();

        Map<String, String> sourceProps = new HashMap<>();
        sourceProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GeodeKafkaSource.class.getName());
        sourceProps.put(ConnectorConfig.NAME_CONFIG, "geode-kafka-source-connector");
        sourceProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        sourceProps.put(REGIONS, TEST_REGIONS);
        sourceProps.put(TOPICS, TEST_TOPICS);

        herder.putConnectorConfig(
                sourceProps.get(ConnectorConfig.NAME_CONFIG),
                sourceProps, true, (error, result)->{
                    System.out.println("CALLBACK: " + result + "::: error?" + error);
                });

    }
}
