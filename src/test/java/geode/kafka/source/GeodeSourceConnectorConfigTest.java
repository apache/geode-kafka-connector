package geode.kafka.source;

import geode.kafka.GeodeConnectorConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static geode.kafka.GeodeConnectorConfig.LOCATORS;
import static geode.kafka.GeodeConnectorConfig.TASK_ID;
import static geode.kafka.source.GeodeSourceConnectorConfig.DURABLE_CLIENT_ID_PREFIX;
import static org.junit.Assert.assertEquals;

public class GeodeSourceConnectorConfigTest {

    @Test
    public void durableClientIdShouldNotBeSetIfPrefixIsEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put(TASK_ID, "0");
        props.put(DURABLE_CLIENT_ID_PREFIX, "");
        props.put(LOCATORS, "localhost[10334]");
        GeodeSourceConnectorConfig config = new GeodeSourceConnectorConfig(props);
        assertEquals("", config.getDurableClientId());
    }

}
