package geode.kafka.source;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GeodeKafkaSourceTest {

    @Test
    public void durableClientIdShouldNotBeSetIfPropertyIsNotSet() {
        GeodeKafkaSource source = new GeodeKafkaSource();
        Map<String, String> props = new HashMap<>();
        source.start(props);

    }

    @Test
    public void cqPrefixShouldBeProperlyCalculatedFromProps() {

    }
}
