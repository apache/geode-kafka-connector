package geode.kafka.sink;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GeodeKafkaSinkTest {

    @Test
    public void test() {
        GeodeKafkaSink sink = new GeodeKafkaSink();
        Map<String, String> props = new HashMap();
        sink.start(props);
    }
}
