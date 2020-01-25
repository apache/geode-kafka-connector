package geode.kafka;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GeodeConnectorConfigTest {

    @Test
    public void parseRegionNamesShouldSplitOnComma() {
        GeodeConnectorConfig config = new GeodeConnectorConfig();
        List<String> regionNames = config.parseNames("region1,region2,region3,region4");
        assertEquals(4, regionNames.size());
        assertThat(true, allOf(is(regionNames.contains("region1"))
                , is(regionNames.contains("region2"))
                , is(regionNames.contains("region3"))
                , is(regionNames.contains("region4"))));
    }

    @Test
    public void parseRegionNamesShouldChomp() {
        GeodeConnectorConfig config = new GeodeConnectorConfig();
        List<String> regionNames = config.parseNames("region1, region2, region3,region4");
        assertEquals(4, regionNames.size());
        assertThat(true, allOf(is(regionNames instanceof List)
                , is(regionNames.contains("region1"))
                , is(regionNames.contains("region2"))
                , is(regionNames.contains("region3"))
                , is(regionNames.contains("region4"))));
    }

    @Test
    public void shouldBeAbleToParseGeodeLocatorStrings() {
        GeodeConnectorConfig config = new GeodeConnectorConfig();
        String locatorString="localhost[8888], localhost[8881]";
        List<LocatorHostPort> locators = config.parseLocators(locatorString);
        assertThat(2, is(locators.size()));
    }

    @Test
    public void durableClientIdShouldNotBeSetIfPropertyIsNotSet() {
        Map<String, String> props = new HashMap<>();
        GeodeConnectorConfig config = new GeodeConnectorConfig(props);
        assertEquals("", config.getDurableClientId());
    }

    @Test
    public void cqPrefixShouldBeProperlyCalculatedFromProps() {

    }
}
