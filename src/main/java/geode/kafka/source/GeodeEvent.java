package geode.kafka.source;

import org.apache.geode.cache.query.CqEvent;

/**
 * wrapper class to store regionName and cq event so the correct topics can be updated
 */
public class GeodeEvent {

    private String regionName;
    private CqEvent event;

    public GeodeEvent(String regionName, CqEvent event) {
        this.regionName = regionName;
        this.event = event;
    }

    public String getRegionName() {
        return regionName;
    }

    public CqEvent getEvent() {
        return event;
    }
}
