package geode.kafka.source;

import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class GeodeKafkaSourceListener implements CqStatusListener {

    private static final Logger logger = LoggerFactory.getLogger(GeodeKafkaSourceListener.class);

    public String regionName;
    private BlockingQueue<GeodeEvent> eventBuffer;

    public GeodeKafkaSourceListener(BlockingQueue<GeodeEvent> eventBuffer, String regionName) {
        this.eventBuffer = eventBuffer;
        this.regionName = regionName;
    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
        try {
            eventBuffer.offer(new GeodeEvent(regionName, aCqEvent), 2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

            while (true) {
                try {
                    if (!eventBuffer.offer(new GeodeEvent(regionName, aCqEvent), 2, TimeUnit.SECONDS))
                        break;
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                logger.info("GeodeKafkaSource Queue is full");
            }
        }
    }

    @Override
    public void onError(CqEvent aCqEvent) {

    }

    @Override
    public void onCqDisconnected() {
        //we should probably redistribute or reconnect
    }

    @Override
    public void onCqConnected() {

    }
}
