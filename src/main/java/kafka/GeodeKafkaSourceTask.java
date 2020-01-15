package kafka;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GeodeKafkaSourceTask extends SourceTask {
  private static String REGION_NAME = "REGION_NAME";
  private static String OFFSET = "OFFSET";
  private static String topics[];
  private int batchSize;
  private int queueSize;
  private static BlockingQueue<CqEvent> eventBuffer;
  private Map<String, String> sourcePartition;
  private Map<String, Long> offset;

  private ClientCache clientCache;

  @Override
  public String version() {
    return null;
  }

  @Override
  public void start(Map<String, String> props) {
    batchSize = 100;
    queueSize = 100000;
    String regionName = "someRegion";
    eventBuffer = new LinkedBlockingQueue<>(queueSize);
    topics = new String[] {"default"};
    sourcePartition = new HashMap<>();
    sourcePartition.put(REGION_NAME, regionName);

    offset = new HashMap<>();
    offset.put("OFFSET", 0L);

    installOnGeode("localHost", 10334, "someRegion");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    ArrayList<SourceRecord> records = new ArrayList<>(batchSize);
    ArrayList<CqEvent> events = new ArrayList<>(batchSize);
    if (eventBuffer.drainTo(events, batchSize) > 0) {
      for (CqEvent event : events) {

        for (String topic : topics)
          records.add(new SourceRecord(sourcePartition, offset, topic, null, event));
      }

      return records;
    }

    return null;
  }

  @Override
  public void stop() {
    clientCache.close(true);
  }

  private void installOnGeode(String locatorHost, int locatorPort, String regionName) {
    clientCache = new ClientCacheFactory().set("durable-client-id", "someClient")
        .set("durable-client-timeout", "200")
        .setPoolSubscriptionEnabled(true).addPoolLocator(locatorHost, locatorPort).create();
    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    cqAttributesFactory.addCqListener(new GeodeKafkaSourceListener());
    CqAttributes cqAttributes = cqAttributesFactory.create();
    try {
      clientCache.getQueryService().newCq("kafkaCQFor" + regionName, "select * from /" + regionName, cqAttributes,
          true);
    } catch (CqExistsException e) {
      e.printStackTrace();
    } catch (CqException e) {
      e.printStackTrace();
    }
    clientCache.readyForEvents();
  }

  private static class GeodeKafkaSourceListener implements CqListener {

    @Override
    public void onEvent(CqEvent aCqEvent) {
      try {
        eventBuffer.offer(aCqEvent, 2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {

        while (true) {
          try {
            if (!eventBuffer.offer(aCqEvent, 2, TimeUnit.SECONDS))
              break;
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
          System.out.println("GeodeKafkaSource Queue is full");
        }
      }
    }

    @Override
    public void onError(CqEvent aCqEvent) {

    }
  }
}
