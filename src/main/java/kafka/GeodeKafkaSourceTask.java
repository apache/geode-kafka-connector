package kafka;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.RegionNotFoundException;
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
    System.out.println("JASON task start");
    batchSize = 100;
    queueSize = 100000;
    String regionName = "someRegion";
    eventBuffer = new LinkedBlockingQueue<>(queueSize);
    topics = new String[] {"someTopic"};
    sourcePartition = new HashMap<>();
    sourcePartition.put(REGION_NAME, regionName);

    offset = new HashMap<>();
    offset.put("OFFSET", 0L);

    installOnGeode("localHost", 10334, "someRegion");
    System.out.println("JASON task start end");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
//    System.out.println("JASON polling");
    ArrayList<SourceRecord> records = new ArrayList<>(batchSize);
    ArrayList<CqEvent> events = new ArrayList<>(batchSize);
    if (eventBuffer.drainTo(events, batchSize) > 0) {
      for (CqEvent event : events) {

        for (String topic : topics)
          records.add(new SourceRecord(sourcePartition, offset, topic, null, event));
      }

      System.out.println("JASON we polled and returning records" + records.size());
      return records;
    }

//    System.out.println("JASON we didn't poll any records");
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
    System.out.println("JASON installing on Geode");
    CqAttributes cqAttributes = cqAttributesFactory.create();
    try {
      System.out.println("JASON installing new cq");
      clientCache.getQueryService().newCq("kafkaCQFor" + regionName, "select * from /" + regionName, cqAttributes,
          true).execute();
      System.out.println("JASON finished installing cq");
    } catch (CqExistsException e) {
      System.out.println("UHH");
      e.printStackTrace();
    } catch (CqException | RegionNotFoundException e) {
      System.out.println("UHH e");
      e.printStackTrace();
    }
    catch (Exception e) {
      System.out.println("UHHHHHH " + e);
    }
    System.out.println("JASON task calling ready for events");
    clientCache.readyForEvents();
    System.out.println("JASON task ready for events");
  }

  private static class GeodeKafkaSourceListener implements CqListener {

    @Override
    public void onEvent(CqEvent aCqEvent) {
      try {
        System.out.println("JASON cqEvent and putting into eventBuffer");
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
