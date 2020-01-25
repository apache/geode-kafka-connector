package geode.kafka.source;

import geode.kafka.GeodeConnectorConfig;
import geode.kafka.GeodeContext;
import org.apache.geode.cache.query.CqEvent;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static geode.kafka.GeodeConnectorConfig.DEFAULT_CQ_PREFIX;
import static geode.kafka.GeodeConnectorConfig.REGION_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeodeKafkaSourceTaskTest {


    @Test
    public void whenLoadingEntireRegionAbleToPutInitialResultsIntoEventBuffer() {
        GeodeContext geodeContext = mock(GeodeContext.class);
        BlockingQueue<GeodeEvent> eventBuffer = new LinkedBlockingQueue(100);
        boolean loadEntireRegion = true;
        boolean isDurable = false;
        List<CqEvent> fakeInitialResults = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            fakeInitialResults.add(mock(CqEvent.class));
        }

        when(geodeContext.newCqWithInitialResults(anyString(), anyString(), any(), anyBoolean())).thenReturn(fakeInitialResults);
        GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
        task.installListenersToRegion(geodeContext, 1, eventBuffer, "testRegion", DEFAULT_CQ_PREFIX, loadEntireRegion, isDurable);
        assertEquals(10, eventBuffer.size());
    }

    @Test
    public void whenNotLoadingEntireRegionShouldNotPutInitialResultsIntoEventBuffer() {
        GeodeContext geodeContext = mock(GeodeContext.class);
        BlockingQueue<GeodeEvent> eventBuffer = new LinkedBlockingQueue(100);
        boolean loadEntireRegion = false;
        boolean isDurable = false;
        List<CqEvent> fakeInitialResults = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            fakeInitialResults.add(mock(CqEvent.class));
        }

        when(geodeContext.newCqWithInitialResults(anyString(), anyString(), any(), anyBoolean())).thenReturn(fakeInitialResults);
        GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
        task.installListenersToRegion(geodeContext, 1, eventBuffer, "testRegion", DEFAULT_CQ_PREFIX, loadEntireRegion, isDurable);
        assertEquals(0, eventBuffer.size());
    }

    @Test
    public void cqListenerOnEventPopulatesEventsBuffer() {}

    @Test
    public void pollReturnsEventsWhenEventBufferHasValues() {

    }

    @Test
    public void regionsArePassedCorrectlyToTask() {

    }

    @Test
    public void installOnGeodeShouldCallCq() {
        GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
    }





    @Test
    public void createSourcePartitionsShouldReturnAMapOfSourcePartitions() {
        GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
        List<String> regionNames = Arrays.asList(new String[]{"region1", "region2", "region3"});
        Map<String, Map<String,String>> sourcePartitions = task.createSourcePartitionsMap(regionNames);
        assertThat(3, is(sourcePartitions.size()));
        assertThat(true, is(sourcePartitions.get("region1").get(REGION_NAME).equals("region1")));
        assertThat(true, is(sourcePartitions.get("region2").get(REGION_NAME).equals("region2")));
        assertThat(true, is(sourcePartitions.get("region3").get(REGION_NAME).equals("region3")));
    }



    @Test
    public void listOfLocatorsShouldBeConfiguredIntoClientCache() {

    }

    @Test
    public void shouldNotBeDurableIfDurableClientIdIsNull() {

    }

    @Test
    public void shouldNotCallReadyForEventsIfDurableClientPrefixIsEmpty() {

    }

    //Source properties tests
    @Test
    public void propertiesShouldBeCorrectlyTranslatedToConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(GeodeConnectorConfig.QUEUE_SIZE, GeodeConnectorConfig.DEFAULT_QUEUE_SIZE);
        props.put(GeodeConnectorConfig.BATCH_SIZE, GeodeConnectorConfig.DEFAULT_BATCH_SIZE);

        GeodeKafkaSourceTask task = new GeodeKafkaSourceTask();
//        task.start(props);

//        assertThat(task.getQueueSize(GeodeConnectorConfig.QUEUE_SIZE));


    }


}
