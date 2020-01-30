package geode.kafka.sink;

import org.apache.geode.cache.Region;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchRecordsTest {
    @Test
    public void updatingARecordShouldRemoveFromTheRemoveListIfNullValuesIsRemoveBooleanIsSet() {
        Map updates = mock(Map.class);
        Collection removes = mock(Collection.class);
        when(removes.contains(any())).thenReturn(true);
        BatchRecords records = new BatchRecords(updates, removes);
        SinkRecord sinkRecord = mock(SinkRecord.class);
        records.addUpdateOperation(sinkRecord, true);
        verify(removes, times(1)).remove(any());
    }

    @Test
    public void updatingARecordShouldAddToTheUpdateMap() {
        Map updates = mock(Map.class);
        Collection removes = mock(Collection.class);
        when(removes.contains(any())).thenReturn(false);
        BatchRecords records = new BatchRecords(updates, removes);
        SinkRecord sinkRecord = mock(SinkRecord.class);
        records.addUpdateOperation(sinkRecord, true);
        verify(updates, times(1)).put(any(), any());
    }

    @Test
    public void updatingARecordShouldNotRemoveFromTheRemoveListIfNullValuesIsNotSet() {
        boolean nullValuesMeanRemove = false;
        Map updates = mock(Map.class);
        Collection removes = mock(Collection.class);
        when(removes.contains(any())).thenReturn(true);
        BatchRecords records = new BatchRecords(updates, removes);
        SinkRecord sinkRecord = mock(SinkRecord.class);
        records.addUpdateOperation(sinkRecord, nullValuesMeanRemove);
        verify(removes, times(0)).remove(any());
    }


    @Test
    public void removingARecordShouldRemoveFromTheUpdateMapIfKeyIsPresent() {
        Map updates = mock(Map.class);
        Collection removes = mock(Collection.class);
        when(updates.containsKey(any())).thenReturn(true);
        BatchRecords records = new BatchRecords(updates, removes);
        SinkRecord sinkRecord = mock(SinkRecord.class);
        records.addRemoveOperation(sinkRecord);
        verify(updates, times(1)).remove(any());
    }

    @Test
    public void removingARecordAddToTheRemoveCollection() {
        Map updates = mock(Map.class);
        Collection removes = mock(Collection.class);
        BatchRecords records = new BatchRecords(updates, removes);
        SinkRecord sinkRecord = mock(SinkRecord.class);
        records.addRemoveOperation(sinkRecord);
        verify(removes, times(1)).add(any());
    }

    @Test
    public void executeOperationsShouldInvokePutAllAndRemoveAll() {
        Region region = mock(Region.class);
        BatchRecords records = new BatchRecords();
        records.executeOperations(region);
        verify(region, times(1)).putAll(any());
        verify(region, times(1)).removeAll(any());
    }



}
