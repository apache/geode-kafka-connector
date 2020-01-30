package geode.kafka.sink;

import org.apache.geode.cache.Region;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A collection of records to put/remove from a region
 */
public class BatchRecords {

    private Map updateMap;
    private Collection removeList;

    public BatchRecords() {
        this(new HashMap(), new ArrayList());
    }

    /** Used for tests**/
    public BatchRecords(Map updateMap, Collection removeList) {
        this.updateMap = updateMap;
        this.removeList = removeList;
    }

    public void addRemoveOperation(SinkRecord record) {
        //if a previous operation added to the update map
        //let's just remove it so we don't do a put and then a remove
        //depending on the order of operations (putAll then removeAll or removeAll or putAll)...
        //...we could remove one of the if statements.
        if (updateMap.containsKey(record.key())) {
            updateMap.remove(record.key());
        } else {
            removeList.add(record.key());
        }
    }

    public void addUpdateOperation(SinkRecord record, boolean nullValuesMeansRemove) {
        //it's assumed the records in are order
        //if so if a previous value was in the remove list
        // let's not remove it at the end of this operation
        if (nullValuesMeansRemove) {
            if (removeList.contains(record.key())) {
                removeList.remove(record.key());
            }
        }
        updateMap.put(record.key(), record.value());
    }


    public void executeOperations(Region region) {
        region.putAll(updateMap);
        region.removeAll(removeList);
    }
}
