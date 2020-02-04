package geode.kafka.source;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class SharedEventBufferSupplier implements EventBufferSupplier {

    private static BlockingQueue<GeodeEvent> eventBuffer;

    public SharedEventBufferSupplier(int size) {
        recreateEventBufferIfNeeded(size);
    }

    BlockingQueue recreateEventBufferIfNeeded(int size) {
        if (eventBuffer == null || (eventBuffer.size() + eventBuffer.remainingCapacity()) != size) {
            synchronized (GeodeKafkaSource.class) {
                if (eventBuffer == null || (eventBuffer.size() + eventBuffer.remainingCapacity()) != size) {
                    BlockingQueue<GeodeEvent> oldEventBuffer = eventBuffer;
                    eventBuffer = new LinkedBlockingQueue<>(size);
                    if (oldEventBuffer != null) {
                        eventBuffer.addAll(oldEventBuffer);
                    }
                }
            }
        }
        return eventBuffer;
    }

    /**
     * Callers should not store a reference to this and instead always call get to make sure we always use the latest buffer
     * Buffers themselves shouldn't change often but in cases where we want to modify the size
     */
    public BlockingQueue<GeodeEvent> get() {
        return eventBuffer;
    }
}
