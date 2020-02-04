package geode.kafka.source;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public interface EventBufferSupplier extends Supplier<BlockingQueue<GeodeEvent>> {
}
