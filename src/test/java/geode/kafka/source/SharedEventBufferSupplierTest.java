package geode.kafka.source;

import org.junit.Test;

import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class SharedEventBufferSupplierTest {

    @Test
    public void creatingNewSharedEventSupplierShouldCreateInstance() {
        SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
        assertNotNull(supplier.get());
    }

    @Test
    public void alreadySharedEventSupplierShouldReturnSameInstanceOfEventBuffer() {
        SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
        BlockingQueue<GeodeEvent> queue = supplier.get();
        supplier = new SharedEventBufferSupplier(1);
        assertEquals(queue, supplier.get());
    }

    @Test
    public void newEventBufferShouldBeReflectedInAllSharedSuppliers() {
        SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
        SharedEventBufferSupplier newSupplier = new SharedEventBufferSupplier(2);
        assertEquals(supplier.get(), newSupplier.get());
    }

    @Test
    public void newEventBufferSuppliedShouldNotBeTheOldQueue() {
        SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
        BlockingQueue<GeodeEvent> queue = supplier.get();
        SharedEventBufferSupplier newSupplier = new SharedEventBufferSupplier(2);
        assertNotEquals(queue, newSupplier.get());
    }

    @Test
    public void newEventBufferShouldContainAllEventsFromTheOldSupplier() {
        SharedEventBufferSupplier supplier = new SharedEventBufferSupplier(1);
        GeodeEvent geodeEvent = mock(GeodeEvent.class);
        supplier.get().add(geodeEvent);
        SharedEventBufferSupplier newSupplier = new SharedEventBufferSupplier(2);
        assertEquals(geodeEvent, newSupplier.get().poll());
    }
}
