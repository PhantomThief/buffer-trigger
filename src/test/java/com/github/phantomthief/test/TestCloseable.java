package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2020-07-20.
 */
class TestCloseable {

    @Test
    void testSimpleClose() {
        AtomicInteger result = new AtomicInteger();
        AtomicInteger consumeCount = new AtomicInteger();
        BufferTrigger<String> trigger = BufferTrigger.<String, AtomicInteger> simple()
                .setContainer(AtomicInteger::new, (it, c) -> {
                    it.incrementAndGet();
                    return true;
                })
                .interval(1, SECONDS)
                .consumer(it -> {
                    result.addAndGet(it.intValue());
                    consumeCount.incrementAndGet();
                })
                .build();
        trigger.close(); // no thing happen due to lazy

        trigger.enqueue("test");
        trigger.enqueue("test");
        sleepUninterruptibly(2, SECONDS);
        assertEquals(2, result.intValue());

        trigger.enqueue("test");
        trigger.close();
        assertEquals(3, result.intValue());

        assertThrows(IllegalStateException.class, () -> trigger.enqueue("test"));
        trigger.manuallyDoTrigger();
        int thisConsumerCount = consumeCount.intValue();

        sleepUninterruptibly(2,SECONDS);
        assertEquals(thisConsumerCount, consumeCount.intValue());
    }

    @Test
    void testSimpleClose2() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger result = new AtomicInteger();
        AtomicInteger consumeCount = new AtomicInteger();
        BufferTrigger<String> trigger = BufferTrigger.<String, AtomicInteger> simple()
                .setContainerEx(AtomicInteger::new, (it, c) -> {
                    it.incrementAndGet();
                    return 1;
                })
                .setScheduleExecutorService(executor)
                .interval(1, SECONDS)
                .consumer(it -> {
                    result.addAndGet(it.intValue());
                    consumeCount.incrementAndGet();
                })
                .setExceptionHandler((it, it2) -> {})
                .build();
        trigger.close(); // no thing happen due to lazy

        trigger.enqueue("test");
        trigger.enqueue("test");
        sleepUninterruptibly(2, SECONDS);
        assertEquals(2, result.intValue());

        trigger.enqueue("test");
        trigger.close();
        assertEquals(3, result.intValue());

        assertThrows(IllegalStateException.class, () -> trigger.enqueue("test"));
        trigger.manuallyDoTrigger();

        int thisConsumerCount = consumeCount.intValue();
        sleepUninterruptibly(2,SECONDS);

        assertFalse(executor.isShutdown());
        assertEquals(thisConsumerCount, consumeCount.intValue());
    }

    @Test
    void testBlockingClose() {
        AtomicInteger result = new AtomicInteger();
        AtomicInteger consumeCount = new AtomicInteger();
        BufferTrigger<String> trigger = BufferTrigger.<String> batchBlocking()
                .linger(1, SECONDS)
                .batchSize(10)
                .setConsumer(it -> {
                    result.addAndGet(it.size());
                    consumeCount.incrementAndGet();
                })
                .build();
        trigger.close(); // no thing happen due to lazy

        for (int i = 0; i < 10; i++) {
            trigger.enqueue("test");
        }
        sleepUninterruptibly(1, SECONDS);
        assertEquals(10, result.intValue());

        trigger.enqueue("test");
        trigger.close();
        assertEquals(11, result.intValue());

        assertThrows(IllegalStateException.class, () -> trigger.enqueue("test"));
        trigger.manuallyDoTrigger();

        int thisConsumerCount = consumeCount.intValue();
        sleepUninterruptibly(2, SECONDS);
        assertEquals(thisConsumerCount, consumeCount.intValue());
    }

    @Test
    void testBlockingClose2() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger result = new AtomicInteger();
        AtomicInteger consumeCount = new AtomicInteger();
        BufferTrigger<String> trigger = BufferTrigger.<String> batchBlocking()
                .linger(1, SECONDS)
                .batchSize(10)
                .setScheduleExecutorService(executor)
                .setConsumerEx(it -> {
                    result.addAndGet(it.size());
                    consumeCount.incrementAndGet();
                })
                .build();
        trigger.close(); // no thing happen due to lazy

        for (int i = 0; i < 10; i++) {
            trigger.enqueue("test");
        }
        sleepUninterruptibly(1, SECONDS);
        assertEquals(10, result.intValue());

        trigger.enqueue("test");
        trigger.close();
        assertEquals(11, result.intValue());

        assertThrows(IllegalStateException.class, () -> trigger.enqueue("test"));
        trigger.manuallyDoTrigger();

        int thisConsumerCount = consumeCount.intValue();
        sleepUninterruptibly(2, SECONDS);

        assertFalse(executor.isShutdown());
        assertEquals(thisConsumerCount, consumeCount.intValue());
    }
}
