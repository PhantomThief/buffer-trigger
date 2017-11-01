package com.github.phantomthief.test;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;

/**
 * @author w.vela
 * Created on 16/5/7.
 */
class ConflictReadWriteTest {

    private final Map<Object, AtomicInteger> counter = new IdentityHashMap<>();

    @Test
    void test() {
        Random random = new Random();
        BufferTrigger<Integer> bufferTrigger = SimpleBufferTrigger
                .<Integer, AtomicInteger> newGenericBuilder().on(5, SECONDS, 1) //
                .setContainerEx(AtomicInteger::new, (e, c) -> {
                    AtomicInteger atomicInteger;
                    synchronized (counter) {
                        atomicInteger = counter.computeIfAbsent(e, i -> new AtomicInteger());
                    }
                    atomicInteger.incrementAndGet();
                    try {
                        sleepUninterruptibly(random.nextInt(1000), MILLISECONDS);
                        e.incrementAndGet();
                    } finally {
                        atomicInteger.decrementAndGet();
                    }
                    return 1;
                }) //
                .consumer(container -> {
                    System.out.println(
                            "start consume:" + container.hashCode() + ", size:" + container.get());
                    AtomicInteger atomicInteger;
                    synchronized (counter) {
                        atomicInteger = counter.get(container);
                    }
                    assertEquals(0, atomicInteger.get());
                    sleepUninterruptibly(new Random().nextInt(10000), MILLISECONDS);
                    synchronized (counter) {
                        atomicInteger = counter.get(container);
                        counter.remove(container);
                    }
                    assertEquals(0, atomicInteger.get());
                    System.out.println(
                            "end consume:" + container.hashCode() + ", size:" + container.get());
                }) //
                .build();
        ExecutorService executorService = newFixedThreadPool(20);
        for (int i = 0; i < 100000; i++) {
            executorService.execute(() -> bufferTrigger.enqueue(1));
        }
        shutdownAndAwaitTermination(executorService, 1, DAYS);
    }
}
